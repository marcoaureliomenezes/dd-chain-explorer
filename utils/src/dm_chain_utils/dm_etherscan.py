"""
Etherscan API v2 client with disk-based ABI cache and 4byte.directory fallback.

Strategy for decoding ANY transaction input:
  1. Try Etherscan API → fetch verified contract ABI   → full decode (method + params)
  2. Fallback to 4byte.directory → lookup 4-byte selector  → method name only (params raw hex)
  3. If both fail → return None (skip message)

API v2 routes requests via `chainid` query parameter instead of subdomain.
  mainnet  → chainid=1
  goerli   → chainid=5
  sepolia  → chainid=11155111

Usage:
    from dm_chain_utils.dm_etherscan import EtherscanClient

    client = EtherscanClient(logger, api_key, network='mainnet')
    abi = client.get_contract_abi('0xAbcd...')          # None if unverified
    sig = client.get_4byte_signature('0x38ed1739')      # fallback method name
    blk = client.get_block_by_timestamp(1700000000)     # {'result': '18500000', ...}
"""

import json
import os
from functools import lru_cache
from logging import Logger
from pathlib import Path
from typing import Any

import requests

_ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

_ETHERSCAN_CHAIN_IDS = {
    "mainnet": 1,
    "goerli": 5,
    "sepolia": 11155111,
}

_4BYTE_URL = "https://www.4byte.directory/api/v1/signatures/"

# Disk cache dir — survives across runs within the same container lifetime
_ABI_CACHE_DIR = Path(os.getenv("ABI_CACHE_DIR", "/tmp/abi_cache"))


class EtherscanClient:
    """Thin Etherscan API v2 wrapper with disk-backed ABI cache."""

    def __init__(
        self,
        logger: Logger,
        api_key: str,
        network: str = "mainnet",
        api_key_name: str | None = None,
    ):
        self.logger = logger
        self._api_key = api_key
        self._api_key_id = api_key_name or "UNKNOWN_KEY"  # parameter store key name for logging
        self._chain_id = _ETHERSCAN_CHAIN_IDS.get(network, 1)
        self._base_url = _ETHERSCAN_V2_BASE
        self._call_count = 0  # tracks total API calls for consumption monitoring
        _ABI_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @property
    def call_count(self) -> int:
        """Total number of Etherscan API calls made since instantiation."""
        return self._call_count

    def _track_call(self, action: str, status: str = "ok") -> None:
        """Increment call counter and log structured API consumption info."""
        self._call_count += 1
        self.logger.info(
            f"etherscan;api_call;api_key_name:{self._api_key_id};"
            f"action:{action};status:{status};request_count:{self._call_count}"
        )

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def get_contract_abi(self, address: str) -> list[Any] | None:
        """
        Return the ABI list for *address*, or None if not verified.
        Results are cached to disk so Etherscan is only called once per address.
        """
        address = address.lower()
        cached = self._load_from_disk(address)
        if cached is not None:
            return cached

        abi = self._fetch_abi_from_etherscan(address)
        if abi is not None:
            self._save_to_disk(address, abi)
        return abi

    def get_4byte_signature(self, selector: str) -> str | None:
        """
        Query 4byte.directory for a human-readable function signature
        matching *selector* (e.g. '0x38ed1739').
        Returns the first match, e.g. 'swapExactTokensForTokens(uint256,...)'
        """
        try:
            resp = requests.get(
                _4BYTE_URL,
                params={"hex_signature": selector},
                timeout=5,
            )
            resp.raise_for_status()
            results: list[dict[str, Any]] = resp.json().get("results", [])
            if results:
                text_signature: str = results[0]["text_signature"]
                return text_signature
        except Exception as exc:
            self.logger.debug(f"[4byte] lookup failed for {selector}: {exc}")
        return None

    def get_block_by_timestamp(self, timestamp: int, closest: str = "before") -> dict[str, Any]:
        """
        Return the block number closest to *timestamp* (Unix epoch).

        Parameters
        ----------
        timestamp : int
            Unix timestamp.
        closest : str
            'before' or 'after' — which side of the timestamp to prefer.

        Returns
        -------
        dict
            Etherscan response with keys 'status', 'message', 'result'
            (result = block number as string).  Returns {"message": "ERROR", "result": None}
            on network/parse failures.
        """
        params: dict[str, str | int] = {
            "chainid": self._chain_id,
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": timestamp,
            "closest": closest,
            "apikey": self._api_key,
        }
        try:
            resp = requests.get(self._base_url, params=params, timeout=10)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            self._track_call("getblocknobytime", data.get("message", "UNKNOWN"))
            return data
        except Exception as exc:
            self._track_call("getblocknobytime", "ERROR")
            self.logger.warning(f"[Etherscan] get_block_by_timestamp error: {exc}")
            return {"message": "ERROR", "result": None}

    def get_contract_txs_by_block_interval(
        self,
        address: str,
        startblock: int,
        endblock: int,
        page: int = 1,
        offset: int = 1000,
        sort: str = "asc",
    ) -> dict[str, Any]:
        """
        Return normal transactions for *address* between *startblock* and *endblock*.
        """
        params: dict[str, str | int] = {
            "chainid": self._chain_id,
            "module": "account",
            "action": "txlist",
            "address": address,
            "startblock": startblock,
            "endblock": endblock,
            "page": page,
            "offset": offset,
            "sort": sort,
            "apikey": self._api_key,
        }
        try:
            resp = requests.get(self._base_url, params=params, timeout=15)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            self._track_call("txlist", data.get("message", "UNKNOWN"))
            return data
        except Exception as exc:
            self._track_call("txlist", "ERROR")
            self.logger.warning(f"[Etherscan] get_contract_txs_by_block_interval error for {address}: {exc}")
            return {"message": "ERROR", "result": None}

    def get_internal_txs_by_block_interval(
        self,
        address: str,
        startblock: int,
        endblock: int,
        page: int = 1,
        offset: int = 1000,
        sort: str = "asc",
    ) -> dict[str, Any]:
        """Return internal transactions for *address* in a block range."""
        params: dict[str, str | int] = {
            "chainid": self._chain_id,
            "module": "account",
            "action": "txlistinternal",
            "address": address,
            "startblock": startblock,
            "endblock": endblock,
            "page": page,
            "offset": offset,
            "sort": sort,
            "apikey": self._api_key,
        }
        try:
            resp = requests.get(self._base_url, params=params, timeout=15)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            self._track_call("txlistinternal", data.get("message", "UNKNOWN"))
            return data
        except Exception as exc:
            self._track_call("txlistinternal", "ERROR")
            self.logger.warning(f"[Etherscan] get_internal_txs_by_block_interval error for {address}: {exc}")
            return {"message": "ERROR", "result": None}

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _fetch_abi_from_etherscan(self, address: str) -> list[Any] | None:
        params: dict[str, str] = {
            "chainid": str(self._chain_id),
            "module": "contract",
            "action": "getabi",
            "address": address,
            "apikey": self._api_key,
        }
        try:
            resp = requests.get(self._base_url, params=params, timeout=10)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            self._track_call("getabi", data.get("message", "UNKNOWN"))
        except Exception as exc:
            self._track_call("getabi", "ERROR")
            self.logger.warning(f"[Etherscan] HTTP error for {address}: {exc}")
            return None

        if data.get("status") != "1":
            self.logger.debug(f"[Etherscan] ABI not available for {address}: {data.get('result')}")
            return None

        try:
            abi: list[Any] = json.loads(data["result"])
            self.logger.debug(f"[Etherscan] ABI fetched for {address}")
            return abi
        except (json.JSONDecodeError, KeyError) as exc:
            self.logger.warning(f"[Etherscan] ABI parse error for {address}: {exc}")
            return None

    # B019: lru_cache on a bound method keeps every EtherscanClient instance
    # alive for the process lifetime — acceptable here because one client is
    # constructed per Lambda invocation and the whole process is short-lived;
    # flagged, not fixed, in this task's scope (T-D.5 is lint-config, not a
    # caching-strategy redesign).
    @lru_cache(maxsize=4096)  # noqa: B019
    def _load_from_disk(self, address: str) -> list[Any] | None:
        path = _ABI_CACHE_DIR / f"{address}.json"
        if path.exists():
            try:
                cached: list[Any] = json.loads(path.read_text())
                return cached
            except Exception:
                pass
        return None

    def _save_to_disk(self, address: str, abi: list[Any]) -> None:
        path = _ABI_CACHE_DIR / f"{address}.json"
        try:
            path.write_text(json.dumps(abi))
        except Exception as exc:
            self.logger.debug(f"[ABI cache] write failed for {address}: {exc}")
