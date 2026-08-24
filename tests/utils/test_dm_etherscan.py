"""Unit tests for ``dm_chain_utils.dm_etherscan.EtherscanClient``.

Intent: CONTRACT — AC-23 (live-surface pyramid: kept dm_chain_utils modules); also
covers the security-lane's H-01 correction (the client logs the SSM *parameter name*,
never the key value).
Size: unit — every HTTP call is monkeypatched, no network egress, sub-second runtime.
"""

from __future__ import annotations

import json
import logging

import pytest
from dm_chain_utils.dm_etherscan import EtherscanClient


class _FakeResponse:
    def __init__(self, payload: dict, status: int = 200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


@pytest.fixture()
def client(tmp_path, monkeypatch):
    # Isolate the disk ABI cache to a throwaway directory for every test.
    monkeypatch.setattr("dm_chain_utils.dm_etherscan._ABI_CACHE_DIR", tmp_path)
    return EtherscanClient(
        logging.getLogger("test"), api_key="live-secret-key-value", api_key_name="api-key-1"
    )


class TestTrackCallLoggingDoesNotLeakTheKey:
    """Regression guard for the pattern the security lane flagged in the sibling
    (now-retired) producer code: the log line must carry the SSM *parameter name*,
    never the raw key material."""

    def test_track_call_logs_key_name_not_value(self, client, caplog):
        caplog.set_level(logging.INFO)

        client._track_call("getabi", "OK")

        assert "api-key-1" in caplog.text
        assert "live-secret-key-value" not in caplog.text

    def test_call_count_increments(self, client):
        assert client.call_count == 0
        client._track_call("getabi", "OK")
        assert client.call_count == 1


class TestGetBlockByTimestamp:
    def test_success_returns_parsed_response(self, client, monkeypatch):
        monkeypatch.setattr(
            "dm_chain_utils.dm_etherscan.requests.get",
            lambda *a, **kw: _FakeResponse({"message": "OK", "result": "18500000"}),
        )

        result = client.get_block_by_timestamp(1700000000)

        assert result == {"message": "OK", "result": "18500000"}
        assert client.call_count == 1

    def test_network_error_returns_error_dict_not_raise(self, client, monkeypatch):
        def _boom(*a, **kw):
            raise ConnectionError("network down")

        monkeypatch.setattr("dm_chain_utils.dm_etherscan.requests.get", _boom)

        result = client.get_block_by_timestamp(1700000000)

        assert result == {"message": "ERROR", "result": None}


class TestGetContractTxsByBlockInterval:
    def test_success_returns_txlist(self, client, monkeypatch):
        monkeypatch.setattr(
            "dm_chain_utils.dm_etherscan.requests.get",
            lambda *a, **kw: _FakeResponse({"message": "OK", "result": [{"hash": "0x1"}]}),
        )

        result = client.get_contract_txs_by_block_interval("0xabc", 100, 200)

        assert result["result"] == [{"hash": "0x1"}]

    def test_error_status_returns_error_dict(self, client, monkeypatch):
        monkeypatch.setattr(
            "dm_chain_utils.dm_etherscan.requests.get",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        result = client.get_contract_txs_by_block_interval("0xabc", 100, 200)

        assert result == {"message": "ERROR", "result": None}


class TestGet4ByteSignature:
    def test_returns_first_match_text_signature(self, client, monkeypatch):
        monkeypatch.setattr(
            "dm_chain_utils.dm_etherscan.requests.get",
            lambda *a, **kw: _FakeResponse(
                {"results": [{"text_signature": "swapExactTokensForTokens(uint256)"}]}
            ),
        )

        sig = client.get_4byte_signature("0x38ed1739")

        assert sig == "swapExactTokensForTokens(uint256)"

    def test_returns_none_when_no_results(self, client, monkeypatch):
        monkeypatch.setattr(
            "dm_chain_utils.dm_etherscan.requests.get",
            lambda *a, **kw: _FakeResponse({"results": []}),
        )

        assert client.get_4byte_signature("0xdeadbeef") is None

    def test_returns_none_on_request_exception(self, client, monkeypatch):
        def _boom(*a, **kw):
            raise TimeoutError("slow")

        monkeypatch.setattr("dm_chain_utils.dm_etherscan.requests.get", _boom)

        assert client.get_4byte_signature("0xdeadbeef") is None


class TestGetContractAbi:
    def test_fetches_and_caches_to_disk(self, client, monkeypatch, tmp_path):
        abi = [{"type": "function", "name": "transfer"}]
        monkeypatch.setattr(
            "dm_chain_utils.dm_etherscan.requests.get",
            lambda *a, **kw: _FakeResponse({"status": "1", "result": json.dumps(abi)}),
        )

        result = client.get_contract_abi("0xABC")

        assert result == abi
        cached = json.loads((tmp_path / "0xabc.json").read_text())
        assert cached == abi

    def test_unverified_contract_returns_none(self, client, monkeypatch):
        monkeypatch.setattr(
            "dm_chain_utils.dm_etherscan.requests.get",
            lambda *a, **kw: _FakeResponse({"status": "0", "result": "Contract source not verified"}),
        )

        assert client.get_contract_abi("0xunverified") is None

    def test_second_call_reads_from_disk_cache_without_new_request(self, client, monkeypatch, tmp_path):
        abi = [{"type": "function", "name": "approve"}]
        (tmp_path / "0xcached.json").write_text(json.dumps(abi))

        def _fail_if_called(*a, **kw):
            raise AssertionError("Etherscan should not be called — disk cache hit expected")

        monkeypatch.setattr("dm_chain_utils.dm_etherscan.requests.get", _fail_if_called)

        assert client.get_contract_abi("0xcached") == abi
