"""Unit tests for ``dm_chain_utils.dm_parameter_store.ParameterStoreClient``.

Intent: CONTRACT — AC-23 (live-surface pyramid: kept dm_chain_utils modules).
Size: unit — moto-mocked SSM, no network egress, sub-second runtime.

Deliberately absent: a test for ``list_parameters`` — the account-wide bulk-decrypt
helper (security-lane finding L3, CWE-269: "loaded gun in a shared library, no caller
exists today") was removed from this module by T-D.5, not merely left untested.
"""

from __future__ import annotations

import pytest
from dm_chain_utils.dm_parameter_store import ParameterStoreClient
from moto import mock_aws

REGION = "sa-east-1"


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        yield ParameterStoreClient(region_name=REGION)


class TestGetParameter:
    def test_returns_decrypted_value(self, client):
        client._client.put_parameter(
            Name="/etherscan-api-keys/api-key-1", Value="secret-value", Type="SecureString"
        )

        value = client.get_parameter("/etherscan-api-keys/api-key-1")

        assert value == "secret-value"

    def test_returns_none_when_parameter_missing(self, client):
        assert client.get_parameter("/etherscan-api-keys/does-not-exist") is None


class TestPutAndDeleteParameter:
    def test_put_then_get_roundtrips(self, client):
        ok = client.put_parameter("/etherscan-api-keys/api-key-2", "another-secret")

        assert ok is True
        assert client.get_parameter("/etherscan-api-keys/api-key-2") == "another-secret"

    def test_delete_parameter_removes_it(self, client):
        client.put_parameter("/etherscan-api-keys/api-key-3", "value")

        ok = client.delete_parameter("/etherscan-api-keys/api-key-3")

        assert ok is True
        assert client.get_parameter("/etherscan-api-keys/api-key-3") is None

    def test_delete_missing_parameter_returns_false(self, client):
        assert client.delete_parameter("/etherscan-api-keys/never-existed") is False


class TestGetParametersByPath:
    def test_returns_every_parameter_under_the_prefix(self, client):
        client.put_parameter("/etherscan-api-keys/api-key-1", "v1")
        client.put_parameter("/etherscan-api-keys/api-key-2", "v2")
        client.put_parameter("/web3-api-keys/infura/api-key-1", "unrelated")

        result = client.get_parameters_by_path("/etherscan-api-keys")

        assert result == {
            "/etherscan-api-keys/api-key-1": "v1",
            "/etherscan-api-keys/api-key-2": "v2",
        }

    def test_returns_empty_dict_when_path_has_no_parameters(self, client):
        assert client.get_parameters_by_path("/nothing-here") == {}


def test_list_parameters_helper_is_removed():
    """The bulk-decrypt-everything helper must not exist on the kept client at all —
    not merely be untested. Guards against a silent reintroduction."""
    assert not hasattr(ParameterStoreClient, "list_parameters")
