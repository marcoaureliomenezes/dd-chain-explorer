"""Contract tests for the DLT pipelines' data-quality expectations.

Intent: CONTRACT — AC-23 (live-surface pyramid: DLT expectation predicates).
Size: contract — no Spark/DLT runtime is started (``dlt`` is Databricks-runtime-only
and not pip-installable outside a workspace); each expectation's SQL condition string
is read directly from the pipeline source file and evaluated by a small, grammar-
limited interpreter (``tests/dabs/_dlt_expectation_support.py``) against representative
valid/malformed sample rows. Because the condition string is extracted, not hand-
copied, a change to the real predicate is a change to what this test evaluates too.
"""

from __future__ import annotations

import pytest

from tests.conftest import REPO_ROOT
from tests.dabs._dlt_expectation_support import evaluate_condition, extract_expectations, find

ETHEREUM_PIPELINE = (
    REPO_ROOT / "apps" / "dabs" / "dlt_ethereum" / "src" / "streaming" / "ethereum_pipeline.py"
)
APP_LOGS_PIPELINE = (
    REPO_ROOT / "apps" / "dabs" / "dlt_app_logs" / "src" / "streaming" / "app_logs_pipeline.py"
)


@pytest.fixture(scope="module")
def ethereum_expectations():
    return extract_expectations(ETHEREUM_PIPELINE)


@pytest.fixture(scope="module")
def app_logs_expectations():
    return extract_expectations(APP_LOGS_PIPELINE)


class TestEvaluatorGrammar:
    """The interpreter itself, over hand-written condition strings — proves the
    grammar-limited evaluator is correct before trusting it against extracted text."""

    def test_is_not_null_true_and_false(self):
        assert evaluate_condition("block_number IS NOT NULL", {"block_number": 1}) is True
        assert evaluate_condition("block_number IS NOT NULL", {"block_number": None}) is False
        assert evaluate_condition("block_number IS NOT NULL", {}) is False

    def test_is_null_true_and_false(self):
        assert evaluate_condition("to_address IS NULL", {"to_address": None}) is True
        assert evaluate_condition("to_address IS NULL", {"to_address": "0xabc"}) is False

    def test_rlike_matches_valid_address(self):
        cond = "from_address RLIKE '^0x[a-fA-F0-9]{40}$'"
        valid = "0x" + "a" * 40
        assert evaluate_condition(cond, {"from_address": valid}) is True

    def test_rlike_rejects_malformed_address(self):
        cond = "from_address RLIKE '^0x[a-fA-F0-9]{40}$'"
        assert evaluate_condition(cond, {"from_address": "not-an-address"}) is False
        assert evaluate_condition(cond, {"from_address": "0x" + "a" * 39}) is False  # too short

    def test_and_requires_both_clauses(self):
        cond = "from_address IS NOT NULL AND from_address RLIKE '^0x[a-fA-F0-9]{40}$'"
        valid = "0x" + "b" * 40
        assert evaluate_condition(cond, {"from_address": valid}) is True
        assert evaluate_condition(cond, {"from_address": None}) is False
        assert evaluate_condition(cond, {"from_address": "garbage"}) is False

    def test_or_passes_when_either_clause_true(self):
        cond = "to_address IS NULL OR to_address RLIKE '^0x[a-fA-F0-9]{40}$'"
        valid = "0x" + "c" * 40
        assert evaluate_condition(cond, {"to_address": None}) is True  # contract creation
        assert evaluate_condition(cond, {"to_address": valid}) is True
        assert evaluate_condition(cond, {"to_address": "not-an-address"}) is False


class TestEthereumPipelineExtraction:
    """Locks the exact set + enforcement kind of the ethereum pipeline's silver
    expectations — a regression here means the pipeline's data-quality contract
    silently changed."""

    def test_silver_eth_blocks_requires_block_number_and_hash(self, ethereum_expectations):
        block_number = find(ethereum_expectations, "silver_eth_blocks", "valid_block_number")
        block_hash = find(ethereum_expectations, "silver_eth_blocks", "valid_hash")

        assert block_number.kind == "expect_or_drop"
        assert block_hash.kind == "expect_or_drop"
        assert block_number.matches({"block_number": 1}) is True
        assert block_number.matches({"block_number": None}) is False

    def test_from_address_is_expect_or_drop_not_downgraded_to_warn_only(self, ethereum_expectations):
        # ISSUE-030 / DE-P-003: from_address was deliberately promoted expect ->
        # expect_or_drop. A regression back to `expect` (warn-only) must fail this test.
        from_address = find(ethereum_expectations, "silver_eth_transactions_staging", "valid_from_address")

        assert from_address.kind == "expect_or_drop"
        valid = "0x" + "d" * 40
        assert from_address.matches({"from_address": valid}) is True
        assert from_address.matches({"from_address": None}) is False
        assert from_address.matches({"from_address": "not-hex"}) is False

    def test_to_address_stays_expect_and_allows_null_for_contract_creation(self, ethereum_expectations):
        # EIP-155 contract-creation transactions legitimately have to_address = NULL.
        # Downgrading this to expect_or_drop would silently drop every contract deploy.
        to_address = find(ethereum_expectations, "silver_eth_transactions_staging", "valid_to_address")

        assert to_address.kind == "expect"
        valid = "0x" + "e" * 40
        assert to_address.matches({"to_address": None}) is True
        assert to_address.matches({"to_address": valid}) is True
        assert to_address.matches({"to_address": "malformed"}) is False

    def test_silver_txs_inputs_decoded_fast_requires_tx_hash(self, ethereum_expectations):
        exp = find(ethereum_expectations, "silver_txs_inputs_decoded_fast", "valid_tx_hash")

        assert exp.kind == "expect_or_drop"
        assert exp.matches({"tx_hash": "0xabc"}) is True
        assert exp.matches({"tx_hash": None}) is False

    def test_at_least_the_known_expectation_count_survives(self, ethereum_expectations):
        # A coarse guard against silent mass-deletion of expectations during a future
        # edit — not a substitute for the per-expectation tests above.
        assert len(ethereum_expectations) >= 9


class TestAppLogsPipelineExtraction:
    def test_silver_logs_require_level_and_message(self, app_logs_expectations):
        level_exps = [e for e in app_logs_expectations if e.name == "valid_level"]
        message_exps = [e for e in app_logs_expectations if e.name == "valid_message"]

        assert len(level_exps) >= 1
        assert len(message_exps) >= 1
        for exp in level_exps + message_exps:
            assert exp.kind == "expect_or_drop"

        one_level = level_exps[0]
        assert one_level.matches({"level": "INFO"}) is True
        assert one_level.matches({"level": None}) is False
