"""Source-text extraction + evaluation for DLT ``@dlt.expect``/``@dlt.expect_or_drop``
predicates.

Not a test module (no ``test_`` prefix — pytest will not collect it).

Why source-text extraction instead of importing the pipeline modules: ``dlt`` is a
Databricks-runtime-only package, not pip-installable outside a workspace, so the
pipeline modules under ``apps/dabs/dlt_*/`` cannot be imported in a local/CI process.
Reading the decorator strings directly off disk lets ``tests/dabs/test_dlt_expectations_contract.py``
detect real drift (a changed regex, a demoted ``expect_or_drop`` -> ``expect``, a
renamed column) without needing a live Spark/DLT runtime — the predicate *string* is
the same artifact Databricks evaluates at pipeline run time, so a change here is a
change there too.

This evaluator only supports the specific expression grammar actually used by this
repo's two DLT pipelines today: ``col IS [NOT] NULL``, ``col RLIKE '<regex>'``,
combined with a single top-level ``AND``/``OR``. It is not a general SQL parser and
will raise ``ValueError`` on anything else — a deliberate fail-loud if the pipeline
author introduces a condition shape this test suite does not yet understand.
"""

from __future__ import annotations

import re
from pathlib import Path

_DECORATOR_RE = re.compile(
    r'^@dlt\.(expect_or_drop|expect)\(\s*"([^"]+)"\s*,\s*"((?:[^"\\]|\\.)*)"\s*\)\s*(?:#.*)?$'
)
_DEF_RE = re.compile(r"^def\s+(\w+)\s*\(")

_IS_NOT_NULL_RE = re.compile(r"^(\w+)\s+IS\s+NOT\s+NULL$")
_IS_NULL_RE = re.compile(r"^(\w+)\s+IS\s+NULL$")
_RLIKE_RE = re.compile(r"^(\w+)\s+RLIKE\s+'((?:[^'\\]|\\.)*)'$")
_BOOL_SPLIT_RE = re.compile(r"\s+(AND|OR)\s+")


class Expectation:
    """One ``@dlt.expect``/``@dlt.expect_or_drop`` decorator, as found in source."""

    def __init__(self, kind: str, name: str, condition: str, function: str):
        self.kind = kind  # "expect" or "expect_or_drop"
        self.name = name
        self.condition = condition
        self.function = function

    def __repr__(self) -> str:  # pragma: no cover - debug aid only
        return f"Expectation({self.kind!r}, {self.name!r}, {self.condition!r}, fn={self.function!r})"

    def matches(self, row: dict) -> bool:
        """Evaluate this expectation's condition against a sample row dict."""
        return evaluate_condition(self.condition, row)


def extract_expectations(source_path: Path) -> list[Expectation]:
    """Return every ``@dlt.expect*`` decorator found directly above a ``def`` line."""
    expectations: list[Expectation] = []
    pending: list[tuple[str, str, str]] = []
    for raw_line in source_path.read_text().splitlines():
        line = raw_line.strip()
        m = _DECORATOR_RE.match(line)
        if m:
            kind, name, condition = m.groups()
            pending.append((kind, name, condition))
            continue
        d = _DEF_RE.match(line)
        if d:
            function = d.group(1)
            for kind, name, condition in pending:
                expectations.append(Expectation(kind, name, condition, function))
            pending = []
            continue
        if line.startswith("#") or line == "":
            continue
        # Any other real source line breaks direct decorator-to-def adjacency.
        pending = []
    return expectations


def _eval_clause(clause: str, row: dict) -> bool:
    clause = clause.strip()

    m = _IS_NOT_NULL_RE.match(clause)
    if m:
        return row.get(m.group(1)) is not None

    m = _IS_NULL_RE.match(clause)
    if m:
        return row.get(m.group(1)) is None

    m = _RLIKE_RE.match(clause)
    if m:
        column, pattern = m.groups()
        value = row.get(column)
        return value is not None and re.search(pattern, value) is not None

    raise ValueError(f"Unsupported expectation clause grammar: {clause!r}")


def evaluate_condition(condition: str, row: dict) -> bool:
    """Evaluate a ``@dlt.expect*`` SQL condition string against a plain-dict sample row.

    Supports a single top-level chain of ``AND``/``OR`` over ``IS [NOT] NULL`` and
    ``RLIKE`` clauses — exactly the grammar this repo's DLT pipelines use today.
    """
    parts = _BOOL_SPLIT_RE.split(condition)
    result = _eval_clause(parts[0], row)
    i = 1
    while i < len(parts):
        op = parts[i]
        clause_result = _eval_clause(parts[i + 1], row)
        result = (result and clause_result) if op == "AND" else (result or clause_result)
        i += 2
    return result


def find(expectations: list[Expectation], function: str, name: str) -> Expectation:
    for exp in expectations:
        if exp.function == function and exp.name == name:
            return exp
    raise LookupError(f"no expectation named {name!r} on function {function!r}")
