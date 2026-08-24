"""Shared pytest fixtures and import plumbing for the repo-level live-surface pyramid.

This tree tests the surviving post-capture-retirement production surface: both AWS
Lambda handlers, the three kept ``dm_chain_utils`` library modules, the ``job_export_gold``
DABs batch task's pure logic, and the DLT expectation predicates (via source-text
extraction, never a live Spark/DLT runtime).

Run with:
    pytest tests -p no:cacheprovider

None of the modules under test are installed as packages in this tree (the Lambda
handlers and the DABs batch tasks are deployed, not pip-installed), so this conftest
puts each import root on ``sys.path`` explicitly, and loads same-named ``handler.py``
files under unique module names via ``importlib`` to avoid collisions.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[1]

UTILS_SRC = REPO_ROOT / "utils" / "src"
LAMBDA_CONTRACTS_INGESTION = REPO_ROOT / "apps" / "lambda" / "contracts_ingestion"
LAMBDA_GOLD_TO_DYNAMODB = REPO_ROOT / "apps" / "lambda" / "gold_to_dynamodb"
DABS_EXPORT_GOLD_SRC = REPO_ROOT / "apps" / "dabs" / "job_export_gold" / "src" / "batch"

# The shared library is a real dependency of both Lambda handlers — put it on
# sys.path once, for every test module in this tree.
if str(UTILS_SRC) not in sys.path:
    sys.path.insert(0, str(UTILS_SRC))

# PYTHONDONTWRITEBYTECODE keeps stray __pycache__ dirs out of the repo tree even
# when a test imports a module directly off disk (DADAIA.md workspace hygiene law).
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")


def load_module_from_path(module_name: str, file_path: Path) -> ModuleType:
    """Load a Python file as a uniquely-named module, bypassing package/sys.path rules.

    Both Lambda functions ship a same-named ``handler.py`` in sibling directories;
    importing them by relative package name would collide. This loads each one under
    an explicit, distinct module name instead.
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    assert spec is not None and spec.loader is not None, f"cannot load spec for {file_path}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
