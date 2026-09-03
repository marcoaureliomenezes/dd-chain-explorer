"""Contract tests for the DABs target set and the validate/deploy entrypoints.

Intent: CONTRACT — SPEC v0.6.0 ADR-10 (two environments: ``dev`` = Free Edition,
``prod`` = the official account; no ``hml``) and the regression seam of bug
``dabs-validate-target-skips-dashboard-render`` (the validate lane never rendered the
dashboard templates, so a fresh checkout failed all four dashboard bundles).
Size: contract — reads ``apps/dabs/*/databricks.yml`` and runs ``make -n`` / the render
script's argument parsing only; no Databricks CLI call, no file is rendered.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from tests.conftest import REPO_ROOT

DABS_DIR = REPO_ROOT / "apps" / "dabs"
BUNDLES = sorted(p.parent for p in DABS_DIR.glob("*/databricks.yml"))
RENDER = DABS_DIR / "render_dashboard_templates.sh"
EXPECTED_TARGETS = {"dev", "prod"}


def _targets(bundle: Path) -> dict:
    return yaml.safe_load((bundle / "databricks.yml").read_text())["targets"]


def test_seven_bundles_are_discovered():
    assert len(BUNDLES) == 7, [b.name for b in BUNDLES]


@pytest.mark.parametrize("bundle", BUNDLES, ids=lambda b: b.name)
def test_every_bundle_declares_exactly_dev_and_prod(bundle: Path):
    assert set(_targets(bundle)) == EXPECTED_TARGETS


@pytest.mark.parametrize("bundle", BUNDLES, ids=lambda b: b.name)
def test_prod_is_a_real_production_target_without_a_hard_coded_identity(bundle: Path):
    prod = _targets(bundle)["prod"]
    assert prod.get("mode") == "production"
    # The deployer (the production workspace's own service principal, OAuth M2M from
    # the `production` GitHub environment) IS the run_as identity — no application id
    # of any workspace belongs in the tree.
    assert "run_as" not in prod
    assert "host" not in prod.get("workspace", {})


@pytest.mark.parametrize("bundle", BUNDLES, ids=lambda b: b.name)
def test_no_bundle_mentions_the_retired_environment(bundle: Path):
    assert "hml" not in (bundle / "databricks.yml").read_text().lower()


@pytest.mark.parametrize("make_target", ["dabs_validate_all", "dabs_deploy_all", "dabs_deploy_dashboards"])
@pytest.mark.parametrize("target", sorted(EXPECTED_TARGETS))
def test_every_validate_or_deploy_entrypoint_renders_the_dashboards_first(make_target: str, target: str):
    out = subprocess.run(
        ["make", "-n", make_target, f"TARGET={target}"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    render_call = f"render_dashboard_templates.sh --target {target}"
    assert render_call in out, out
    assert out.index(render_call) < out.index("databricks bundle"), out


def test_deploy_all_script_delegates_the_target_map_to_the_render_script():
    text = (DABS_DIR / "deploy_all.sh").read_text()
    assert 'render_dashboard_templates.sh" --target "${TARGET}"' in text
    assert "DASHBOARD_CATALOG" not in text  # the map lives in exactly one place


@pytest.mark.parametrize("bad_target", ["hml", "prd", "production", ""])
def test_render_script_rejects_an_unknown_target_before_writing(bad_target: str):
    result = subprocess.run(
        ["bash", str(RENDER), "--target", bad_target],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "Usage:" in result.stderr
    assert "rendered:" not in result.stdout
