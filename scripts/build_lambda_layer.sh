#!/usr/bin/env bash
# scripts/build_lambda_layer.sh — build the dm-chain-utils Lambda layer, from source.
#
# Deterministic, reproducible build:
#   1. Third-party dependencies are installed hash-pinned from
#      apps/lambda/requirements.txt (--require-hashes).
#   2. dm_chain_utils itself is installed as a PATH requirement, --no-deps —
#      this closes dependency confusion (a malicious "dm-chain-utils" package
#      could never be published to a public index and picked up instead,
#      because pip is never asked to resolve that name at all). --no-index is
#      deliberately NOT used for step 1: the third-party transitive deps
#      (certifi, charset-normalizer, idna, urllib3) legitimately come from
#      the index, hash-pinned.
#   3. build/python/ is zipped into a reproducible zip: sorted entry order,
#      fixed mtimes — so the same inputs always produce the same bytes (and
#      the same sha256), which is what lets Terraform (T-B.14) key off the
#      hash instead of re-uploading a "new" layer on every unrelated commit.
#
# Usage:
#   scripts/build_lambda_layer.sh
#
# Output (both printed as the LAST line, machine-readable, and left on disk):
#   LAYER_ZIP=<path> LAYER_SHA256=<hex>
#
# CI upload + Terraform -var pass-through are T-A.7 / T-B.14 — this script's
# job ends at producing the zip + its hash.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BUILD_DIR="${REPO_ROOT}/build"
LAYER_PYTHON_DIR="${BUILD_DIR}/python"
ZIP_DIR="${REPO_ROOT}/.lambda_zip"
ZIP_PATH="${ZIP_DIR}/dm_chain_utils_layer.zip"
REQUIREMENTS_LOCK="${REPO_ROOT}/apps/lambda/requirements.txt"
UTILS_DIR="${REPO_ROOT}/utils"

PYTHON_BIN="${PYTHON_BIN:-python3}"
LAMBDA_PYTHON_VERSION="3.12"
LAMBDA_PLATFORM="manylinux2014_x86_64"

# A fixed epoch mtime (2026-01-01T00:00:00Z) makes the zip byte-reproducible —
# two builds from the same inputs produce the same sha256, regardless of when
# or on which machine they ran.
REPRODUCIBLE_MTIME="2026-01-01 00:00:00"

echo "== build_lambda_layer.sh: cleaning ${BUILD_DIR} and ${ZIP_DIR} ==" >&2
rm -rf "${BUILD_DIR}" "${ZIP_DIR}"
mkdir -p "${LAYER_PYTHON_DIR}" "${ZIP_DIR}"

echo "== installing third-party dependencies (hash-pinned, from index) ==" >&2
# --platform/--python-version/--implementation/--only-binary=:all: force pip
# into "foreign target" resolution mode: it downloads wheels for the target
# (Lambda's runtime), never consults or reuses what happens to already be
# importable in the *building* interpreter. Skipping this flag set is a real
# footgun — a build host/venv that already has these exact package versions
# installed for its own purposes (a CI runner's tool venv, a developer's
# pytest venv) will otherwise cause pip to silently treat them as "already
# satisfied" and skip copying them into the target dir at all, producing a
# layer zip silently missing dependencies with a clean exit code.
"${PYTHON_BIN}" -m pip install \
    --require-hashes \
    --only-binary=:all: \
    --platform "${LAMBDA_PLATFORM}" \
    --python-version "${LAMBDA_PYTHON_VERSION}" \
    --implementation cp \
    --target "${LAYER_PYTHON_DIR}" \
    --requirement "${REQUIREMENTS_LOCK}"

echo "== installing dm_chain_utils as a PATH requirement (--no-deps) ==" >&2
"${PYTHON_BIN}" -m pip install \
    --no-deps \
    --target "${LAYER_PYTHON_DIR}" \
    "${UTILS_DIR}"

echo "== stripping build metadata that would break reproducibility ==" >&2
find "${LAYER_PYTHON_DIR}" -name '__pycache__' -type d -prune -exec rm -rf {} +
find "${LAYER_PYTHON_DIR}" -name '*.dist-info' -type d -print0 \
    | xargs -0 -I{} find {} -name 'RECORD' -delete

echo "== zipping ${LAYER_PYTHON_DIR} -> ${ZIP_PATH} (reproducible) ==" >&2
( \
    cd "${BUILD_DIR}" && \
    find python -type f -print0 \
        | sort -z \
        | while IFS= read -r -d '' f; do
            touch -d "${REPRODUCIBLE_MTIME}" "${f}"
          done && \
    find python -type f -print0 \
        | sort -z \
        | xargs -0 zip -X -q "${ZIP_PATH}" \
)

LAYER_SHA256="$(sha256sum "${ZIP_PATH}" | awk '{print $1}')"
# source_code_hash on aws_lambda_layer_version must be base64 of the RAW
# sha256 digest bytes (what AWS returns as Content.CodeSha256) -- the hex
# digest above is only for the content-addressed S3 key (T-R.2 F-02).
LAYER_SHA256_B64="$(echo -n "${LAYER_SHA256}" | xxd -r -p | base64)"

echo "== done ==" >&2
echo "LAYER_ZIP=${ZIP_PATH} LAYER_SHA256=${LAYER_SHA256} LAYER_SHA256_B64=${LAYER_SHA256_B64}"
