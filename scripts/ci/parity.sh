#!/usr/bin/env bash
# Regenerate parity/results/probes.md against upstream and compare it to the committed file.
# The committed file is restored before this script exits.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

if [[ -z "${UPSTREAM_DIR:-}" ]]; then
  if [[ "${CLONE_UPSTREAM:-}" == "1" ]]; then
    UPSTREAM_DIR="$PWD/upstream/temporal"
    if [[ ! -d "$UPSTREAM_DIR/.git" ]]; then
      mkdir -p "$(dirname "$UPSTREAM_DIR")"
      git clone --depth 1 --branch v1.32.1 https://github.com/temporalio/temporal "$UPSTREAM_DIR"
    fi
  else
    echo "parity.sh: set UPSTREAM_DIR, or CLONE_UPSTREAM=1 to clone into upstream/temporal" >&2
    exit 1
  fi
fi
export UPSTREAM_DIR

rev=$(git -C "$UPSTREAM_DIR" rev-parse HEAD)
if [[ "$rev" != af08a5e5* ]]; then
  echo "parity.sh: $UPSTREAM_DIR HEAD $rev does not start with af08a5e5" >&2
  exit 1
fi
echo "upstream: $rev"

committed="parity/results/probes.md"
if [[ ! -f "$committed" ]]; then
  echo "parity.sh: missing $committed" >&2
  exit 1
fi
backup=$(mktemp)
cp -- "$committed" "$backup"
trap 'cp -- "$backup" "$committed"; rm -f "$backup"' EXIT

out_dir="${OUT_DIR:-/tmp/tq-parity}"
mkdir -p "$out_dir"

echo "running parity/run_probes.sh"
set +e
( cd parity && ./run_probes.sh ) >"$out_dir/run_probes.log" 2>&1
rc=$?
set -e

cp -- "$committed" "$out_dir/probes.md"
cp -- "$backup" "$committed"

if [[ "$rc" -ne 0 ]]; then
  echo "parity.sh: run_probes.sh exited $rc; log: $out_dir/run_probes.log" >&2
  tail -n 80 "$out_dir/run_probes.log" >&2 || true
  exit "$rc"
fi

python3 scripts/ci/probecmp.py "$committed" "$out_dir/probes.md"
