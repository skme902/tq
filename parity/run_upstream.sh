#!/usr/bin/env bash
# Usage: run_upstream.sh SCRIPT TRACE [STORE=sqlite|mem] [PARTITIONS=1] [RUN=0]
# Builds upstream service/matching (pristine clone, untouched) with two overlay files:
#   parity_driver_test.go   = parity/driver/driver.go with `package matching`
#   parity_upstream_test.go = parity/upstream/parity_upstream_test.go
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# Upstream clone: UPSTREAM_DIR, default ../upstream/temporal. Fetch it separately (see parity/README.md):
#   git clone --branch v1.32.1 https://github.com/temporalio/temporal ../upstream/temporal  # commit af08a5e5
upd="${UPSTREAM_DIR:-$here/../upstream/temporal}"
if [[ ! -f "$upd/service/matching/matching_engine.go" ]]; then
  echo "run_upstream.sh: upstream clone not found at $upd (set UPSTREAM_DIR; see parity/README.md)" >&2; exit 2
fi
up=$(cd "$upd" && pwd)
gen="$here/upstream/gen"; mkdir -p "$gen"
sed 's/^package driver$/package matching/' "$here/driver/driver.go" > "$gen/parity_driver_test.go"
cat > "$gen/overlay.json" <<JSON
{"Replace":{
 "$up/service/matching/parity_driver_test.go":"$gen/parity_driver_test.go",
 "$up/service/matching/parity_upstream_test.go":"$here/upstream/parity_upstream_test.go"}}
JSON
bin="$gen/matching.test"
if [[ ! -x "$bin" || "$here/driver/driver.go" -nt "$bin" || "$here/upstream/parity_upstream_test.go" -nt "$bin" ]]; then
  (cd "$up" && go test -tags parity_upstream -overlay "$gen/overlay.json" -c -o "$bin" ./service/matching)
fi
PARITY_SCRIPT="$1" PARITY_TRACE="$2" PARITY_STORE="${3:-sqlite}" PARITY_PARTITIONS="${4:-1}" PARITY_RUN="${5:-0}" \
  "$bin" -test.run '^TestParityHarness$' -test.count=1 -test.timeout=30m >/dev/null
