#!/usr/bin/env bash
# Race tests for this module. The parity module is vetted and built only:
# its tests need an upstream Temporal clone.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

if [[ "${CI_REQUIRE_POSTGRES:-}" == "1" && -z "${TQ_TEST_POSTGRES_DSN:-}" ]]; then
  echo "test.sh: CI_REQUIRE_POSTGRES=1 but TQ_TEST_POSTGRES_DSN is unset" >&2
  exit 1
fi

if [[ -n "${TQ_TEST_POSTGRES_DSN:-}" ]]; then
  echo "Postgres tests: enabled (TQ_TEST_POSTGRES_DSN is set)"
else
  echo "Postgres tests: skipped (TQ_TEST_POSTGRES_DSN is unset)"
fi

if [[ -n "${TEST_JSON:-}" ]]; then
  mkdir -p "$(dirname -- "$TEST_JSON")"
  # Keep the step log readable; the file is the raw JSON.
  go test -json -race -count=1 ./... | tee "$TEST_JSON" | python3 -c '
import json, sys
for line in sys.stdin:
    s = line.strip()
    if not s.startswith("{"):
        sys.stdout.write(line)
        continue
    try:
        ev = json.loads(s)
    except json.JSONDecodeError:
        sys.stdout.write(line)
        continue
    if ev.get("Action") == "output":
        sys.stdout.write(ev.get("Output") or "")
'
else
  go test -race -count=1 ./...
fi

echo "parity: go vet ./... && go build ./..."
( cd parity && go vet ./... && go build ./... )
