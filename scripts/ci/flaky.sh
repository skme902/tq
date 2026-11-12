#!/usr/bin/env bash
# Re-run tests listed in scripts/ci/flaky-tests.txt. Exit non-zero if any fail.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

count="${FLAKY_COUNT:-30}"
list="scripts/ci/flaky-tests.txt"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

python3 - "$list" "$work/cases.tsv" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
with open(src) as f, open(dst, "w") as out:
    for raw in f:
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        pkg, regex = line.split(None, 1)
        out.write(f"{pkg}\t{regex}\n")
PY

: >"$work/results.tsv"
while IFS=$'\t' read -r pkg regex; do
  echo "flaky: $pkg $regex x$count"
  json="$work/out.json"
  err="$work/err.txt"
  set +e
  go test -json -race -count="$count" -run "^(${regex})$" "$pkg" >"$json" 2>"$err"
  rc=$?
  set -e
  if [[ -s "$err" ]]; then
    cat "$err" >&2
  fi
  read -r runs fails < <(python3 - "$json" "$regex" "$count" "$rc" <<'PY'
import json, sys
path, name, count, rc = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
runs = fails = 0
shown = 0
for line in open(path):
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        ev = json.loads(line)
    except json.JSONDecodeError:
        continue
    test = ev.get("Test") or ""
    if test != name and not test.startswith(name + "/"):
        continue
    act = ev.get("Action")
    if test == name and act in ("pass", "skip"):
        runs += 1
    elif test == name and act == "fail":
        runs += 1
        fails += 1
    elif act == "output" and "--- FAIL:" in ev.get("Output", "") and shown < 30:
        sys.stderr.write(ev["Output"])
        if not ev["Output"].endswith("\n"):
            sys.stderr.write("\n")
        shown += 1
if runs == 0:
    # Missing test, or the package failed before the test ran.
    fails = count
elif rc != 0 and fails == 0:
    fails = 1
print(f"{runs} {fails}")
PY
)
  printf '%s\t%s\t%s\t%s\n' "$pkg" "$regex" "$runs" "$fails" >>"$work/results.tsv"
  echo "flaky: $pkg $regex runs=$runs failures=$fails"
done <"$work/cases.tsv"

{
  echo "### Flaky tests"
  echo
  echo "| package | test | runs | failures |"
  echo "|---|---|---:|---:|"
  while IFS=$'\t' read -r pkg regex runs fails; do
    echo "| ${pkg} | ${regex} | ${runs} | ${fails} |"
  done <"$work/results.tsv"
  echo
} >"$work/table.md"

cat "$work/table.md"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  cat "$work/table.md" >>"$GITHUB_STEP_SUMMARY"
fi

total=$(awk -F'\t' '{s += $4} END {print s+0}' "$work/results.tsv")
if [[ "$total" -gt 0 ]]; then
  echo "flaky: ${total} failure(s)" >&2
  exit 1
fi
