#!/usr/bin/env bash
# Runs every script through upstream and our library, RUNS repeats each, sequentially (no
# CPU contention between implementations), then writes the comparison to results/.
set -euo pipefail
cd "$(dirname "$0")"
RUNS=${RUNS:-3}
OUT=${OUT:-traces/full}
mkdir -p "$OUT" results bin
go run ./cmd/gen -out scripts
go build -o bin/ours ./cmd/ours
for s in scripts/*.json; do
  n=$(basename "$s" .json)
  for r in $(seq 0 $((RUNS-1))); do
    echo "$(date +%T) $n run $r"
    ./run_upstream.sh "$PWD/$s" "$PWD/$OUT/$n.upstream-sqlite-p1.$r.json" sqlite 1 "$r"
    ./bin/ours -script "$s" -out "$OUT/$n.ours-mem.$r.json" -store mem -run "$r"
    ./bin/ours -script "$s" -out "$OUT/$n.ours-sqlite.$r.json" -store sqlite -run "$r"
    ./run_upstream.sh "$PWD/$s" "$PWD/$OUT/$n.upstream-sqlite-p4.$r.json" sqlite 4 "$r"
  done
  ./run_upstream.sh "$PWD/$s" "$PWD/$OUT/$n.upstream-mem-p1.0.json" mem 1 0
done
go run ./cmd/compare -scripts scripts "$OUT"/*.json > results/compare.md
echo "$(date +%T) done -> results/compare.md"
