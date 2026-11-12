#!/usr/bin/env bash
# Bench smoke: the run must succeed. No thresholds (shared runners are noisy).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

out_dir="${OUT_DIR:-/tmp/tq-bench}"
mkdir -p "$out_dir"
go test -run '^$' -bench 'BenchmarkEngine|BenchmarkStoreCommit' -benchtime 2000x . | tee "$out_dir/bench.txt"
