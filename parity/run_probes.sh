#!/usr/bin/env bash
# Vetting runs: det-fair ablations (ours vs the upstream det-fair reference) and system-limit probes (both impls).
set -euo pipefail
cd "$(dirname "$0")"
rm -rf scripts-ablate scripts-probe traces/probe; mkdir -p traces/probe results bin
[[ -f scripts/det-fair.json ]] || go run ./cmd/gen -out scripts
python3 probes.py
go build -o bin/ours ./cmd/ours
ref=traces/probe/det-fair.upstream-sqlite-p1.0.json
./run_upstream.sh "$PWD/scripts/det-fair.json" "$PWD/$ref" sqlite 1 0 >/dev/null
for s in scripts-ablate/*.json; do ./bin/ours -script "$s" -out "traces/probe/ablate-$(basename "$s")" -store mem -run 0; done
{
echo "## Ablations (ours/mem, each row one feature removed; compared with the full upstream det-fair run)"
for s in scripts-ablate/*.json; do n=$(basename "$s" .json); echo; echo "### $n"
  go run ./cmd/compare -scripts scripts "$ref" "traces/probe/ablate-$n.json" | grep -E '^\| ours/mem/p1 \| 0 \| det-'; done
echo; echo "## Held leases (ours only: each lease acked after -hold-ms; upstream acks at start)"
echo "Order must still match upstream det-fair (leases do not hold back reads), with 0 lost and 0 dup."
for st in mem sqlite; do
  ./bin/ours -script scripts/det-fair.json -out "traces/probe/hold-det-fair.ours-$st.json" -store "$st" -run 0 -hold-ms 2000
  ./bin/ours -script scripts/churn.json -out "traces/probe/hold-churn.ours-$st.json" -store "$st" -run 0 -hold-ms 500
done
echo; echo "### det-fair, hold 2000 ms"
go run ./cmd/compare -scripts scripts "$ref" traces/probe/hold-det-fair.ours-*.json | grep -E '^\| ours/'
echo; echo "### churn, hold 500 ms (delivery counts only; churn order is not deterministic)"
go run ./cmd/compare -scripts scripts traces/probe/hold-churn.ours-*.json | grep -E '^\| ours/[a-z]+/p1 \| 0 \| [0-9]'
echo; echo "## System-limit probes (4000 backlog, 4 pollers)"; echo
for s in scripts-probe/*.json; do n=$(basename "$s" .json)
  ./run_upstream.sh "$PWD/$s" "$PWD/traces/probe/$n.up.json" sqlite 1 0 >/dev/null
  ./bin/ours -script "$s" -out "traces/probe/$n.ours-mem.json" -store mem -run 0
  ./bin/ours -script "$s" -out "traces/probe/$n.ours-sqlite.json" -store sqlite -run 0
done
python3 - <<'PY'
import json,glob
print('| trace | delivered | complete | drain s | in first 1s | rate after 1s |'); print('|---|---|---|---|---|---|')
for f in sorted(glob.glob('traces/probe/probe-*.json')):
    t=json.load(open(f)); s=t['marks']['poll_start']; d=sorted(x['at'] for x in t['deliveries'])
    tail=[x for x in d if x>=s+1e9]; r=len(tail)/((tail[-1]-tail[0])/1e9) if len(tail)>1 else float('nan')
    print(f"| {f.split('/')[-1][:-5]} | {len(d)} | {t['complete']} | {(d[-1]-s)/1e9:.2f} | {len([x for x in d if x<s+1e9])} | {r:.0f} |")
print()
print('## Config-change lag (4 pollers at 50 rps; limit unset or raised at +2 s; lag = first 10 deliveries within 50 ms)')
print(); print('| trace | lag ms |'); print('|---|---|')
for f in sorted(glob.glob('traces/probe/lag-*.json')):
    t=json.load(open(f)); c=t['marks']['change']; d=sorted(x['at'] for x in t['deliveries'] if x['at']>=c)
    lag=next(((d[i]-c)/1e6 for i in range(len(d)-10) if d[i+10]-d[i]<0.05e9), float('nan'))
    print(f"| {f.split('/')[-1][:-5]} | {lag:.0f} |")
PY
} > results/probes.md
cat results/probes.md
