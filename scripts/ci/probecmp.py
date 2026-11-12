#!/usr/bin/env python3
"""probecmp.py BASE NEW: compare the deterministic cells of parity/results/probes.md files.

Rows compared (timing cells masked):
  | ours/... |   delivery rows: name, run, script and the four order/fairness scores, and complete
  | probe-... |  system-limit rows: trace, delivered, complete
Config-change lag rows (| lag-... |) are timing only and are not compared.
Exits 1 on any mismatch or row-count difference."""
import sys

def rows(text):
    out = []
    for l in text.splitlines():
        if not l.startswith(("| ours/", "| probe-")):
            continue
        r = [c.strip() for c in l.strip().strip("|").split("|")]
        if r[0].startswith("probe-"):
            r = r[:3]
        elif len(r) == 14:
            r = r[:7] + [r[12]]
        out.append(r)
    return out

def main(base, new):
    b, n = rows(open(base).read()), rows(open(new).read())
    mm = [(i, x, y) for i, (x, y) in enumerate(zip(b, n)) if x != y]
    k = len(mm) + abs(len(b) - len(n))
    print(f"probe rows {len(b)} {len(n)} mismatches {k}")
    for i, x, y in mm[:12]:
        print("  row", i, "base", " | ".join(x), "\n         new ", " | ".join(y))
    return 1 if k else 0

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: probecmp.py BASE NEW")
    sys.exit(main(sys.argv[1], sys.argv[2]))
