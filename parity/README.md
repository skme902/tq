# parity: differential harness against upstream Temporal matching

This is a separate Go module (`github.com/skme902/tq/parity`). The library does not depend on it or on
Temporal. Results and method: [`../docs/PARITY_REPORT.md`](../docs/PARITY_REPORT.md).

## Prerequisites

- Go (the version in `go.mod`) and python3.
- **A separate clone of upstream Temporal at v1.32.1 (commit af08a5e5).** It is not part of this repo:

  ```
  git clone --branch v1.32.1 --depth 1 https://github.com/temporalio/temporal ../upstream/temporal
  git -C ../upstream/temporal rev-parse --short=8 HEAD   # af08a5e5
  ```

  The default location is `<repo>/upstream/temporal`, which is gitignored. Point `UPSTREAM_DIR` at a clone in
  another location. The clone is never modified: the adapter (`upstream/parity_upstream_test.go`, build tag
  `parity_upstream`) and the shared driver are injected with `go test -overlay`. The adapter uses unexported
  upstream internals, so other upstream versions may not build.

## Running

```
./run_all.sh        # RUNS=3 by default; about 15 min. Generates scripts/, writes traces/ and results/compare.md
./run_probes.sh     # ablations, system-limit and config-lag probes -> results/probes.md (about 75 s)
```

All paths are relative to this directory, so no absolute paths are needed. Generated files are gitignored and
rebuilt on demand: `scripts*/` (seeded and deterministic), `traces/`, `bin/` and `upstream/gen/` (overlay and the
compiled upstream test binary). `results/*.md` are the committed outputs that the report cites.
`FINDINGS.log` records every anomaly found and how it was resolved.

The Temporal-derived adapter code carries attribution headers. See `../UPSTREAM_LICENSE` and `../NOTICE`.
