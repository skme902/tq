# CI

Workflows live in `.github/workflows/`. Local entry points are `scripts/ci/`.

## Pull requests and main

`.github/workflows/ci.yml` runs on push to `main`, on every pull request, and on `workflow_dispatch`.

| Job | What it does |
|---|---|
| `lint` | `scripts/ci/lint.sh`: `gofmt -l` on every tracked `*.go` file (both modules); `go vet ./...` and `staticcheck ./...` in the root module only (`parity/` is vetted by `test.sh`, not staticchecked) |
| `test` | Postgres 17, then `scripts/ci/test.sh`: `go test -race -count=1 ./...`, then `go vet` and `go build` in the `parity/` module. |

The test job sets `TQ_TEST_POSTGRES_DSN` so Postgres tests run, and `CI_REQUIRE_POSTGRES=1` so the job fails if that DSN is missing. SQLite and memstore tests always run. On failure the job uploads the `go test -json` log as the `go-test-json` artifact.

`parity/` is a separate module. Its tests need an upstream Temporal clone, so CI only vets and builds it. `go build ./...` there does not need the clone: the upstream adapter is behind the `parity_upstream` build tag.

## Nightly

`.github/workflows/nightly.yml` runs at cron `17 21 * * *` (02:47 IST) and on `workflow_dispatch`. GitHub runs schedules only from the default branch, so nightly starts once this is on `main`. The `flaky`, `parity`, and `bench` jobs do not depend on each other.

| Job | What it does |
|---|---|
| `flaky` | `scripts/ci/flaky.sh` with `FLAKY_COUNT=30` and Postgres 17. Any failure fails the job. |
| `parity` | Clones `temporalio/temporal` v1.32.1 into `$RUNNER_TEMP/temporal` and runs `scripts/ci/parity.sh`. Uploads `probes.md` and the probe log. |
| `bench` | Postgres 17, then `scripts/ci/bench-smoke.sh`. Uploads `bench.txt`. No thresholds. |

## Running checks locally

Run the scripts from anywhere; they `cd` to the repo root.

### Postgres 17

Postgres tests skip unless `TQ_TEST_POSTGRES_DSN` is set. Each test creates and drops its own schema, so the role needs `CREATE` (the `postgres` superuser does).

```bash
docker run -d --name tq-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=tqtest \
  -p 5432:5432 postgres:17
export TQ_TEST_POSTGRES_DSN='postgres://postgres:postgres@localhost:5432/tqtest?sslmode=disable'
```

`CI_REQUIRE_POSTGRES=1 scripts/ci/test.sh` fails immediately when the DSN is unset.

### Lint

```bash
go install honnef.co/go/tools/cmd/staticcheck@2026.2.2   # the version CI pins
scripts/ci/lint.sh
```

`lint.sh` uses `staticcheck` on `PATH`, or `$(go env GOPATH)/bin/staticcheck`.

### Tests

```bash
scripts/ci/test.sh
TEST_JSON=/tmp/tq-test.json scripts/ci/test.sh   # also writes go test -json
```

The script prints whether Postgres tests are enabled, runs the race tests, then vets and builds `parity/`.

### Flaky list

`scripts/ci/flaky-tests.txt` lines are `<package> <TestName>` (an exact top-level test name; patterns are rejected), with `#` comments. A skipped run counts as a failure, and each `go test` gets `-timeout ${FLAKY_TIMEOUT:-15m}`. Nightly runs each one with `-race` and `-count` (default 30):

```bash
FLAKY_COUNT=30 scripts/ci/flaky.sh
```

The script prints a markdown table (`package | test | runs | failures`) and appends it to `$GITHUB_STEP_SUMMARY` when that variable is set.

PR CI still runs the full `go test` suite, including every test named in the flaky list, and a failure there fails the job. The nightly job is what repeats those tests. A test stays in `flaky-tests.txt` until it has passed nightly repeatedly.

### Parity

Parity compares the deterministic cells of a fresh `parity/results/probes.md` with the committed file. It needs a clone of [temporalio/temporal](https://github.com/temporalio/temporal) at v1.32.1, commit `af08a5e5`. The script checks `git rev-parse HEAD` and refuses any other commit. It restores the committed `probes.md` before it exits. `scripts/ci/probecmp.py` compares the deterministic cells only: for `| ours/` delivery rows the name, run, script, the four scores and `complete`; for `| probe-` system-limit rows the delivered count and `complete`. Timing cells and the config-change lag table are not compared. A mismatch (`probe rows N M mismatches K` with `K > 0`) fails the script.

```bash
# Clone yourself:
git clone --depth 1 --branch v1.32.1 https://github.com/temporalio/temporal /tmp/temporal
UPSTREAM_DIR=/tmp/temporal OUT_DIR=/tmp/tq-parity scripts/ci/parity.sh

# Or let the script clone to upstream/temporal (gitignored):
CLONE_UPSTREAM=1 scripts/ci/parity.sh
```

Fresh output is `${OUT_DIR:-/tmp/tq-parity}/probes.md` plus `run_probes.log`. See also [`parity/README.md`](../parity/README.md).

### Bench smoke

```bash
scripts/ci/bench-smoke.sh
```

This runs `go test -run '^$' -bench 'BenchmarkEngine|BenchmarkStoreCommit' -benchtime 2000x .` and writes `${OUT_DIR:-/tmp/tq-bench}/bench.txt`. The command must succeed. There is no performance gate. Postgres benches run only when `TQ_TEST_POSTGRES_DSN` is set; otherwise they skip.
