#!/usr/bin/env bash
# gofmt, go vet, and staticcheck for the root module.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

unformatted=$(git ls-files -z '*.go' | xargs -0 -r gofmt -l)
if [[ -n "$unformatted" ]]; then
  printf '%s\n' "$unformatted"
  exit 1
fi

go vet ./...

sc=$(command -v staticcheck || true)
if [[ -z "$sc" ]]; then
  sc="$(go env GOPATH)/bin/staticcheck"
fi
if [[ ! -x "$sc" ]]; then
  echo "lint.sh: staticcheck not found on PATH or in \$(go env GOPATH)/bin" >&2
  echo "install: go install honnef.co/go/tools/cmd/staticcheck@2026.2.2" >&2
  exit 1
fi
"$sc" ./...
