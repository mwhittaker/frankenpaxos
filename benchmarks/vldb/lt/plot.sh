#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.lt.plot \
        --results "$d/results.csv" \
        --output "$d/lt.pdf"
}

main "$@"
