#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.lt_plot \
        --results "$d/lt_results.csv" \
        --output "$d/lt.pdf"
}

main "$@"
