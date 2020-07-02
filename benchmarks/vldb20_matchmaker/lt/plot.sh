#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb20_matchmaker.lt.plot \
        --results "$d/results.csv" \
        --output "$d/../output/lt.pdf"
}

main "$@"
