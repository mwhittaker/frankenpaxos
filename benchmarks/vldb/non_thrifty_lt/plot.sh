#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb.non_thrifty_lt.plot \
        --thrifty_results "$d/thrifty_results.csv" \
        --non_thrifty_results "$d/non_thrifty_results.csv" \
        --output "$d/../output/non_thrifty_lt.pdf"
}

main "$@"
