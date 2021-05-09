#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.scalog_lt.plot \
        --scalog_results "$d/results.csv" \
        --compartmentalized_results "$d/compartmentalized_results.csv" \
        --output "$d/../output/scalog_lt.pdf"
}

main "$@"
