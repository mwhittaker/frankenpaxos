#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.craq_skew.plot \
        --results "$d/results.csv" \
        --output "$d/../output/craq_skew.pdf"
}

main "$@"
