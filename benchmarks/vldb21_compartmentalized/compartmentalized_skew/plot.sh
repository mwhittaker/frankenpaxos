#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.compartmentalized_skew.plot \
        --compartmentalized_results "$d/results.csv" \
        --craq_results "$d/craq_results.csv" \
        --output "$d/../output/compartmentalized_skew.pdf"
}

main "$@"
