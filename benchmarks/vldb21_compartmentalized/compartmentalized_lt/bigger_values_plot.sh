#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.compartmentalized_lt.bigger_values_plot \
        --compartmentalized_results "$d/bigger_values_results.csv" \
        --unreplicated_results "$d/unreplicated_bigger_values_results.csv" \
        --output "$d/../output/bigger_values_compartmentalized_lt.pdf"
}

main "$@"
