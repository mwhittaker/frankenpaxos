#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.compartmentalized_lt.plot \
        --coupled_results "$d/coupled_results.csv" \
        --compartmentalized_results "$d/compartmentalized_results.csv" \
        --unreplicated_results "$d/unreplicated_results.csv" \
        --output "$d/../output/compartmentalized_lt.pdf"
}

main "$@"
