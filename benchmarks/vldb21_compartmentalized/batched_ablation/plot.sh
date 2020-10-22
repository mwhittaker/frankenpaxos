#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.batched_ablation.plot \
        --coupled_results "$d/coupled_results.csv" \
        --compartmentalized_results "$d/compartmentalized_results.csv" \
        --output "$d/../output/batched_ablation.pdf"
}

main "$@"
