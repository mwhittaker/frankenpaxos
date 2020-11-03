#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.weak_read_scale.plot \
        --results "$d/eventual_results.csv" \
        --output "$d/../output/weak_read_scale_eventual.pdf"
    python -m benchmarks.vldb21_compartmentalized.weak_read_scale.plot \
        --results "$d/sequential_results.csv" \
        --output "$d/../output/weak_read_scale_sequential.pdf"
}

main "$@"
