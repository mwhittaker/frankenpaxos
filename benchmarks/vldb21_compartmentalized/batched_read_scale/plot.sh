#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_compartmentalized.batched_read_scale.plot \
        --results "$d/results.csv" \
        --output "$d/../output/batched_read_scale.pdf"
}

main "$@"
