#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_evelyn.scale_load.plot \
        --results "$d/results.csv" \
        --output "$d/../output/scale_load.pdf"
}

main "$@"
