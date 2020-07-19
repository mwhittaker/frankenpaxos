#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_evelyn.no_scale_fraction.plot \
        --results "$d/results.csv" \
        --output "$d/../output/no_scale_fraction.pdf"
}

main "$@"
