#! /usr/bin/env bash

set -euo pipefail

main() {
    local -r d="$(dirname $0)"
    python -m benchmarks.vldb21_evelyn.lt_surprise.plot \
        --results "$d/results.csv" \
        --output_with "$d/../output/lt_with.pdf" \
        --output_without "$d/../output/lt_without.pdf"
}

main "$@"
