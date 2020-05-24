#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.vldb21_evelyn.e3_no_scale_fraction.plot \
        --results benchmarks/vldb21_evelyn/e3_no_scale_fraction/results.csv \
        --output benchmarks/vldb21_evelyn/e3_no_scale_fraction.pdf
}

main "$@"
