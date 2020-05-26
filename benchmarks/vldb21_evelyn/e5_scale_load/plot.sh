#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.vldb21_evelyn.e5_scale_load.plot \
        --results benchmarks/vldb21_evelyn/e5_scale_load/results.csv \
        --output benchmarks/vldb21_evelyn/e5_scale_load.pdf
}

main "$@"
