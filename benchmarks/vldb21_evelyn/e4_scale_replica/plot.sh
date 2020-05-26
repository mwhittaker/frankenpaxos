#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.vldb21_evelyn.e4_scale_replica.plot \
        --results benchmarks/vldb21_evelyn/e4_scale_replica/results.csv \
        --output benchmarks/vldb21_evelyn/e4_scale_replica.pdf
}

main "$@"
