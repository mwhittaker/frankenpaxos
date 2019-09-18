#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.nsdi.fig2_ablation_plot \
        --superbpaxos_results benchmarks/nsdi/fig2_ablation_superbpaxos_results.csv \
        --simplebpaxos_results benchmarks/nsdi/fig2_ablation_simplebpaxos_results.csv
}

main "$@"
