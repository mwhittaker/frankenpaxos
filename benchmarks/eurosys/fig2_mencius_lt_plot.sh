#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.eurosys.fig2_mencius_lt_plot \
        --unbatched_coupled_mencius_results \
            "benchmarks/eurosys/fig2_unbatched_coupled_mencius_results.csv" \
        --unbatched_mencius_results \
            "benchmarks/eurosys/fig2_unbatched_mencius_results.csv" \
        --unbatched_unreplicated_results \
            "benchmarks/eurosys/fig2_unbatched_unreplicated_results.csv" \
        --batched_coupled_mencius_results \
            "benchmarks/eurosys/fig2_batched_coupled_mencius_results.csv" \
        --batched_mencius_results \
            "benchmarks/eurosys/fig2_batched_mencius_results.csv" \
        --batched_unreplicated_results \
            "benchmarks/eurosys/fig2_batched_unreplicated_results.csv" \
        --output_unbatched \
            "benchmarks/eurosys/fig2_mencius_lt_unbatched.pdf" \
        --output_batched \
            "benchmarks/eurosys/fig2_mencius_lt_batched.pdf"
}

main "$@"
