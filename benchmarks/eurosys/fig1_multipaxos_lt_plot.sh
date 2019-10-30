#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.eurosys.fig1_multipaxos_lt_plot \
        --unbatched_coupled_multipaxos_results \
            "benchmarks/eurosys/fig1_unbatched_coupled_multipaxos_results.csv" \
        --unbatched_multipaxos_results \
            "benchmarks/eurosys/fig1_unbatched_multipaxos_results.csv" \
        --unbatched_unreplicated_results \
            "benchmarks/eurosys/fig1_unbatched_unreplicated_results.csv" \
        --batched_coupled_multipaxos_results \
            "benchmarks/eurosys/fig1_batched_coupled_multipaxos_results.csv" \
        --batched_multipaxos_results \
            "benchmarks/eurosys/fig1_batched_multipaxos_results.csv" \
        --batched_unreplicated_results \
            "benchmarks/eurosys/fig1_batched_unreplicated_results.csv" \
        --output_unbatched \
            "benchmarks/eurosys/fig1_multipaxos_lt_unbatched.pdf" \
        --output_batched \
            "benchmarks/eurosys/fig1_multipaxos_lt_batched.pdf"
}

main "$@"
