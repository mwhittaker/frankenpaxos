#! /usr/bin/env bash

set -euo pipefail

main() {
    python -m benchmarks.eurosys.fig4_multipaxos_ablation_plot \
        --unbatched_coupled_multipaxos_results \
            "benchmarks/eurosys/fig4_unbatched_coupled_multipaxos_results.csv" \
        --unbatched_multipaxos_results \
            "benchmarks/eurosys/fig4_unbatched_multipaxos_results.csv" \
        --batched_coupled_multipaxos_results \
            "benchmarks/eurosys/fig4_batched_coupled_multipaxos_results.csv" \
        --batched_multipaxos_results \
            "benchmarks/eurosys/fig4_batched_multipaxos_results.csv" \
        --output_unbatched_throughput \
            "benchmarks/eurosys/fig4_multipaxos_ablation_unbatched_throughput.pdf" \
        --output_batched_throughput \
            "benchmarks/eurosys/fig4_multipaxos_ablation_batched_throughput.pdf"
}

main "$@"
