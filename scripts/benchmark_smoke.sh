#! /usr/bin/env bash

set -euo pipefail

main() {
    # for protocol in unreplicated batchedunreplicated craq multipaxos \
    #                 fasterpaxos supermultipaxos vanillamencius mencius \
    #                 supermencius fastmultipaxos epaxos simplebpaxos \
    #                 superbpaxos simplegcbpaxos unanimousbpaxos \
    #                 matchmakermultipaxos horizontal scalog; do
    for protocol in scalog; do
        echo "Running $protocol."
        python -m "benchmarks.${protocol}.smoke" \
            -m \
            --cluster "benchmarks/${protocol}/local_cluster.json" \
            -i ~/.ssh/id_rsa
        echo
    done
}

main "$@"
