#! /usr/bin/env bash

set -euo pipefail

main() {
    for protocol in unreplicated batchedunreplicated multipaxos \
                    supermultipaxos mencius supermencius fastmultipaxos epaxos \
                    simplebpaxos superbpaxos simplegcbpaxos unanimousbpaxos \
                    matchmakermultipaxos; do
        echo "Running $protocol."
        python -m "benchmarks.${protocol}.smoke" \
            -m \
            --cluster "benchmarks/${protocol}/local_cluster.json" \
            -i ~/.ssh/id_rsa
        echo
    done
}

main "$@"
