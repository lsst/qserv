#!/bin/bash

# Launch Qserv multinode tests on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-infrastructure.sh"
SSH_CFG="$DIR/ssh_config"

# Build CSS input data
i=1
for node in $WORKERS;
do
    CSS_INFO="${CSS_INFO}CREATE NODE worker${i} type=worker port=5012 \
host=worker-${i}.qserv.default.svc.cluster.local; "
    i=$((i+1))
done

ssh -F "$SSH_CFG" "$ORCHESTRATOR" "kubectl exec master -- bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py && \
    qserv-test-integration.py -V DEBUG'"

