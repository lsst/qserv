#!/bin/bash

# Launch Qserv multinode tests on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

# Build CSS input data
i=1
for node in $WORKERS;
do
# TODO report bug in Jira for A. Salnikov?
# FIXME: update qserv-admin.py: AttributeError: 'CssAccess' object has no attribute 'setNodeStatus'
#    CSS_INFO="${CSS_INFO}UPDATE NODE worker${i} state=INACTIVE;"
    CSS_INFO="${CSS_INFO}DELETE NODE worker${i};"
    i=$((i+1))
done

ssh -F "$SSH_CFG" "$ORCHESTRATOR" "kubectl exec master -- bash -c '. /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py'"

