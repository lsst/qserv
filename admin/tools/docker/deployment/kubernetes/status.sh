#!/bin/bash

# Get status for Qserv pods and services 

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

echo
echo "Check that Qserv master and workers pods are running on all nodes"
echo "================================================================="
echo
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "kubectl get pods -l app=qserv"

echo
echo "Check that Qserv services are running on all these pods"
echo "======================================================="
echo
ssh $SSH_CFG_OPT "$ORCHESTRATOR" "$ORCHESTRATION_DIR/qserv-status.sh"
