#!/bin/bash

# Remove Qserv logfiles on all nodes
# Used for debugging purpose

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env.sh"

echo "Remove Qserv logfiles on all nodes"
parallel "kubectl exec {} -- sh -c 'rm /qserv/run/var/log/*.log'" ::: $MASTER_POD $WORKER_PODS
