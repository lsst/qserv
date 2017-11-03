#!/bin/bash

# Show ip address for Qserv containers

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$HOME/.kube/env.sh"

parallel 'kubectl exec {} -- sh -c "printf \"host: %s, ip: %s\n\" \$(hostname) \$(hostname --ip-address)"' ::: $MASTER_POD $WORKER_PODS
