#!/bin/bash

# Copy data from container /qserv/data
# to temporary directory
# temporary directory might be mounted on host fs
# to host temporar 

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env.sh"

DATA_DIR="/qserv/data"
TMP_DIR="/qserv/run/tmp"

echo "Stop Qserv on all pods"
if ! "$DIR"/qserv-status.sh -a stop; then
    parallel "kubectl exec {} -- sh -c \
    '/qserv/run/bin/qserv-status.sh ; \
    if [ \$? -ne 127 ]; then \
        echo \"ERROR: failed to stop Qserv\"; exit 1; \
    else echo \"Qserv stopped\"; fi'" ::: $MASTER_POD $WORKER_PODS
fi

echo "Extract data from pods to host temporary directories"
parallel -v "kubectl exec {} -- cp -r $DATA_DIR $TMP_DIR" ::: $MASTER_POD $WORKER_PODS

