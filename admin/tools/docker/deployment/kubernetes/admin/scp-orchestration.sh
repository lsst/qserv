#!/bin/bash

# Copy administration scripts to kubernetes
# orchestration node

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/../env-cluster.sh"

echo "Copy administration scripts to kubernetes orchestration node"

ENV_FILE=$(readlink -m "$DIR/../env.sh")
if [ ! -f "$ENV_FILE" ]; then
    echo "FATAL: non-existent configuration file $ENV_FILE"
    exit 1
fi
scp -F "$SSH_CFG" -r "$DIR/../orchestration" "$ORCHESTRATOR":/home/qserv
scp -F "$SSH_CFG" -r "$ENV_FILE" "${ORCHESTRATOR}:/home/qserv/orchestration"
scp -F "$SSH_CFG" "$ENV_INFRASTRUCTURE_FILE" "${ORCHESTRATOR}:/home/qserv/orchestration"

