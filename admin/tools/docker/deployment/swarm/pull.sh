#!/bin/sh

# Download Docker container related to Qserv integration tests

# @author  Fabrice Jammes, IN2P3/SLAC

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/swarm-env.sh"
. "${DIR}/env.sh"

export DOCKER_HOST=tcp://$SWARM_HOSTNAME:$SWARM_PORT

# Images have to be pulled to the whole cluster in
# current swarm version
docker pull "${MASTER_IMAGE}"
docker pull "${WORKER_IMAGE}"
