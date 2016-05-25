#!/bin/sh

# Display swarm cluster status

# @author  Fabrice Jammes, IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/swarm-env.sh"

export DOCKER_HOST=tcp://$SWARM_HOSTNAME:$SWARM_PORT

# Client node side
docker version
docker info

