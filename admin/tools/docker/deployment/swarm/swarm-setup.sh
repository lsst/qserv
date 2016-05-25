#!/bin/sh

# Configure a swarm cluster for Qserv

# @author  Fabrice Jammes, IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/swarm-env.sh"

# DEBUG_OPT="--debug -l debug"

export TOKEN=$(docker run --rm swarm create)
echo "Docker Swarm token: $TOKEN"

# Swarm node side
# Join swarm nodes
for i in $(seq 0 $INSTANCE_LAST_ID)
do
    HOSTNAME="$HOSTNAME_TPL$i"
    HOST_IP=$(getent hosts "$HOSTNAME" | awk '{{ print $1 }}')
    HOSTFILE_OPT="--add-host $HOSTNAME:$HOST_IP"
    echo "Join swarm node on $HOSTNAME"
    docker run $HOSTFILE_OPT -d swarm $DEBUG_OPT join --addr="$HOSTNAME:$DOCKER_PORT" "token://$TOKEN"
    MANAGER_HOSTFILE_OPT="$MANAGER_HOSTFILE_OPT $HOSTFILE_OPT"
done

echo "Launch swarm manager"
docker run $MANAGER_HOSTFILE_OPT -d -p "$SWARM_PORT:$SWARM_PORT" swarm $DEBUG_OPT manage --host=0.0.0.0:$SWARM_PORT token://$TOKEN


# Wait for all swarm nodes to be in "Healthy" status
# Swarm 'info' interface is weak
PENDING="TRUE"
while [ -n "$PENDING" ]
do
    STATUS=$(docker -H tcp://$SWARM_HOSTNAME:$SWARM_PORT info | grep "Status: " || true)
    if [ -n "$STATUS" ]; then
        PENDING=$(echo "$STATUS" | grep "Pending" || true)
        echo "Waiting for all swarm node to reach 'Healthy' status"
        sleep 1
    fi
done

echo "Docker Swarm cluster ready"
