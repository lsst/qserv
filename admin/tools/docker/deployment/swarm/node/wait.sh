#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -h          this message

  Launch Qserv integration tests on one Docker host

EOD
}

while true; do
    # FIXME: add container name to avoid using wrong container
    CONTAINER_ID=$(docker ps -q)
    [ -n  "$CONTAINER_ID" ] && break
    echo "Waiting for Qserv container availability on $(hostname)"
    sleep 2
done

docker exec "$CONTAINER_ID" /qserv/scripts/wait.sh
