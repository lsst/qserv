#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -x
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
    CONTAINER_ID=$(docker ps -q)
    [ -n  "$CONTAINER_ID" ] && break
	sleep 1
done

docker exec "$CONTAINER_ID" /qserv/scripts/wait.sh
