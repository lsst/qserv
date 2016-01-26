#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -x
set -e

. ./env.sh


usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -h          this message
    -C          Rebuild the images from scratch

  Launch Qserv integration tests on one Docker host

EOD
}

# get the options
while getopts h c ; do
    case $c in
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

# Start dynamic DNS for local Docker containers
docker rm -f docker_spy || echo "No existing container for docker_spy"

# WARNING: Passing the docker sock down into a container is a fairly risky thing to do.
# docker-spy needs it to track docker container creation/removal and manage
# automatically related DNS entries
docker run --name docker_spy --detach=true -e "DNS_DOMAIN=$DNS_DOMAIN" -v /var/run/docker.sock:/var/run/docker.sock iverberk/docker-spy

DNS_IP=$(docker inspect -f '{{ .NetworkSettings.IPAddress }}' docker_spy)

docker rm -f "$MASTER" || echo "No existing container for $MASTER"
docker run --detach=true --dns="$DNS_IP" --dns-search="$DNS_DOMAIN" --name "$MASTER" -h "${MASTER}" "$MASTER_IMAGE"

for i in $WORKERS;
do
    docker rm -f "$i" || echo "No existing container for $i"
    docker run --detach=true --dns="$DNS_IP" --dns-search="$DNS_DOMAIN" --name "$i" -h "${i}"  "$WORKER_IMAGE"
done

# Wait for Qserv services to be up and running
for i in $MASTER $WORKERS;
do
    docker exec "$i" /qserv/scripts/wait.sh
done


for i in $(seq 1 "$NB_WORKERS");
do
    CSS_INFO="${CSS_INFO}CREATE NODE worker${i} type=worker port=5012 host=worker${i}.$DNS_DOMAIN;
"
done
docker exec "$MASTER" bash -c ". /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py && \
    qserv-test-integration.py"
