#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

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

docker rm -f "$MASTER" || echo "No existing container for $MASTER"
docker run --detach=true \
    -e "QSERV_MASTER=$MASTER" --name "$MASTER" -h "${MASTER}" "$MASTER_IMAGE"
MASTER_IP=$(docker inspect -f '{{ .NetworkSettings.IPAddress }}' $MASTER)

for i in $WORKERS;
do
    docker rm -f "$i" || echo "No existing container for $i"
    docker run --detach=true --add-host $MASTER:$MASTER_IP\
        -e "QSERV_MASTER=$MASTER" --name "$i" -h "${i}"  "$WORKER_IMAGE"
	WORKER_IP=$(docker inspect -f '{{ .NetworkSettings.IPAddress }}' $i)
	HOSTFILE="${HOSTFILE}$WORKER_IP    $i\n"
done

# Add worker entries to master hostfile, for data-loading
docker exec -u root "$MASTER" sh -c "echo '$HOSTFILE' >> /etc/hosts"

# Wait for Qserv services to be up and running
for i in $MASTER $WORKERS;
do
    docker exec "$i" /qserv/scripts/wait.sh
done


for i in $(seq 1 "$NB_WORKERS");
do
    CSS_INFO="${CSS_INFO}CREATE NODE worker${i} type=worker port=25012 host=worker${i}.$DNS_DOMAIN;
"
done
docker exec "$MASTER" bash -c ". /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py && \
    qserv-test-integration.py"
