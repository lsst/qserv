#!/bin/sh


set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/multiczar-env.sh"

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -h          this message

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
    --privileged \
    --cap-add sys_admin \
    --cap-add sys_ptrace \
    -v /home/jgates/work/qserv_testdata:/qserv/qserv_testdata \
    -e "QSERV_MASTER=$MASTER" --name "$MASTER" -h "${MASTER}" "$MASTER_SHARED_IMAGE"
MASTER_IP=$(docker inspect -f '{{ .NetworkSettings.IPAddress }}' $MASTER)
HOSTFILE="${HOSTFILE}$MASTER_IP  master.localdomain
"

for i in $CZARS;
do
docker run --detach=true \
    --privileged \
    --cap-add sys_admin \
    --cap-add sys_ptrace \
    -e "QSERV_MASTER=${i}" --name "$i" -h "${i}" "$MASTER_MULTI_IMAGE"
    CZAR_IP=$(docker inspect -f '{{ .NetworkSettings.IPAddress }}' $i)
    HOSTFILE="${HOSTFILE}$CZAR_IP  $i
"
done

for i in $WORKERS;
do
    docker rm -f "$i" || echo "No existing container for $i"
    docker run --detach=true --add-host $MASTER:$MASTER_IP\
       --privileged \
       --cap-add sys_admin \
       --cap-add sys_ptrace \
        -e "QSERV_MASTER=$MASTER" --name "$i" -h "${i}"  "$WORKER_IMAGE"
    WORKER_IP=$(docker inspect -f '{{ .NetworkSettings.IPAddress }}' $i)
    HOSTFILE="${HOSTFILE}$WORKER_IP    $i
"
done

# Add worker entries to hostfiles, for data-loading
for i in $MASTER $CZARS $WORKERS;
do
    docker exec -u root "$i" sh -c "echo '$HOSTFILE' >> /etc/hosts"
done


# Wait for Qserv services to be up and running
for i in $MASTER $CZARS $WORKERS;
do
    docker exec "$i" /qserv/scripts/wait.sh
done


for i in $(seq 1 "$NB_WORKERS");
do
    CSS_INFO="${CSS_INFO}CREATE NODE worker${i} type=worker port=5012 host=worker${i}.$DNS_DOMAIN;
"
done

CZARARGS=""
for i in $CZARS;
do
    CZARARGS="$CZARARGS -z ${i}"
done

for i in $CZARS;
do
docker exec "$MASTER" bash -c ". /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py && \
    setup -k -r /qserv/qserv_testdata -t qserv-dev && \
    qserv-test-integration.py ${CZARARGS} -q $i"
done

