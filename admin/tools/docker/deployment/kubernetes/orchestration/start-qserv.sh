#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env-infrastructure.sh"
. "${DIR}/env-docker.sh"

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -h          this message

  Launch Qserv service and pods on Kubernetes 

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

if [ -n "$HOST_LOG_DIR" ]; then
    LOG_VOLUME_OPT="--mount type=bind,src=$HOST_LOG_DIR,dst=/qserv/run/var/log"
fi
if [ -n "$HOST_DATA_DIR" ]; then
    DATA_VOLUME_OPT="--mount type=bind,src=$HOST_DATA_DIR,dst=/qserv/data"
fi

MASTER_OPT="-e QSERV_MASTER=master"

kubectl create -f "${DIR}/qserv-service.yaml" -f "${DIR}/master.yaml" 

kubectl create -f "${DIR}/worker.yaml"

j=1
for i in $WORKERS;
do
    j=$((j+1));

done
