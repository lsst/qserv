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

# TODO: move to k8s
if [ -n "$HOST_LOG_DIR" ]; then
    LOG_VOLUME_OPT="--mount type=bind,src=$HOST_LOG_DIR,dst=/qserv/run/var/log"
fi
if [ -n "$HOST_DATA_DIR" ]; then
    DATA_VOLUME_OPT="--mount type=bind,src=$HOST_DATA_DIR,dst=/qserv/data"
fi

kubectl create -f "${DIR}/qserv-service.yaml"

YAML_FILE="${DIR}/master.yaml"
awk \
-v HOST="$MASTER" \
-v MASTER_IMAGE="$MASTER_IMAGE" \
'{gsub(/<HOST>/, HOST);
 gsub(/<MASTER_IMAGE>/, MASTER_IMAGE);
 print}' "$DIR/master.yaml.tpl" > "$YAML_FILE"
kubectl create -f "$YAML_FILE"

j=1
for host in $WORKERS;
do
    YAML_FILE="${DIR}/worker-${j}.yaml"
    awk \
    -v HOST="$host" \
    -v NODE_ID="$j" \
    -v WORKER_IMAGE="$WORKER_IMAGE" \
    '{gsub(/<NODE_ID>/, NODE_ID);
     gsub(/<HOST>/, HOST);
     gsub(/<WORKER_IMAGE>/, WORKER_IMAGE);
     print}' "$DIR/worker.yaml.tpl" > "$YAML_FILE"
    kubectl create -f "$YAML_FILE"
    j=$((j+1));
done
