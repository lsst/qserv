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

kubectl create -f "${DIR}/qserv-service.yaml"

YAML_TPL="${DIR}/pod.yaml.tpl"
YAML_FILE="${DIR}/master.yaml"
INI_FILE="$DIR/pod.ini"

cat << EOF > "$INI_FILE"
[spec]
host_custom_dir: $HOST_CUSTOM_DIR
host_data_dir: $HOST_DATA_DIR
host_log_dir: $HOST_LOG_DIR
host_tmp_dir: $HOST_TMP_DIR
host: $MASTER
image: $MASTER_IMAGE
pod_name: master
EOF

"$DIR"/templater.py -i "$INI_FILE" -t "$YAML_TPL" -o "$YAML_FILE"

kubectl create -f "$YAML_FILE"

j=1
for host in $WORKERS;
do
    YAML_FILE="${DIR}/worker-${j}.yaml"
    cat << EOF > "$DIR"/pod.ini
[spec]
host_custom_dir: $HOST_CUSTOM_DIR
host_data_dir: $HOST_DATA_DIR
host_log_dir: $HOST_LOG_DIR
host_tmp_dir: $HOST_TMP_DIR
host: $host
image: $WORKER_IMAGE
pod_name: worker-$j
EOF
    "$DIR"/templater.py -i "$INI_FILE" -t "$YAML_TPL" -o "$YAML_FILE"
    kubectl create -f "$YAML_FILE"
    j=$((j+1));
done
