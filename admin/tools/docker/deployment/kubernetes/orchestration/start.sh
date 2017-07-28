#!/bin/sh

# Launch Qserv pods on Kubernetes cluster

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

CFG_DIR="${DIR}/yaml"
RESOURCE_DIR="${DIR}/resource"

TMP_DIR=$(mktemp -d --suffix=-kube-$USER)

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

YAML_MASTER_TPL="${CFG_DIR}/pod.master.yaml.tpl"
YAML_FILE="${CFG_DIR}/master.yaml"
INI_FILE="${CFG_DIR}/pod.master.ini"

cat << EOF > "$INI_FILE"
[spec]
host_custom_dir: $HOST_CUSTOM_DIR
host_data_dir: $HOST_DATA_DIR
host_log_dir: $HOST_LOG_DIR
host_tmp_dir: $HOST_TMP_DIR
host: $MASTER
image: $MASTER_IMAGE
master_hostname: $MASTER
pod_name: master
EOF

"$DIR"/yaml-builder.py -i "$INI_FILE" -r "$RESOURCE_DIR" -t "$YAML_MASTER_TPL" -o "$YAML_FILE"

echo "Create kubernetes pod for Qserv master"
kubectl create -f "$YAML_FILE"

YAML_WORKER_TPL="${CFG_DIR}/pod.worker.yaml.tpl"
j=1
for host in $WORKERS;
do
    YAML_FILE="${CFG_DIR}/worker-${j}.yaml"
    INI_FILE="${CFG_DIR}/pod.worker-${j}.ini"
    cat << EOF > "$INI_FILE"
[spec]
host_custom_dir: $HOST_CUSTOM_DIR
host_data_dir: $HOST_DATA_DIR
host_log_dir: $HOST_LOG_DIR
host_tmp_dir: $HOST_TMP_DIR
host: $host
image: $CONTAINER_IMAGE
image_mariadb: qserv/mariadb_scisql:10.1.25
master_hostname: $MASTER
mysql_root_password: CHANGEME
pod_name: worker-$j
EOF
    "$DIR"/yaml-builder.py -i "$INI_FILE" -r "$RESOURCE_DIR" -t "$YAML_WORKER_TPL" -o "$YAML_FILE"
    echo "Create kubernetes pod for Qserv worker-${j}"
    kubectl create -f "$YAML_FILE"
    j=$((j+1));
done
