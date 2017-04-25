#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

CFG_DIR="${DIR}/yaml"

TMP_DIR=$(mktemp -d --suffix=-kube-$USER)
SCHEMA_CACHE_OPT="--schema-cache-dir=$TMP_DIR/schema"

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

YAML_TPL="${CFG_DIR}/pod.yaml.tpl"
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

"$DIR"/templater.py -i "$INI_FILE" -t "$YAML_TPL" -o "$YAML_FILE"

echo "Create kubernetes pod for Qserv master"
kubectl create $SCHEMA_CACHE_OPT -f "$YAML_FILE"

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
image: $WORKER_IMAGE
master_hostname: $MASTER
pod_name: worker-$j
EOF
    "$DIR"/templater.py -i "$INI_FILE" -t "$YAML_TPL" -o "$YAML_FILE"
    echo "Create kubernetes pod for Qserv worker-${j}"
    kubectl create $SCHEMA_CACHE_OPT -f "$YAML_FILE"
    j=$((j+1));
done
