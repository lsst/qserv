#!/bin/sh

TMP_PATH=$(which qserv-configure.py)
QSERV_BIN_DIR=$(dirname $TMP_PATH)
source ${QSERV_BIN_DIR}/env.sh

while getopts "hr:" option
do
    case $option in
    h)
        usage
        ;;
    r)
        QSERV_RUN_DIR=${OPTARG}
        ;;
    esac
done

check_qserv_run_dir

for service in ${SERVICES}; do
    ${QSERV_RUN_DIR}/etc/init.d/$service start
done

