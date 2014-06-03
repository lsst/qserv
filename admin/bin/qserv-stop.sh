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

services_rev=`echo ${SERVICES} | tr ' ' '\n' | tac`
for service in $services_rev; do
    ${QSERV_RUN_DIR}/etc/init.d/$service stop
done

# still usefull ?
echo "Cleaning  ${QSERV_RUN_DIR}/xrootd-run/result/"
rm -f ${QSERV_RUN_DIR}/xrootd-run/result/*

