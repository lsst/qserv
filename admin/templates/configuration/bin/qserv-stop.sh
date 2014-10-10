#!/bin/sh

QSERV_RUN_DIR={{QSERV_RUN_DIR}}
. ${QSERV_RUN_DIR}/bin/env.sh

check_qserv_run_dir

services_rev=`echo ${SERVICES} | tr ' ' '\n' | tac`
for service in $services_rev; do
    ${QSERV_RUN_DIR}/etc/init.d/$service stop
done

# still usefull ?
echo "Cleaning  ${QSERV_RUN_DIR}/xrootd-run/result/"
rm -f ${QSERV_RUN_DIR}/xrootd-run/result/*

