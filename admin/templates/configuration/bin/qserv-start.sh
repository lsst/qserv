#!/bin/sh

QSERV_RUN_DIR={{QSERV_RUN_DIR}}
. ${QSERV_RUN_DIR}/bin/env.sh

check_qserv_run_dir

for service in ${SERVICES}; do
    ${QSERV_RUN_DIR}/etc/init.d/$service start
done

