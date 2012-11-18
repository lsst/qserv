#!/bin/bash

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

qserv-admin --delete-data --dbpass ${QSERV_MYSQL_PASS} 
qserv-admin --load --dbpass ${QSERV_MYSQL_PASS} --source ${QSERV_DATA}/pt11/ --table Object --output ${QSERV_DATA}/pt11_partition/
# if it fails : launch next commands to reset to initial state.
# mysql> drop database LSST;
# mysql> drop database qserv_worker_meta_1
# bash> rm -rf /opt/qserv/xrootd-run//q/LSST/
