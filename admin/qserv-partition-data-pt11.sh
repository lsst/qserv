#!/bin/bash

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

# in order to load numpy
export PYTHONPATH=/usr/lib64/python2.6/site-packages/

mkdir -p ${QSERV_DATA}/pt11_partition/
${QSERV_BASE}/bin/qserv-admin --partition --source ${QSERV_DATA}/pt11/ --table Object --output ${QSERV_DATA}/pt11_partition/
