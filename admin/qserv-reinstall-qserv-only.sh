#!/bin/bash

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

rm -r ${QSERV_BASE}/qserv/master/dist/

export QSERV_ONLY=1
export LOG_FILE_PREFIX="CLEAN"
${QSERV_SRC}/admin/qserv-build-install-cmd-with-opts.sh
unset QSERV_ONLY

