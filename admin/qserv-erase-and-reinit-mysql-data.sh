#!/bin/bash

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

export INIT_MYSQL_DB=1
export LOG_FILE_PREFIX="INIT-MYSQL-DB"
${QSERV_SRC}/admin/qserv-build-install-cmd-with-opts.sh
unset INIT_MYSQL_DB

