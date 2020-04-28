#!/bin/sh

set -e
set -x

# Customize parameter below
INSTALL_DIR=/root/directory/where/qserv/stack/will/be/installed

QSERV_INSTALL_SCRIPT=${QSERV_SRC_DIR}/admin/tools/qserv-install.sh
bash ${QSERV_INSTALL_SCRIPT} -i ${INSTALL_DIR}
