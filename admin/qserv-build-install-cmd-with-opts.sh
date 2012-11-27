#!/bin/bash

source ${QSERV_SRC}/qserv-env.sh
source ${QSERV_SRC}/qserv-install-params.sh

cd ${QSERV_BASE}

INSTALL_OPTS="--install-dir=\"${QSERV_BASE}\""

if [ -n "${QSERV_LOG}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --log-dir=\"${QSERV_LOG}\""
fi
if [ -n "${QSERV_MYSQL_DATA}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --mysql-data-dir=\"${QSERV_MYSQL_DATA}\""
fi
if [ -n "${QSERV_MYSQL_PORT}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --mysql-port=${QSERV_MYSQL_PORT}"
fi
if [ -n "${QSERV_MYSQL_PROXY_PORT}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --mysql-proxy-port=${QSERV_MYSQL_PROXY_PORT}"
fi
if [ -n "${QSERV_MYSQL_PASS}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --db-pass=\"${QSERV_MYSQL_PASS}\""
fi
if [ -n "${CMSD_MANAGER_PORT}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --cmsd-manager-port=${CMSD_MANAGER_PORT}"
fi
if [ -n "${XROOTD_PORT}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --xrootd-port=${XROOTD_PORT}"
fi
if [ -n "${GEOMETRY_DIR}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --geometry-dir=\"${GEOMETRY_DIR}\""
fi
if [ -n "${MONO_NODE}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --mono-node"
fi

if [ -n "${QSERV_ONLY}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --qserv"
elif [ -n "${QSERV_CLEAN_ALL}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --clean-all"
elif [ -n "${INIT_MYSQL_DB}" ]; then
	INSTALL_OPTS="${INSTALL_OPTS} --init-mysql-db"
fi

if [ -e "${LOG_FILE_PREFIX}" ]; then
        LOG_FILE_PREFIX="UNDEFINED"
fi

CMD="${QSERV_SRC}/admin/qserv-install ${INSTALL_OPTS} &> \"${QSERV_LOG}/${LOG_FILE_PREFIX}-$DATE.log\""
echo "Running  : ${CMD}"
eval ${CMD}
# ${QSERV_SRC}/admin/qserv-install ${INSTALL_OPTS} 2&> ${QSERV_BASE}/INSTALL-$DATE.log

# patching install script
## perl -i.bak -pe 's/`(.*)`/run_command("$1")/g' ~/src/qserv-0.3.0rc3/admin/qserv-install
## perl replace.pl < ~/src/qserv-0.3.0rc3/admin/qserv-install.0 > ~/src/qserv-0.3.0rc3/admin/qserv-install
