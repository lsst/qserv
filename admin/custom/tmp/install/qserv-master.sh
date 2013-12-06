#!/bin/bash

QSERV_BASE_DIR=%(QSERV_BASE_DIR)s
QSERV_SRC_DIR=%(QSERV_SRC_DIR)s
MYSQLD_SOCK=%(MYSQLD_SOCK)s
MYSQLD_USER=%(MYSQLD_USER)s
MYSQLD_PASS=%(MYSQLD_PASS)s

MYSQL_CMD="${QSERV_BASE_DIR}/bin/mysql --user=${MYSQLD_USER} --password=${MYSQLD_PASS} --sock=${MYSQLD_SOCK}"

rm -rf "${QSERV_BASE_DIR}/qserv/master/dist"

# TODO : isolate Qserv binaries from source
# in order to avoid symlink creation
rm -f ${QSERV_BASE_DIR}/qserv
ln -s ${QSERV_SRC_DIR} ${QSERV_BASE_DIR}/qserv

export XRD_DIR="${QSERV_BASE_DIR}/xrootd";
export XRD_PLATFORM="x86_64_linux_26_dbg";
export PROTOC="${QSERV_BASE_DIR}/bin/protoc";
export PROTOC_INC="${QSERV_BASE_DIR}/include";
export PROTOC_LIB="${QSERV_BASE_DIR}/lib";
export MYSQL_ROOT="${QSERV_BASE_DIR}";
export SEARCH_ROOTS="${QSERV_BASE_DIR}";

echo -e "\n"
echo "Building Qserv commons libs"
echo "---------------------------"
cd "${QSERV_BASE_DIR}/qserv/common" &&
scons &&
scons master ||
exit 1

DEST="${QSERV_BASE_DIR}/qserv/master/python/lsst/qserv/master/"
if [ ! -f ${DEST}/geometry.py ]
then
    echo "Downloading geometry.py"
    wget -P ${DEST} http://dev.lsstcorp.org/cgit/LSST/DMS/geom.git/plain/python/lsst/geom/geometry.py
fi

echo -e "\n"
echo "Initializing Qserv master database "
echo "-----------------------------------"
${QSERV_BASE_DIR}/etc/init.d/mysqld start &&
${MYSQL_CMD} < ${QSERV_BASE_DIR}/tmp/install/sql/qserv-master.sql && 
${MYSQL_CMD} < ${QSERV_BASE_DIR}/tmp/install/sql/qservw_workerid.sql || 
exit 1

echo -e "\n"
echo "Building Qserv master (rpc server)"
echo "----------------------------------"
cd "${QSERV_BASE_DIR}/qserv/master/" &&
scons &&
scons install ||
exit 1

echo -e "\n"
echo "Building Qserv worker (xrootd plugin)"
echo "-------------------------------------"
rm -rf "${QSERV_BASE_DIR}/qserv/worker/tests/.tests" &&
# worker use next env variable to access mysql DB, and use hard-coded
# login/password :
# a configuration file containing mysql credentials would be welcome
export QSW_DBSOCK=${MYSQLD_SOCK} &&
export QSW_MYSQLDUMP=${QSERV_BASE_DIR}/bin/mysqldump &&
cd "${QSERV_BASE_DIR}/qserv/worker/" &&
scons ||
exit 1
${QSERV_BASE_DIR}/etc/init.d/mysqld stop
