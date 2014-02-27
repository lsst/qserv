#!/bin/bash

QSERV_BASE_DIR=%(QSERV_BASE_DIR)s
QSERV_SRC_DIR=%(QSERV_SRC_DIR)s
MYSQLD_SOCK=%(MYSQLD_SOCK)s
MYSQLD_USER=%(MYSQLD_USER)s
MYSQLD_PASS=%(MYSQLD_PASS)s

MYSQL_CMD="${QSERV_BASE_DIR}/bin/mysql --user=${MYSQLD_USER} --password=${MYSQLD_PASS} --sock=${MYSQLD_SOCK}"

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
${QSERV_BASE_DIR}/etc/init.d/mysqld stop
