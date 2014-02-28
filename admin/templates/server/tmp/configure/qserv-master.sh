#!/bin/bash

QSERV_DIR=%(QSERV_DIR)s
MYSQL_DIR=%(MYSQL_DIR)s
MYSQLD_SOCK=%(MYSQLD_SOCK)s
MYSQLD_USER=%(MYSQLD_USER)s
MYSQLD_PASS=%(MYSQLD_PASS)s

SQL_DIR=${QSERV_DIR}/tmp/configure/sql

MYSQL_CMD="${MYSQL_DIR}/bin/mysql -vvv --user=${MYSQLD_USER} --password=${MYSQLD_PASS} --sock=${MYSQLD_SOCK}"

DEST="${QSERV_DIR}/lib/python/lsst/qserv/master/"
if [ ! -f ${DEST}/geometry.py ]
then
    echo "Downloading geometry.py"
    wget -P ${DEST} http://dev.lsstcorp.org/cgit/LSST/DMS/geom.git/plain/python/lsst/geom/geometry.py
fi

echo 
echo "-- Initializing Qserv master database "
${QSERV_DIR}/etc/init.d/mysqld start &&
echo "-- Inserting data"
${MYSQL_CMD} < ${SQL_DIR}/qserv-master.sql && 
${MYSQL_CMD} < ${SQL_DIR}/qservw_workerid.sql && 
${QSERV_DIR}/etc/init.d/mysqld stop ||
exit 1
