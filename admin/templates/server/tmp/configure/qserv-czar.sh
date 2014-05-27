#!/bin/bash

QSERV_DIR=%(QSERV_DIR)s
MYSQL_DIR=%(MYSQL_DIR)s
MYSQLD_SOCK=%(MYSQLD_SOCK)s
MYSQLD_USER=%(MYSQLD_USER)s
MYSQLD_PASS=%(MYSQLD_PASS)s

SQL_DIR=${QSERV_DIR}/tmp/configure/sql

MYSQL_CMD="${MYSQL_DIR}/bin/mysql -vvv --user=${MYSQLD_USER} --password=${MYSQLD_PASS} --sock=${MYSQLD_SOCK}"

echo 
echo "-- Initializing Qserv czar database "
${QSERV_DIR}/etc/init.d/mysqld start &&
echo "-- Inserting data"
${MYSQL_CMD} < ${SQL_DIR}/qserv-czar.sql && 
${MYSQL_CMD} < ${SQL_DIR}/qservw_workerid.sql && 
${QSERV_DIR}/etc/init.d/mysqld stop ||
exit 1
