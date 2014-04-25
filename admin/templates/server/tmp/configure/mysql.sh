#!/usr/bin/env bash

QSERV_DIR=%(QSERV_DIR)s
PATH=%(PATH)s
MYSQLD_SOCK=%(MYSQLD_SOCK)s
MYSQLD_DATA_DIR=%(MYSQLD_DATA_DIR)s
MYSQLD_HOST=%(MYSQLD_HOST)s
MYSQLD_PORT=%(MYSQLD_PORT)s

SQL_DIR=${QSERV_DIR}/tmp/configure/sql

function is_up {
    timeout 1 cat < /dev/null > /dev/tcp/${MYSQLD_HOST}/${MYSQLD_PORT}
    return ${status}
}

is_up &&
{
    echo "-- Service already running on ${MYSQLD_HOST}:${MYSQLD_PORT}"
    echo "-- Please stop it and relaunch configuration procedure}"
    exit 1
}

echo "-- Removing previous data." &&
rm -rf ${MYSQLD_DATA_DIR}/* &&
echo "-- ." &&
mysql_install_db --defaults-file=${QSERV_DIR}/etc/my.cnf --user=${USER} &&
echo "-- Starting mysql server." &&
${QSERV_DIR}/etc/init.d/mysqld start &&
sleep 5 &&
echo "-- Changing mysql root password." &&
mysql -S ${MYSQLD_SOCK} -u root < ${SQL_DIR}/mysql-password.sql &&
rm ${SQL_DIR}/mysql-password.sql &&
echo "-- Shutting down mysql server." &&
${QSERV_DIR}/etc/init.d/mysqld stop || 
{
    echo -n "Failed to set mysql root user password."
    echo "Please set the mysql root user password with : "
    echo "mysqladmin -S ${QSERV_DIR}/var/lib/mysql/mysql.sock -u root password <password>"
    echo "mysqladmin -u root -h ${MYSQLD_HOST} -P${MYSQLD_PASS} password <password>"
    exit 1
}

