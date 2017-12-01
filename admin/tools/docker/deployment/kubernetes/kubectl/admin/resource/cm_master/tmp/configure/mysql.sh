#!/usr/bin/env bash

set -e

MYSQLD_DATA_DIR=/qserv/data/mysql
MYSQLD_HOST=127.0.0.1
MYSQLD_PORT=13306
MYSQLD_SOCK=/qserv/run/var/lib/mysql/mysql.sock
MYSQL_DIR=/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/mariadb/10.1.21.lsst2
PATH=/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/qserv/travis-gde9786aa4e/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/xrootd/master-gb6706eb477/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/scisql/0.3.8.lsst1+2/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/protobuf/2.6.1.lsst4-1-gdbc86f2+1/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/partition/12.1-1-g721d0e5+35/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/mysqlproxy/0.8.5-1-g072f2a0+2/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/mariadb/10.1.21.lsst2/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/lua/5.1.4.lsst1-2-gda519a9/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/mariadbclient/10.1.21.lsst2/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/python_psutil/4.1.0+4/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/doxygen/1.8.13.lsst1/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/pep8_naming/0.4.1+1/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/pyflakes/1.6.0/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/flake8/3.5.0/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/pytest/3.2.0+1/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/python_future/0.16.0+1/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/scons/3.0.0.lsst1+1/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/sconsUtils/14.0-12-g20aad02/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/apr_util/1.5.4/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/apr/1.5.2/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/boost/1.60.lsst1+2/bin:/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/antlr/2.7.7.lsst1/bin:/qserv/stack/eups/2.1.4/bin:/qserv/stack/python/miniconda3-4.3.21/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
QSERV_RUN_DIR=/qserv/run
QSERV_UNIX_USER=qserv
SQL_DIR=${QSERV_RUN_DIR}/tmp/configure/sql

is_socket_available() {
    # run command in subshell so that we can redirect errors (which are expected)
    (timeout 1 bash -c "cat < /dev/null > /dev/tcp/${MYSQLD_HOST}/${MYSQLD_PORT}") 2>/dev/null
    # status == 1 means that the socket is available
    local status=$?
    local retcode=0
    if [[ ${status} == 0 ]]; then
        echo "WARN: A service is already running on MySQL socket : ${MYSQLD_HOST}:${MYSQLD_PORT}"
        echo "WARN: Please stop it and relaunch configuration procedure"
        retcode=1
    elif [[ ${status} == 124 ]]; then
        echo "WARN: A time-out occured while testing MySQL socket state : ${MYSQLD_HOST}:${MYSQLD_PORT}"
        echo "WARN: Please check that a service doesn't already use this socket, possibly with an other user account"
        retcode=124
    elif [[ ${status} != 1 ]]; then
        echo "WARN: Unable to test MySQL socket state : ${MYSQLD_HOST}:${MYSQLD_PORT}"
        echo "WARN: Please check that a service doesn't already use this socket"
        retcode=2
    fi
    return ${retcode}
}

is_socket_available || exit 1

echo "-- Removing previous data."
rm -rf ${MYSQLD_DATA_DIR}/*
echo "-- ."
echo "-- Installing mysql database files."
"${MYSQL_DIR}/scripts/mysql_install_db" --basedir="${MYSQL_DIR}" --defaults-file="${QSERV_RUN_DIR}/etc/my.cnf" --user=${QSERV_UNIX_USER} >/dev/null ||
{
    echo "ERROR : mysql_install_db failed, exiting"
    exit 1
}
echo "-- Starting mariadb server."
${QSERV_RUN_DIR}/etc/init.d/mysqld start
sleep 5
echo "-- Changing mariadb root password."
mysql --no-defaults -S ${MYSQLD_SOCK} -u root < ${SQL_DIR}/mysql-password.sql ||
{
    echo -n "ERROR : Failed to set mariadb root user password."
    echo "Please set the MariaDB root user password with : "
    echo "mysqladmin -S ${QSERV_RUN_DIR}/var/lib/mysql/mysql.sock -u root password <password>"
    echo "mysqladmin -u root -h ${MYSQLD_HOST} -PXXXX password <password>"
    exit 1
}
rm ${SQL_DIR}/mysql-password.sql
echo "-- Shutting down mariadb server."
${QSERV_RUN_DIR}/etc/init.d/mysqld stop

echo "INFO: MariaDB initialization SUCCESSFUL"
