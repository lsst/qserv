#!/usr/bin/env sh

PATH=%(PATH)s
QSERV_DIR=%(QSERV_DIR)s
HOME=%(HOME)s
MYSQLD_SOCK=%(MYSQLD_SOCK)s
MYSQLD_USER=%(MYSQLD_USER)s
MYSQLD_PASS=%(MYSQLD_PASS)s
SQL_DIR=${QSERV_DIR}/tmp/configure/sql

${QSERV_DIR}/etc/init.d/mysqld start && 

mysql -vvv --socket=${MYSQLD_SOCK} --user=${MYSQLD_USER} --pass=${MYSQLD_PASS} < ${SQL_DIR}/qms_qmsdb.sql &&
${QSERV_DIR}/etc/init.d/mysqld stop 
