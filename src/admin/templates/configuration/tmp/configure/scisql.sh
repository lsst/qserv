#!/bin/sh

set -e

QSERV_RUN_DIR='{{QSERV_RUN_DIR}}'
MYSQL_DIR='{{MYSQL_DIR}}'
MYSQLD_SOCK='{{MYSQLD_SOCK}}'
MYSQLD_PASSWORD_ROOT='{{MYSQLD_PASSWORD_ROOT}}'

${QSERV_RUN_DIR}/etc/init.d/mysqld start
echo "-- Deploying sciSQL plugin in MySQL database"
echo "${MYSQLD_PASSWORD_ROOT}" | scisql-deploy.py --mysql-dir="$MYSQL_DIR" \
				 --mysql-socket="${MYSQLD_SOCK}" \
				 --mysql-user="root"
"${QSERV_RUN_DIR}/etc/init.d/mysqld" stop ||
exit 1

echo "INFO: sciSQL installation and configuration SUCCESSFUL"
