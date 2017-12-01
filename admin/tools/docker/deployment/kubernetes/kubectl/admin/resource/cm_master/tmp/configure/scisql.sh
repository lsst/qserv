#!/bin/sh

set -e

QSERV_RUN_DIR='/qserv/run'
MYSQL_DIR='/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/mariadb/10.1.21.lsst2'
MYSQLD_SOCK='/qserv/run/var/lib/mysql/mysql.sock'
MYSQLD_PASSWORD_ROOT='CHANGEME'

${QSERV_RUN_DIR}/etc/init.d/mysqld start
echo "-- Deploying sciSQL plugin in MySQL database"
echo "${MYSQLD_PASSWORD_ROOT}" | scisql-deploy.py --mysql-dir="$MYSQL_DIR" \
				 --mysql-socket="${MYSQLD_SOCK}" \
				 --mysql-user="root"
"${QSERV_RUN_DIR}/etc/init.d/mysqld" stop ||
exit 1

echo "INFO: sciSQL installation and configuration SUCCESSFUL"
