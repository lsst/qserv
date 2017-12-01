
QSERV_RUN_DIR="/qserv/run"
MYSQL_DIR="/qserv/stack/stack/miniconda3-4.3.21-10a4fa6/Linux64/mariadb/10.1.21.lsst2"
MYSQLD_SOCK="/qserv/run/var/lib/mysql/mysql.sock"
MYSQLD_PASSWORD_ROOT="CHANGEME"

SQL_DIR="${QSERV_RUN_DIR}/tmp/configure/sql"

"${QSERV_RUN_DIR}"/etc/init.d/mysqld start &&
for file_name in ${SQL_FILE}; do
    echo "-- Loading ${file_name} in MySQL"
    "${MYSQL_DIR}"/bin/mysql --no-defaults -vvv --user="root" \
    --password="${MYSQLD_PASSWORD_ROOT}" --socket="${MYSQLD_SOCK}" < "${SQL_DIR}/${file_name}"
done &&
"${QSERV_RUN_DIR}"/etc/init.d/mysqld stop ||
{
    >&2 echo "ERROR: unable to load ${SQL_FILE}"
    exit 1
}
