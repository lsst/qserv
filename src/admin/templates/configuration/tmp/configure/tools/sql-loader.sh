
QSERV_RUN_DIR="{{QSERV_RUN_DIR}}"
MYSQL_DIR="{{MYSQL_DIR}}"
MYSQLD_SOCK="{{MYSQLD_SOCK}}"
MYSQLD_PASSWORD_ROOT="{{MYSQLD_PASSWORD_ROOT}}"

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
