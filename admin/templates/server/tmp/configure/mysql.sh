MYSQL_DIR=%(MYSQLD_DIR)s
MYSQLD_DATA_DIR=%(MYSQLD_DATA_DIR)s
MYSQLD_HOST=%(MYSQLD_HOST)s
MYSQLD_PASS=%(MYSQLD_PASS)s

killall mysqld mysqld_safe
rm -rf ${MYSQLD_DATA_DIR}/*
    
${MYSQL_DIR}/bin/mysql_install_db --defaults-file=${MYSQL_DIR}/etc/my.cnf --user=$prod_user
    
echo "Starting mysql server."
${MYSQL_DIR}/bin/mysqld_safe --defaults-file=${MYSQL_DIR}/etc/my.cnf &
sleep 5;

echo "Changing mysql root password."
${MYSQL_DIR}/bin/mysql -S ${MYSQL_DIR}/var/lib/mysql/mysql.sock -u root < ${QSERV_DIR}/tmp/mysql-password.sql &&
rm ${QSERV_DIR}/tmp/mysql-password.sql &&
echo "Shutting down mysql server."
${MYSQL_DIR}/bin/mysqladmin -S ${MYSQL_DIR}/var/lib/mysql/mysql.sock shutdown -u root -p'%(MYSQLD_PASS)s' || 
{
    echo -n "Failed to set mysql root user password."
    echo "Please set the mysql root user password with : "
    echo "mysqladmin -S ${QSERV_DIR}/var/lib/mysql/mysql.sock -u root password <password>"
    echo "mysqladmin -u root -h ${MYSQLD_HOST} -P${MYSQLD_PASS} password <password>"
    exit 1
}

