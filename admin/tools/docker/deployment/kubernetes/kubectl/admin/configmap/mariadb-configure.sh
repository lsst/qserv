#!/bin/sh

# Configure mariadb:
# - create data directory
# - create root password
# - create qserv databases and user
# - deploy scisql plugin

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DATA_DIR="/qserv/data"
MARIADB_LOCK="$DATA_DIR/mariadb-cfg.lock"
MYSQLD_DATA_DIR="$DATA_DIR/mysql"
MYSQLD_SOCKET="$MYSQLD_DATA_DIR/mysql.sock"
# TODO: Set password using k8s
MYSQLD_PASSWORD_ROOT="CHANGEME"
USER="mysql"
# TODO manage via configmap
SQL_DIR="/config-sql"

MARIADB_CONF="/config-mariadb/my.cnf"
if [ -e "$MARIADB_CONF" ]; then
    cp /etc/mysql/my.cnf /etc/mysql/my.cnf.0
    ln -sf "$MARIADB_CONF" /etc/mysql/my.cnf
fi

# Make timezone adjustments (if requested)
# TODO: set tz using k8s: http://cloudgeekz.com/1170/howto-timezone-for-kubernetes-pods.html
if [ "$SET_CONTAINER_TIMEZONE" = "true" ]; then

    # These files have to be write-enabled for the current user ('qserv')

    echo ${CONTAINER_TIMEZONE} >/etc/timezone && \
    cp /usr/share/zoneinfo/${CONTAINER_TIMEZONE} /etc/localtime
    dpkg-reconfigure -f noninteractive tzdata

    echo "Container timezone set to: $CONTAINER_TIMEZONE"
else
    echo "Container timezone not modified"
fi

# Run configuration step iff Qserv data directory is empty
EMPTY_DATA_DIR="$(find "$DATA_DIR" -prune -empty -type d)"

if [ -n "$EMPTY_DATA_DIR" ]
then
    touch "$MARIADB_LOCK"
    echo "-- "
    echo "-- Installing mysql database files."
    mysql_install_db --basedir="${MYSQL_DIR}" --user=${USER} >/dev/null ||
        {
            echo "ERROR : mysql_install_db failed, exiting"
            exit 1
        }

    echo "-- "
    echo "-- Start mariadb server."
    mysqld &
    sleep 5

    echo "-- "
    echo "-- Change mariadb root password."
    mysqladmin -u root password "$MYSQLD_PASSWORD_ROOT"

    echo "-- "
    echo "-- Initializing Qserv database"
    for file_name in "${SQL_DIR}"/*; do
        echo "-- Loading ${file_name} in MySQL"
        if mysql -vvv --user="root" --password="${MYSQLD_PASSWORD_ROOT}" \
            < "${file_name}"
        then
            echo "-- -> success"
        else
            echo "-- -> error"
            exit 1
        fi
    done

    echo "-- "
    echo "-- Deploy scisql plugin"
	# WARN: SciSQL shared library (libcisql*.so) deployed by command
	# below will be removed at each container startup.
    # That's why this shared library is currently 
	# installed in mysql plugin directory at image creation.
    echo "$MYSQLD_PASSWORD_ROOT" | scisql-deploy.py --mysql-plugin-dir=/usr/lib/mysql/plugin/ \
        --mysql-bin=/usr/bin/mysql --mysql-socket="$MYSQLD_SOCKET"

    echo "-- Stop mariadb server."
    mysqladmin -u root --password="$MYSQLD_PASSWORD_ROOT" shutdown
    rm "$MARIADB_LOCK"
fi
