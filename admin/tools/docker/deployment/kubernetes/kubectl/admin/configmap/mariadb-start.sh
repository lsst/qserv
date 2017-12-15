# Start mariadb inside pod
# and do not exit

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

MARIADB_CONF="/config-mariadb/my.cnf"
if [ -e "$MARIADB_CONF" ]; then
    cp /etc/mysql/my.cnf /etc/mysql/my.cnf.0
    ln -sf "$MARIADB_CONF" /etc/mysql/my.cnf
fi

echo "-- Start mariadb server."
mysqld || echo "ERROR: Fail to start MariaDB"
