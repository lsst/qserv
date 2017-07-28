# Start mariadb inside pod
# and do not exit

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

echo "-- Start mariadb server."
mysqld || echo "ERROR: Fail to start MariaDB"
