#!/bin/sh

# Start mariadb inside pod
# and do not exit

# @author  Fabrice Jammes, IN2P3/SLAC

set -eux

# Source pathes to eups packages
. /qserv/run/etc/sysconfig/qserv

MARIADB_CONF="/config-etc/my.cnf"
if [ -e "$MARIADB_CONF" ]; then
    mkdir -p /etc/mysql
    ln -sf "$MARIADB_CONF" /etc/mysql/my.cnf
fi

echo "-- Start mariadb server."
mysqld
if [ $? -ne 0 ]; then
    >&2 echo "ERROR: failed to start mariadb server"
    exit 1
fi
