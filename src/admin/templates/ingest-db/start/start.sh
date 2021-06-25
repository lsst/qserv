#!/bin/bash

# Start mariadb inside pod
# and do not exit

# @author  Fabrice Jammes, IN2P3/SLAC

set -eux

# Require root privileges
##
MARIADB_CONF="/config-etc/my.cnf"
if [ -e "$MARIADB_CONF" ]; then
    mkdir -p /etc/mysql
    ln -sf "$MARIADB_CONF" /etc/mysql/my.cnf
fi

if ! id 1000 > /dev/null 2>&1
then
    useradd qserv --uid 1000 --no-create-home
fi
##

echo "-- Start mariadb server."
mysqld
if [ $? -ne 0 ]; then
    >&2 echo "ERROR: failed to start the replication database"
    exit 1
fi
