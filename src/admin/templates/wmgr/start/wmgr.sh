#!/bin/sh
#
# qserv-wmgr   This script starts Qserv worker management service.
#
# description: start and stop qserv worker management service

# Description: qserv-wmgr is the Qserv worker management service \
#              It provides RESTful HTTP interface which is a single \
#              end-point for all worker communication and control.

set -e
set -x

# Load mariadb secrets
. /secret-mariadb/mariadb.secret.sh
if [ -z "$MYSQL_ROOT_PASSWORD" ]; then
    echo "ERROR : mariadb root password is missing, exiting"
    exit 2
fi

# Create configuration files from templates
mkdir /qserv/etc
WMGR_CONFIG="/qserv/etc/wmgr.cnf"
cp "/config-etc/wmgr.cnf" "$WMGR_CONFIG"
sed -i "s/<ENV_MYSQL_ROOT_PASSWORD>/${MYSQL_ROOT_PASSWORD}/" "$WMGR_CONFIG"

# Czar only, Useful for multinode integration test
DOT_CONFIG_TPL="/config-dot-qserv/qserv.conf"
if [ -f "$DOT_CONFIG_TPL" ]; then
    mkdir -p /home/qserv/.lsst
    DOT_CONFIG="/home/qserv/.lsst/qserv.conf"
    cp "$DOT_CONFIG_TPL" "$DOT_CONFIG"
    sed -i "s/<ENV_MYSQL_ROOT_PASSWORD>/${MYSQL_ROOT_PASSWORD}/" "$DOT_CONFIG"
fi

# Source pathes to eups packages
. /qserv/run/etc/sysconfig/qserv

# Disabling buffering in python in order to enable "real-time" logging.
export PYTHONUNBUFFERED=1

qservWmgr.py -c "$WMGR_CONFIG" -v
