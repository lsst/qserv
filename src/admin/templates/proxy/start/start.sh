#!/bin/sh
#
# mysql-proxy This script starts mysql-proxy
#
# description: mysql-proxy is a proxy daemon for mysql

# Description: mysql-proxy is the user (i.e. mysql-client) frontend for \
#              Qserv czar service. \
#              It receive SQL queries, process it using lua plugin, \
#              and send it to Qserv czar. \
#              Once Qserv czar have returned the results, mysql-proxy \
#              sends it to mysql-client. \

set -e

# Source pathes to eups packages
. /qserv/run/etc/sysconfig/qserv

# Run proxy using unix account below
PROXY_USER=qserv

# Create directory for empty chunk files
# TODO: this should be handled by czar in order to have
# EMPTYCHUNK_PATH parameter defined in one unique location
EMPTYCHUNK_PATH="/qserv/data/qserv"
if [ ! -d "$EMPTYCHUNK_PATH" ]; then
    mkdir "$EMPTYCHUNK_PATH"
    chown -R "$PROXY_USER":"$PROXY_USER" "$EMPTYCHUNK_PATH"
fi

# mysql-proxy requires my-proxy.cnf to have
# permissions 660 to start, but configmap file
# is a symlink.
# So it is copied and chmod'ed to work.
MYPROXY_CONF_IN="/config-etc/my-proxy.cnf"
# FIXME: copy to /etc when write access is enabled
MYPROXY_CONF="/tmp/my-proxy.cnf"
if [ -e "$MYPROXY_CONF_IN" ]; then
    cp "$MYPROXY_CONF_IN" "$MYPROXY_CONF"
    chmod 660 "$MYPROXY_CONF"
else
    log_failure_msg "Unable to find mysql-proxy configuration file"
    exit 1
fi

QSERV_CONF="/config-etc/qserv-czar.cnf"

# Set default mysql-proxy configuration.
PROXY_OPTIONS="--proxy-lua-script=${QSERV_DIR}/share/lua/qserv/mysqlProxy.lua \
        --lua-cpath=${QSERV_DIR}/lib/lua/qserv/czarProxy.so"


# jemalloc profilling
JEMU=""
JEMD=""
#JEMU='LD_PRELOAD=${JEMALLOC_DIR}/lib/libjemalloc.so'
#JEMD='MALLOC_CONF=MALLOC_CONF=prof:true,lg_prof_interval:30,lg_prof_sample:17,prof_prefix:/qserv/run/var/log/jeprof'

VALG=""
#VALG="valgrind --leak-check=full"
#VALG="valgrind --leak-check=no"
#VALG="valgrind --tool=callgrind"

# WARNING : can only use the --user switch if running as root
proxy_user_option=""
if [ "$USER" = "root" ]; then
    proxy_user=${PROXY_USER}
    proxy_user_option="--user=$proxy_user"
fi

QSERV_CONFIG=${QSERV_CONF}${JEMU} ${JEMD} ${VALG} mysql-proxy \
    $PROXY_OPTIONS $proxy_user_option \
    --defaults-file=${MYPROXY_CONF}
