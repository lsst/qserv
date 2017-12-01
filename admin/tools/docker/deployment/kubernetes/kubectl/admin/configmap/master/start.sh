#!/bin/sh

# Start cmsd xrootd and qserv-wmgr inside pod

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

# Make timezone adjustments (if requested)
if [ "$SET_CONTAINER_TIMEZONE" = "true" ]; then

    # These files have to be write-enabled for the current user ('qserv')

    echo ${CONTAINER_TIMEZONE} >/etc/timezone && \
    cp /usr/share/zoneinfo/${CONTAINER_TIMEZONE} /etc/localtime

    # To make things fully complete we would also need to run
    # this command. Unfortunatelly the security model of the container
    # won't allow that because the current script is being executed
    # under a non-privileged user 'qserv'. Hence disabling this for now.
    #
    # dpkg-reconfigure -f noninteractive tzdata

    echo "Container timezone set to: $CONTAINER_TIMEZONE"
else
    echo "Container timezone not modified"
fi

. /qserv/run/etc/sysconfig/qserv

MARIADB_CFG_LOCK="/qserv/data/mariadb-cfg.lock"

# Wait for mysql to be configured and started
while true; do
    if mysql --socket "$MYSQLD_SOCK" --user="$MYSQLD_USER_QSERV"  --skip-column-names \
        -e "SELECT CONCAT('Mariadb is up: ', version())"
    then
        if [ -f $MARIADB_CFG_LOCK ]
        then
            echo "Wait for MySQL to be configured"
        else
            break
        fi
    else
        echo "Wait for MySQL startup"
    fi
    sleep 2
done

# Create client configuration
mkdir -p "$HOME/.lsst"
ln -sf /qserv/run/etc/qserv-client.conf "$HOME/.lsst/qserv.conf"

# Create directory for empty chunk files
mkdir -p /qserv/data/qserv

# Create symlink for data
# FIXME: remove it by changing emptychunk file path
# from /qserv/run/var/lib to /qserv/data/
ln -sf /qserv/data /qserv/run/var/lib

# Start qserv-wmgr
#

# Generate wmgr password
echo "USER:CHANGEME" > $QSERV_RUN_DIR/etc/wmgr.secret

$QSERV_RUN_DIR/etc/init.d/qserv-wmgr start || echo "ERROR: fail to start qserv-wmgr"

# Start mysql-proxy 
#
$QSERV_RUN_DIR/etc/init.d/mysql-proxy start || echo "ERROR: fail to start mysql-proxy"

# Start xrootd
#
$QSERV_RUN_DIR/etc/init.d/xrootd start || echo "ERROR: fail to start xrootd"

# TODO: Implement container restart on xrootd/cmsd process crash (see DM-11128)
while /bin/true; do
    if ! "$QSERV_RUN_DIR"/etc/init.d/qserv-wmgr status > /dev/null
    then
        echo "ERROR: qserv-wmgr is not running, exiting"
        exit -1
    fi
    if ! "$QSERV_RUN_DIR"/etc/init.d/mysql-proxy status > /dev/null
    then
        echo "ERROR: mysql-proxy is not running, exiting"
        exit -1
    fi
    if ! "$QSERV_RUN_DIR"/etc/init.d/xrootd status > /dev/null
    then
        echo "ERROR: xrootd is not running"
    fi
    sleep 60
done
