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

# Wait for mysql to start
while true; do
    if mysql --socket "$MYSQLD_SOCK" --user=qserv  --skip-column-names \
        -e "SELECT CONCAT('Mariadb is up: ', version())"
    then
        break
    else
        echo "Wait for MySQL startup"
    fi
    sleep 2
done

# Start qserv-wmgr
#

# Generate wmgr password
echo "USER:CHANGEME" > $QSERV_RUN_DIR/etc/wmgr.secret

$QSERV_RUN_DIR/etc/init.d/qserv-wmgr start

# Start xrootd
#
$QSERV_RUN_DIR/etc/init.d/xrootd start

# TODO: Implement container restart on xrootd/cmsd process crash (see DM-11128)
while /bin/true; do
    $QSERV_RUN_DIR/etc/init.d/qserv-wmgr status
    PROCESS_1_STATUS=$?
    $QSERV_RUN_DIR/etc/init.d/xrootd status
    PROCESS_2_STATUS=$?
    if [ $PROCESS_1_STATUS -ne 0 ]
    then
        echo "ERROR: qserv-wmgr has exited"
        exit -1
    elif [ $PROCESS_2_STATUS -ne 0 ]
    then
        echo "ERROR: xrootd has exited"
    fi
    sleep 60
done
