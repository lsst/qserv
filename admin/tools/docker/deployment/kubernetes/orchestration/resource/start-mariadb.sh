# Start mariadb inside pod
# and do not exit

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

# Install scisql plugin
#
# TODO: see DM-11126
MARIADB_BIN_DIR=$(dirname $(which mysql))
cp "$QSERV_RUN_DIR"/libscisql-scisql_*.so "$MARIADB_BIN_DIR/../lib/plugin/"

# Start mariadb
#
mysqld_safe --defaults-file=${QSERV_RUN_DIR}/etc/my.cnf

