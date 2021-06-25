#!/bin/sh

# Start cmsd and xrootd inside pod
# Launch as qserv user

# @author  Fabrice Jammes, IN2P3/SLAC

set -eux
# set -x

usage() {
    cat << EOD

Usage: `basename $0` [options] [cmd]

  Available options:
    -S <service> Service to start, default to xrootd

  Start cmsd or xrootd.
EOD
}

service=xrootd

XROOTD_RDR_DN="{{.XrootdRedirectorDn}}"

# get the options
while getopts S: c ; do
    case $c in
        S) service="$OPTARG" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift $(($OPTIND - 1))

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

# Source pathes to eups packages
. /qserv/run/etc/sysconfig/qserv

CONFIG_DIR="/config-etc"
XROOTD_CONFIG="$CONFIG_DIR/xrootd.cf"
OPT_XRD_SSI=""

# COMPONENT_NAME is required by xrdssi plugin to
# choose which type of queries to launch against metadata
if [ "$COMPONENT_NAME" = 'worker' ]; then

    MYSQLD_SOCKET="/qserv/data/mysql/mysql.sock"
    XRDSSI_CONFIG="$CONFIG_DIR/xrdssi.cf"

    # Wait for local mysql to be configured and started
    while true; do
        if mysql --socket "$MYSQLD_SOCKET" --user="$MYSQLD_USER_QSERV"  --skip-column-names \
            -e "SELECT CONCAT('Mariadb is up: ', version())"
        then
            break
        else
            echo "Wait for MySQL startup"
        fi
        sleep 2
    done

    # TODO move to /qserv/run/tmp when it is managed as a shared volume
    export VNID_FILE="/qserv/data/mysql/cms_vnid.txt"
    if [ ! -e "$VNID_FILE" ]
    then
        WORKER=$(mysql --socket "$MYSQLD_SOCKET" --batch \
            --skip-column-names --user="$MYSQLD_USER_QSERV" -e "SELECT id FROM qservw_worker.Id;")
        if [ -z "$WORKER" ]; then
            >&2 echo "ERROR: unable to extract vnid from database"
            exit 2
        fi
        echo "$WORKER" > "$VNID_FILE"
    fi

    # Wait for at least one xrootd redirector readiness
    until timeout 1 bash -c "cat < /dev/null > /dev/tcp/${XROOTD_RDR_DN}/2131"
    do
        echo "Wait for xrootd redirector to be up and running  (${XROOTD_RDR_DN})..."
        sleep 2
    done

    OPT_XRD_SSI="-l @libXrdSsiLog.so -+xrdssi $XRDSSI_CONFIG"
fi

# Start service
#
echo "Start $service"
"$service" -c "$XROOTD_CONFIG" -n "$COMPONENT_NAME" -I v4 $OPT_XRD_SSI
