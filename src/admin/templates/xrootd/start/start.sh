#!/bin/sh

# Start cmsd or
# setup ulimit and start xrootd

# @author  Fabrice Jammes, IN2P3/SLAC

set -eux

usage() {
    cat << EOD

Usage: `basename $0` [options] [cmd]

  Available options:
    -S <service> Service to start, default to xrootd

  Prepare cmsd and xrootd (ulimit setup) startup and
  launch associated startup script using qserv user.
EOD
}

service=xrootd

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

XROOTD_RDR_DN="{{.XrootdRedirectorDn}}"

if hostname | egrep "^${XROOTD_RDR_DN}-[0-9]+$"
then
    COMPONENT_NAME='manager'
else
    COMPONENT_NAME='worker'
fi
export COMPONENT_NAME

if [ "$service" = "xrootd" -a "$COMPONENT_NAME" = 'worker' ]; then

    # Increase limit for locked-in-memory size
    MLOCK_AMOUNT=$(grep MemTotal /proc/meminfo | awk '{printf("%.0f\n", $2 - 1000000)}')
    ulimit -l "$MLOCK_AMOUNT"

fi

su qserv -c "/config-start/xrd.sh -S $service"
