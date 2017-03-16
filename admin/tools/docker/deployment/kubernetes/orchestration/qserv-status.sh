#!/bin/bash

# Manage Qserv services inside pods 

# @author Fabrice Jammes SLAC/IN2P3

set -e

# Return service status as default behaviour
ACTION=status

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env.sh"

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -a action   action to perform on qserv services
                might be start, stop or status (default)

  Start, stop or display status of Qserv services on all
  pods.
EOD
}

# get the options
while getopts ha: c ; do
    case $c in
            h) usage ; exit 0 ;;
            a) ACTION="$OPTARG" ;;
            \?) usage ; exit 2 ;;
    esac
done
shift $(expr $OPTIND - 1)

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if ! echo "start stop status" | grep -w "$ACTION" > /dev/null; then
    usage
    exit 2
fi

parallel "kubectl exec {} -- /qserv/run/bin/qserv-${ACTION}.sh" ::: $MASTER_POD $WORKER_PODS
