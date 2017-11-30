#!/bin/sh

# Remove Qserv pods from Kubernetes cluster

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)


usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -h          this message

  Remove Qserv pods from Kubernetes cluster
  and wait for Qserv to stop

EOD
}

# get the options
while getopts h c ; do
    case $c in
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

kubectl delete pods -l app=qserv
"$DIR"/wait-pods-terminate.sh
