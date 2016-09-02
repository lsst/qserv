#!/bin/sh

# @author  Fabrice Jammes, IN2P3/SLAC

set -x
set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -h          this message

  Launch Qserv integration tests on one Docker host

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

i=1
for node in $WORKERS;
do
    CSS_INFO="${CSS_INFO}CREATE NODE worker${i} type=worker port=5012 host=${node};
"
    i=$((i+1))
done
docker exec "$MASTER" bash -c ". /qserv/stack/loadLSST.bash && \
    setup qserv_distrib -t qserv-dev && \
    echo \"$CSS_INFO\" | qserv-admin.py && \
    qserv-test-integration.py"
