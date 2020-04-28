#!/bin/bash

# Copy initial MySQL/qserv data from container to host disk."
# This step is a pre-requisite to data-loading

# @author  Fabrice Jammes, IN2P3

set -x

DEST_DIR="/qserv/data"

DIR=$(cd "$(dirname "$0")"; pwd -P)
cd "${DIR}/.."
. "env.sh"


usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message


  Copy initial MySQL/qserv data from container to host disk.
  Pre-requisites:
      - run 'run.sh' with HOST_DATA_DIR commented in 'env.sh'
      - run 'stop.sh -K'

EOD
}

# get the options
while getopts h ; do
    case $c in
            h) usage ; exit 0 ;;
            \?) usage ; exit 2 ;;
    esac
done
shift $(($OPTIND - 1))

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

read -p "WARNING: This script might erase huge data set. Confirm? " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

shmux -c "docker cp qserv:/qserv/data /tmp" $SSH_MASTER $SSH_WORKERS
shmux -c "sudo -S sh -c 'rm -rf \"$DEST_DIR\" && mv /tmp/data \"$DEST_DIR\" && chown -R qserv:qserv \"$DEST_DIR\"'" $SSH_MASTER $SSH_WORKERS

