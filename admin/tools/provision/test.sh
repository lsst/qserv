#!/bin/bash

# Test script which performs the following tasks:

# Create image
# Boot instances
# Launch Qserv containers
# Lauch integration tests

# @author  Oualid Achbal, IN2P3
# @author  Fabrice Jammes, IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -c          update CentOS7/Docker snapshot
    -p          provision Qserv cluster on Openstack
    -s          launch Qserv integration test using shmux
    -S          launch Qserv integration test using swarm, disable previous option

  Create up to date CentOS7 snapshot and use it to provision Qserv cluster on
  Openstack, then install Qserv and launch integration test on it.
  If no option provided, use '-p -S' by default.

  Pre-requisites: Openstack RC file need to be sourced and $DIR/env-openstack.sh available.

EOD
}

# get the options
while getopts hcpsS c ; do
    case $c in
        h) usage ; exit 0 ;;
        c) CREATE="TRUE" ;;
        p) PROVISION="TRUE" ;;
        s) SHMUX="TRUE" ;;
        S) SWARM="TRUE" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift $(($OPTIND - 1))

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if [ "$OPTIND" -eq 1 ]; then
    PROVISION="TRUE"
    SWARM="TRUE"
fi

. "$DIR/env-openstack.sh"

if [ -n "$CREATE" ]; then
    echo "Create up to date snapshot image"
    "$DIR/create-image.py" --cleanup --config "$CONF_FILE" -vv
fi

if [ -n "$PROVISION" ]; then
    echo "Provision Qserv cluster on Openstack"
    "$DIR/provision-qserv.py" --cleanup --config "$CONF_FILE" --nb-servers "$NB_SERVERS" -vv
fi

. "$DIR/env-infrastructure.sh"

if [ -n "$SWARM" ]; then
	SWARM_DIR="$DIR/../docker/deployment/swarm"
	ln -f "$DIR/ssh_config" "$SWARM_DIR"
	ln -f "$DIR/env-infrastructure.sh" "$SWARM_DIR"
	SSH_CFG="$SWARM_DIR/ssh_config"

	"$SWARM_DIR"/setup-and-test.sh

elif [ -n "$SHMUX" ]; then

    echo "Launch integration tests using shmux"

    # Warning : if  multinode tests failed save your ~/.ssh/config
    # your old ~/.ssh/config is in ~/.ssh/config.backup

    DATE=$(date +%Y%m%d_%H-%M-%S)
    SSH_CONFIG="$HOME/.ssh/config"
    SSH_CONFIG_BACKUP="$SSH_CONFIG.backup.${DATE}"
    if [ -f "$SSH_CONFIG" ]; then
        echo  "WARN: backuping $SSH_CONFIG to $SSH_CONFIG_BACKUP"
        mv "$SSH_CONFIG" "$SSH_CONFIG_BACKUP"
    fi
    cp "$DIR/ssh_config" ~/.ssh/config
    cd ../docker/deployment/parallel

    # Update env.sh
    cp env.example.sh env.sh
    sed -i "s/HOSTNAME_FORMAT=\"qserv%g.domain.org\"/HOSTNAME_FORMAT=\"${HOSTNAME_TPL}%g\"/" env.sh
    sed -i "s/WORKER_LAST_ID=3/WORKER_LAST_ID=${WORKER_LAST_ID}/" env.sh

    # Run multinode tests
    echo "Launch multinode tests"
    ./run-multinode-tests.sh

    if [ -f "$SSH_CONFIG_BACKUP" ]; then
        echo  "Restoring backup of $SSH_CONFIG"
        mv "$SSH_CONFIG_BACKUP" "$SSH_CONFIG"
    fi
fi
