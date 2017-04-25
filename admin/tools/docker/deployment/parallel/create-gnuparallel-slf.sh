#!/bin/bash

set -e
set -x

# Create a *.slf file used by GNU parallel
# to access Qserv cluster

# @author  Fabrice Jammes, IN2P3

# Directory which contain configuration information for Qserv cluster
CLUSTER_CONFIG_DIR="$HOME/.lsst/qserv-cluster"

# ssh configuration file, optional
SSH_CONFIG="$CLUSTER_CONFIG_DIR/ssh_config"

# Machine names
ENV_INFRASTRUCTURE_FILE="$CLUSTER_CONFIG_DIR/env-infrastructure.sh"
. "$ENV_INFRASTRUCTURE_FILE"

SSHLOGINFILE="$CLUSTER_CONFIG_DIR/sshloginfile"

if [ -e "$SSH_CONFIG" ]
then
    SSH_CONFIG_OPT="-F $SSH_CONFIG" 
else
    echo "WARN: non-existing $SSH_CONFIG"
    SSH_CONFIG_OPT="" 
fi

rm -f "$SSHLOGINFILE"
for n in $MASTER $WORKERS
do
    echo "ssh $SSH_CONFIG_OPT $n" >> "$SSHLOGINFILE"
done 
