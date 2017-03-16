#!/bin/bash

# Create a *.slf file used by GNU parallel
# to access Qserv cluster

# @author  Fabrice Jammes, IN2P3

CLUSTER_CONFIG_DIR="$HOME/.lsst/qserv-cluster"

SSH_CONFIG="$CLUSTER_CONFIG_DIR/ssh_config"
SSHLOGINFILE="$CLUSTER_CONFIG_DIR/sshloginfile"

if [ ! -e "$SSH_CONFIG" ]
then
    echo "ERROR: non-existing $SSH_CONFIG"
    exit 1
fi

NODES=$(grep -w Host "$SSH_CONFIG" | sed 's/^.*Host //' )

for n in $NODES
do
    echo "ssh -F $SSH_CONFIG $n" > "$SSHLOGINFILE"
done 
