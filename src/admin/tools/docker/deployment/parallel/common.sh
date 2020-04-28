# Advanced configuration parameters,
# sourced by env.sh
# env.sh is sourced by other scripts from the directory

# Node names 
# ==========

# Disjoint sequences of host names can
# be set directly using variables below

# Master hostname
if [ -z "$MASTER_FORMAT" ]
then
    MASTER_FORMAT="$HOSTNAME_FORMAT"
fi
MASTER=$(printf "$MASTER_FORMAT" "$MASTER_ID")    # Master hostname.

# Workers hostname
WORKERS=$(seq --format "$HOSTNAME_FORMAT" \
    --separator=' ' "$WORKER_FIRST_ID" \
    "$WORKER_LAST_ID")                            # Worker hostnames list.

# ssh hostname used to connect to master
if [ -z "$SSH_MASTER_FORMAT" -a -n "$SSH_HOSTNAME_FORMAT" ]
then
        SSH_MASTER_FORMAT="$SSH_HOSTNAME_FORMAT"
fi

if [ -n "$SSH_MASTER_FORMAT" ]
then
    SSH_MASTER=$(printf "$SSH_MASTER_FORMAT" "$MASTER_ID")    # Master ssh name.
else
    SSH_MASTER="$MASTER"
fi

# ssh hostnames used to connect to workers
if [ -n "$SSH_HOSTNAME_FORMAT" ]
then
    SSH_WORKERS=$(seq --format "$SSH_HOSTNAME_FORMAT" \
        --separator=' ' "$WORKER_FIRST_ID" \
        "$WORKER_LAST_ID")                              # Worker hostnames list. Do not edit
else
    SSH_WORKERS=$WORKERS
fi

# Advanced configuration
# ======================

CONTAINER_NAME=qserv                                # Do not edit

MASTER_IMAGE="qserv/qserv:${BRANCH}_master"         # Do not edit
WORKER_IMAGE="qserv/qserv:${BRANCH}_worker"         # Do not edit

