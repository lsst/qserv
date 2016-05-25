# Rename this file to env.sh and edit variables
# Configuration file sourced by other scripts from the directory

# VERSION can be a git ticket branch but with _ instead of /
# example: u_fjammes_DM-4295
VERSION=dev

NB_WORKERS=$INSTANCE_LAST_ID

# `docker run` settings
# =====================

# Data directory location on docker host, optional
# NOT SUPPORTED: mysql data created at configuration time is removed
# HOST_DATA_DIR=/qserv/data

# Log directory location on docker host, optional
# NOT SUPPORTED
# HOST_LOG_DIR=/qserv/log

# Nodes names
# ===========

MASTER="${HOSTNAME_TPL}0"

for i in $(seq 1 "$NB_WORKERS");
do
    WORKERS="$WORKERS ${HOSTNAME_TPL}${i}"
done

# Set images names
MASTER_IMAGE="qserv/qserv:${VERSION}_master"
WORKER_IMAGE="qserv/qserv:${VERSION}_worker"


