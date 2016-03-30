# Rename this file to env.sh and edit configuration parameters
# env.sh is sourced by other scripts from the directory

# Nodes names
# ===========

MASTER=ccqserv100.in2p3.fr
WORKERS=$(echo ccqserv1{01..24}.in2p3.fr)
#MASTER=ccqserv125.in2p3.fr
#WORKERS=$(echo ccqserv1{26..49}.in2p3.fr)

# Image names
# ===========

# Used to set images names, can be:
#   1. a git ticket branch but with _ instead of /
#   2. a git tag
# example: tickets_DM-5402
BRANCH=dev

MASTER_IMAGE="qserv/qserv:${BRANCH}_master_$MASTER"  # Do not edit
WORKER_IMAGE="qserv/qserv:${BRANCH}_worker_$MASTER"  # Do not edit

# `docker run` settings
# =====================

# Data directory location on docker host, optional
HOST_DATA_DIR=/qserv/data               

# Log directory location on docker host, optional
HOST_LOG_DIR=/qserv/log

CONTAINER_NAME=qserv                                 # Do not edit

# shmux access
export PATH="$PATH:/opt/shmux/bin"

