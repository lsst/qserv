# Rename this file to env.sh and edit configuration parameters
# env.sh is sourced by other scripts from the directory

# shmux path
PATH=$PATH:/opt/shmux/bin/

# Image names
# ===========

# Used to set images names, can be:
#   1. a git ticket branch but with _ instead of /
#   2. a git tag
# example: tickets_DM-5402
BRANCH=dev

# `docker run` settings
# =====================

# Customized configuration templates directory location
# on docker host, optional
# See <qserv-src-dir>/doc/source/HOW-TO/docker-custom-configuration.rst
# for additional information
HOST_CUSTOM_DIR=/qserv/custom

# Data directory location on docker host, optional
HOST_DATA_DIR=/qserv/data

# Log directory location on docker host, optional
HOST_LOG_DIR=/qserv/log

# Qserv temporary directory location on docker host, optional
HOST_TMP_DIR=/qserv/tmp

# ulimit memory lock setting, in bytes, optional
ULIMIT_MEMLOCK=10737418240

# Nodes names
# ===========

# Master id
MASTER_ID=100

# Optional, default to <HOSTNAME_FORMAT>
# MASTER_FORMAT="lsst-qserv-master%02g"

# Optional, default to <SSH_HOSTNAME_FORMAT>
# then $MASTER"
# SSH_MASTER_FORMAT="qserv-master01"

# Format for all node's hostname
HOSTNAME_FORMAT="ccqserv%03g.in2p3.fr"

# Optional, format for node's ssh name
# Used at NCSA
# SSH_HOSTNAME_FORMAT="qserv-db%02g"

# Workers range
WORKER_FIRST_ID=101
WORKER_LAST_ID=124

# Advanced configuration
# ======================

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "${DIR}/common.sh"

