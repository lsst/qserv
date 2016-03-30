#!/bin/sh

# WARN: this procedure is not officially supported, use it at your own risk

# Stop all services on Qserv master and remove all temporary result/message tables
# from its MySQL 'qservResult' database.

# @author Fabrice Jammes IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

RESULT_DATA_DIR=/qserv/data/mysql/qservResult

ssh "$MASTER" "docker exec $CONTAINER_NAME \
    sh -c \"/qserv/run/bin/qserv-stop.sh; \
    rm -f $RESULT_DATA_DIR/result_* $RESULT_DATA_DIR/message_*\""

