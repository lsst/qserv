
# Set nodes names
MASTER=ccqserv100.in2p3.fr
WORKERS=$(echo ccqserv1{01..24}.in2p3.fr)
#MASTER=ccqserv125.in2p3.fr
#WORKERS=$(echo ccqserv1{26..49}.in2p3.fr)

# set image names
BRANCH=dev
DOCKER_NAMESPACE=qserv
MASTER_IMAGE="$DOCKER_NAMESPACE/qserv:${BRANCH}_master_$MASTER"
WORKER_IMAGE="$DOCKER_NAMESPACE/qserv:${BRANCH}_worker_$MASTER"
CONTAINER_NAME=qserv_dev

# shmux access
export PATH="$PATH:/opt/shmux/bin"

# docker settings
HOST_LOG_DIR=/qserv/data
HOST_DATA_DIR=/qserv/log
