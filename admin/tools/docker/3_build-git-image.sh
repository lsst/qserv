#!/bin/sh

# Create Docker image containing Qserv build performed using a given git
# branch/tag. Git repository can be local or remote.

# @author  Fabrice Jammes, IN2P3/SLAC

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

usage() {
    cat << EOD
Usage: $(basename "$0") [options] local-path [local-path1] [local-path2] 

Available options:
  -R git-ref    Use git branch/tag for the build
                (from https://github.com/lsst/qserv)
                Example: 'tickets/DM-6444'
  -h            This message
  -L            Do not push image to Docker Hub
  -T            Prefix for the name of the produced images,
                default is to compute it from git branch name.

Create Docker image containing Qserv binaries.

Qserv is build using a local source repository whose path is provided as argument
Other source directories supporting eups build can be provided as argument and
will also be build.

WARNING: Build is performed in Docker image with user with uid=1000 and
gid=1000. It is better to have same uid/gid on host machine in order for Docker
to write compilation product on host filesystem. Here's and example describing
how to start such a machine: https://github.com/fjammes/os-qserv-build-node

All builds use a Docker image containing latest Qserv stack as input
(i.e. image named $DOCKER_REPO:dev).

EOD
}

DOCKERTAG=''
FORCE="false"
PUSH_TO_HUB="true"

while getopts hFLST: c ; do
    case $c in
        F) FORCE="true" ;;
        h) usage ; exit 0 ;;
        L) PUSH_TO_HUB="false" ;;
        T) DOCKERTAG="$OPTARG" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -eq 0 ]; then
    usage
    exit 2
fi
HOST_SRC_DIRS="$@"

OPT_MOUNT=""
SRC_DIRS=""
for HOST_SRC_DIR in $HOST_SRC_DIRS
do
    PRODUCT=$(basename $HOST_SRC_DIR)
    SRC_DIR="/home/qserv/$PRODUCT"
    SRC_DIRS="$SRC_DIRS $SRC_DIR"
    OPT_MOUNT="$OPT_MOUNT -v $HOST_SRC_DIR:$SRC_DIR"
done

# Remove leading whitespace
SRC_DIRS=$(echo "${SRC_DIRS}" | sed -e 's/^[[:space:]]*//')

# Last product in the list should be qserv, if not change the tag
if [ "$PRODUCT" = "qserv" ]; then
    GIT_REF=$(cd "$HOST_SRC_DIR" && git rev-parse --abbrev-ref HEAD)
else
    GIT_REF=$(cd "$HOST_SRC_DIR" && git rev-parse --abbrev-ref HEAD)
    GIT_REF="${PRODUCT}_${GIT_REF}"
fi

DOCKER_IMAGE=qserv/qserv:dev
CONTAINER="qserv_build"

HOST_UID=$(id -u)
if [ "$FORCE" = "false" -a $HOST_UID != 1000 ]
then
    echo "WARN: User on host must have uid=1000 and gid=1000"
    echo -n "if not run 'chmod o+w' on qserv source directory "
    echo "and run current script with -F option"
    exit
fi

export SRC_DIRS
docker run -e SRC_DIRS --name "$CONTAINER" -t -u qserv \
    -v "$DIR"/git/scripts:/home/qserv/bin \
    $OPT_MOUNT -- "$DOCKER_IMAGE" \
    bash /home/qserv/bin/eups-builder.sh

if [ -z "$DOCKERTAG" ]; then
    # Docker tags must not contain '/'
    TAG=$(echo "$GIT_REF" | tr '/' '_')
    DOCKERTAG="$DOCKER_REPO:$TAG"
fi

docker commit "$CONTAINER" "$DOCKERTAG"
docker rm "$CONTAINER"

if [ "$PUSH_TO_HUB" = "true" ]; then
    docker push "$DOCKERTAG"
    printf "Image %s pushed successfully\n" "$TAG"
fi
