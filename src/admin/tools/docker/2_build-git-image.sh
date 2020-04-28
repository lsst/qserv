#!/bin/sh

# Create Docker image containing Qserv build performed using a given git
# branch/tag. Git repository can be local or remote.

# @author  Fabrice Jammes, IN2P3/SLAC

set -eux

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

DEPS_IMAGE=""

usage() {
    cat << EOD
Usage: $(basename "$0") [options] local-path [local-path1] [local-path2]

Available options:
  -h            This message
  -D            Name of image containing Qserv dependencies
                default to content of $DEPS_TAG_FILE or if not exists: $DOCKER_REPO:$DEPS_TAG_DEFAULT
  -F            Allow to build even if user uid on host is not 1000
                (chmod o+w on source directories is required as a pre-requisite)
  -L            Do not push image to Docker Hub
  -T            Prefix for the name of the produced images,
                default is to compute it from git branch name.

Create Docker image containing Qserv binaries.

Qserv is built using a local git repository whose path is provided as argument
Other local git directories supporting eups build can be provided as arguments and
will also be built.

WARNING: Build is performed in Docker image by user with uid=1000 and
gid=1000. It is better to have same uid/gid on host machine in order for Docker
to write compilation product on host filesystem. Here's and example describing
how to start such a machine: https://github.com/fjammes/os-qserv-build-node

EOD
}

# Name of image produced by current script
QSERV_IMAGE=''

FORCE="false"
PUSH_TO_HUB="true"

while getopts hD:FLST: c ; do
    case $c in
        D) DEPS_IMAGE="$OPTARG" ;;
        F) FORCE="true" ;;
        h) usage ; exit 0 ;;
        L) PUSH_TO_HUB="false" ;;
        T) QSERV_IMAGE="$OPTARG" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -eq 0 ]; then
    usage
    exit 2
fi


if [ -z "$DEPS_IMAGE" ]; then
    echo "Qserv image not provided, switching to locally build image"
    if [ -f "$DEPS_TAG_FILE" ]
    then
        >&2 echo "ERROR: local build not available, use -D option to provide a source Qserv dependencies image"
        exit 1
        QSERV_IMAGE=$(cat $DEPS_TAG_FILE)
    else
        DEPS_IMAGE="$DOCKER_REPO:$DEPS_TAG_DEFAULT"
    fi

fi

HOST_SRC_DIRS="$@"

OPT_MOUNT=""
SRC_DIRS=""
for HOST_SRC_DIR in $HOST_SRC_DIRS
do
    PRODUCT=$(basename $HOST_SRC_DIR)
    SRC_DIR="/home/qserv/src/$PRODUCT"
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
docker run -e SRC_DIRS --name "$CONTAINER" -t -u root \
    -v "$DIR"/git/scripts:/home/qserv/bin \
    $OPT_MOUNT -- "$DEPS_IMAGE" \
    su -c 'bash -lc "/home/qserv/bin/eups-builder.sh"' qserv

if [ -z "$QSERV_IMAGE" ]; then
    # Docker tags must not contain '/'
    TAG=$(echo "$GIT_REF" | tr '/' '_')
    QSERV_IMAGE="$DOCKER_REPO:$TAG"
fi

docker commit "$CONTAINER" "$QSERV_IMAGE"
docker rm "$CONTAINER"

if [ "$PUSH_TO_HUB" = "true" ]; then
    docker push "$QSERV_IMAGE"
    printf "Image %s pushed successfully\n" "$TAG"
fi
