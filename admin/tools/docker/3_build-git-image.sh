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
Usage: $(basename "$0") [options] [local-path]

Available options:
  -R git-ref    Use remote git branch/tag for the build
                (from https://github.com/lsst/qserv)
				Example: 'tickets/DM-6444'
  -h            This message
  -L            Do not push image to Docker Hub
  -T            Prefix for the name of the produced images,
                default is to compute it from git branch name.

Create Docker image containing Qserv binaries.

Qserv is build using a given git repository:
- without -R option, a local source repository path can be provided
  as argument, default behaviour is to use \$QSERV_DIR.
  Repository working tree must point on current branch HEAD, used for
  the built.
- with -R option, git-ref will be cloned from GitHub and used
  for the build. local-path argument must not be provided.

All builds use a Docker image containing latest Qserv stack as input
(i.e. image named $DOCKER_REPO:dev).

EOD
}

DOCKERDIR="$DIR/git"
DOCKERTAG=''
GIT_REF='master'
GITHUB_REPO="https://github.com/lsst/qserv"
PUSH_TO_HUB="true"
SRC_DIR="$QSERV_DIR"
LOCAL=true

while getopts R:hLST: c ; do
    case $c in
        R) LOCAL=false &&  GIT_REF="$OPTARG";;
        h) usage ; exit 0 ;;
        L) PUSH_TO_HUB="false" ;;
        T) DOCKERTAG="$OPTARG" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if $LOCAL ; then
    if [ $# -gt 1 ]; then
        usage
        exit 2
    elif [ -n "$1" ]; then
        SRC_DIR="$1"
    fi
    if [ -z "$SRC_DIR" ]; then
        echo "ERROR: No source directory provided and undefined \$QSERV_DIR."
        usage
        exit 2
    fi
    GIT_REF=$(git rev-parse --abbrev-ref HEAD)
    GIT_REPO="$SRC_DIR"
else
    # Path argument and -R option can not be both provided
    if [ $# -gt 0 ]; then
        usage
        exit 2
    fi
    # git archive is not supported by GitHub
    GIT_REPO="$GITHUB_REPO"
fi

printf "Using branch/tag %s from %s\n" "$GIT_REF" "$GIT_REPO"

if [ -n "$DOCKERDIR/src/qserv" ]; then
	rm -rf "$DOCKERDIR/src/qserv"
fi

# Put source code inside Docker build directory
git clone -b "$GIT_REF" --single-branch "$GIT_REPO" "$DOCKERDIR/src/qserv"

if [ -z "$DOCKERTAG" ]; then
    # Docker tags must not contain '/'
    TAG=$(echo "$GIT_REF" | tr '/' '_')
    DOCKERTAG="$DOCKER_REPO:$TAG"
fi

DOCKERFILE="$DOCKERDIR/Dockerfile"

awk \
-v DOCKER_REPO="$DOCKER_REPO" \
'{gsub(/<DOCKER_REPO>/, DOCKER_REPO);
  print}' "$DOCKERDIR/Dockerfile.tpl" > "$DOCKERFILE"

docker build --tag="$DOCKERTAG" "$DOCKERDIR"

if [ "$PUSH_TO_HUB" = "true" ]; then
    docker push "$DOCKERTAG"
    printf "Image %s pushed successfully\n" "$TAG"
fi

rm -rf "$DOCKERDIR/src/qserv"
