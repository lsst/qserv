#!/bin/sh

# Create Docker image containing Qserv build performed using a given git
# branch/tag. Git repository can be local or remote.

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

usage() {
    cat << EOD
Usage: $(basename "$0") -B <branch/tag>|-S <source_directory> [options]

Available options:
  -B          Remote git branch/tag used for the build
              (from https://github.com/lsst/qserv)
  -h          This message
  -L          Do not push image to Docker Hub
  -S          Path to local git repository used for the build.
              Working tree must point on current branch HEAD.
              Use the latest commit of the current branch.
  -T          Name of the produced image

Create Docker image containing Qserv build performed using a given git
branch/tag:
- Default behaviour is to use \$QSERV_DIR HEAD, but ither a local source
  repository or a remote git branch/tag can be provided
- use a Docker image containing latest Qserv stack as input (i.e.
  qserv/qserv:dev).

EOD
}

DIR=$(cd "$(dirname "$0")"; pwd -P)
DOCKERDIR="$DIR/git"
DOCKERTAG=''
GIT_REF=''
GITHUB_REPO="https://github.com/lsst/qserv"
PUSH_TO_HUB="true"
SRC_DIR="$QSERV_DIR"

while getopts B:hLS:T: c ; do
    case $c in
        B) GIT_REF="$OPTARG" ;;
        h) usage ; exit 0 ;;
        L) PUSH_TO_HUB="false" ;;
        S) SRC_DIR="$OPTARG" ;;
        T) DOCKERTAG="$OPTARG" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ]; then
    usage
    exit 2
fi

if [ -z "$GIT_REF" -a -z "$SRC_DIR" ]; then
    echo "ERROR: provide either branch/tag or local source directory"
    usage
    exit 2
#elif [ -n "$GIT_REF" -a -n "$SRC_DIR" ]; then
#    echo "ERROR: must not provide both branch/tag and local source directory"
#    usage
#    exit 2
elif [ -n "$GIT_REF" ] ; then
    printf "Using remote branch/tag %s from %s\n" "$GIT_REF" "$GITHUB_REPO"
	TMP_DIR=$(mktemp --directory --suffix "_qserv")
    # git archive is not supported by GitHub
    git clone -b "$GIT_REF" --single-branch "$GITHUB_REPO" "$TMP_DIR"
    SRC_DIR="$TMP_DIR"
else
    GIT_REF=$(git rev-parse --abbrev-ref HEAD)
    # strip trailing slash
    SRC_DIR=$(echo "$SRC_DIR" | sed 's%\(.*[^/]\)/*%\1%')
    printf "Using local branch %s from %s\n" "$GIT_REF" "$SRC_DIR"
    # Put source code inside Docker build directory
fi

git archive --remote "$SRC_DIR" --output "$DOCKERDIR/src/qserv_src.tar" "$GIT_REF"

if [ -n "$TMP_DIR" ]; then
	rm -rf "$TMP_DIR"
fi

if [ -z "$DOCKERTAG" ]; then
    # Docker tags must not contain '/'
    TAG=$(echo "$GIT_REF" | tr '/' '_')
    DOCKERTAG="qserv/qserv:$TAG"
fi

docker build --tag="$DOCKERTAG" "$DOCKERDIR"

if [ "$PUSH_TO_HUB" = "true" ]; then
    docker push "$DOCKERTAG"
    printf "Image %s pushed successfully\n" "$TAG"
fi
