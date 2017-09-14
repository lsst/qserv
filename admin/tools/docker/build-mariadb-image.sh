#!/bin/sh

# Create a MariaDB+Scisql docker image.
# Use scisql master tip

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

MARIADB_VERSION="10.1.25"

TAG="$DOCKER_REPO_MARIADB:$MARIADB_VERSION"
VERSION=$(date --date='-1 month' +'%Y-%m')

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -C          Rebuild the images from scratch
    -h          This message

    Create Docker images containing Mariadb $MARIADB_VERSION and scisql (master
    branch tip)

EOD
}

# get the options
while getopts Ch c ; do
    case $c in
            C) CACHE_OPT="--no-cache=true" ;;
            h) usage ; exit 0 ;;
            \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

DOCKERDIR="$DIR/mariadb"

# Build the image
printf "Building image %s from %s, using mariadb $MARIADB_VERSION\n" \
    "$TAG" "$DOCKERDIR"
docker build $CACHE_OPT --build-arg="$MARIADB_VERSION" --tag="$TAG" "$DOCKERDIR"

docker push "$TAG"

printf "Image %s built successfully\n" "$TAG"
