#!/bin/sh

# LSST Data Management System
# Copyright 2015 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

# Create Docker images containing Qserv master and worker instances

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DOCKER_IMAGE="qserv/qserv:dev"
PUSH_TO_HUB="true"

usage() {
    cat << EOD
Usage: $(basename "$0") [options]

Available options:
  -h          this message
  -i image    Docker image to be used as input, default to $DOCKER_IMAGE
  -L			Do not push image to Docker Hub

Create docker images containing Qserv master and worker instances,
use an existing Qserv Docker image as input.

EOD
}

# Get the options
while getopts hi:L c ; do
    case $c in
        h) usage ; exit 0 ;;
        i) DOCKER_IMAGE="$OPTARG" ;;
        L) PUSH_TO_HUB="false" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

DIR=$(cd "$(dirname "$0")"; pwd -P)
DOCKERDIR="$DIR/configured"

# Build the master image

DOCKERFILE="$DOCKERDIR/Dockerfile"
cp "$DOCKERDIR/Dockerfile.tpl" "$DOCKERFILE"
sed -i 's%{{NODE_TYPE_OPT}}%-m%g' "$DOCKERFILE"
sed -i "s%{{DOCKER_IMAGE_OPT}}%$DOCKER_IMAGE%g" "$DOCKERFILE"
sed -i 's%{{COMMENT_ON_WORKER_OPT}}%%g' "$DOCKERFILE"

TAG="${DOCKER_IMAGE}_master"
printf "Building master image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --tag="$TAG" "$DOCKERDIR"
printf "Image %s built successfully\n" "$TAG"

if [ "$PUSH_TO_HUB" = "true" ]; then
    docker push "$TAG"
    printf "Image %s pushed successfully\n" "$TAG"
fi


# Build the worker image

cp "$DOCKERDIR/Dockerfile.tpl" "$DOCKERFILE"
sed -i 's%{{NODE_TYPE_OPT}}%%g' "$DOCKERFILE"
sed -i "s%{{DOCKER_IMAGE_OPT}}%$DOCKER_IMAGE%g" "$DOCKERFILE"
sed -i 's%{{COMMENT_ON_WORKER_OPT}}%# %g' "$DOCKERFILE"

TAG="${DOCKER_IMAGE}_worker"
printf "Building worker image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --tag="$TAG" "$DOCKERDIR"
printf "Image %s built successfully\n" "$TAG"

if [ "$PUSH_TO_HUB" = "true" ]; then
    docker push "$TAG"
    printf "Image %s pushed successfully\n" "$TAG"
fi
