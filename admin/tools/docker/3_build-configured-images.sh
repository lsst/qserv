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

set -eux

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

PUSH_TO_HUB="true"

usage() {
    cat << EOD
Usage: $(basename "$0") [options]

Available options:
  -h          this message
  -i image    Docker image containing Qserv dependencies to be used as input,
              default to content of $DEPS_TAG_FILE
  -L          Do not push image to Docker Hub

Create docker images containing Qserv master and worker instances,
use an existing Qserv Docker image as input.

EOD
}
QSERV_IMAGE=""

# Get the options
while getopts hi:L c ; do
    case $c in
        h) usage ; exit 0 ;;
        i) QSERV_IMAGE="$OPTARG" ;;
        L) PUSH_TO_HUB="false" ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if [ -z "$QSERV_IMAGE" ]; then
    echo "Qserv image not provided, switching to locally build image"
    if [ ! -f "$DEPS_TAG_FILE" ]
    then
        >&2 echo "ERROR: local build not available, use -i option to provide a source Qserv image"
        exit 1
    fi
    QSERV_IMAGE=$(cat $DEPS_TAG_FILE)
fi

makeDockerfile() {
    awk \
        -v NODE_TYPE=${NODETYPE} \
        -v QSERV_IMAGE=${QSERV_IMAGE} \
        -v COMMENT_ON_WORKER=${COMMENTONWORKER} \
        '{gsub(/<NODE_TYPE>/, NODE_TYPE);
         gsub(/<QSERV_IMAGE>/, QSERV_IMAGE);
         gsub(/<COMMENT_ON_WORKER>/, COMMENT_ON_WORKER);
         print}' "$DOCKERDIR/Dockerfile.tpl" > "$DOCKERFILE"
         
    printf "Building %s image %s from %s\n" "$TARGET" "$TAG" "$DOCKERDIR"
    docker build --tag="$TAG" "$DOCKERDIR"
    printf "Image %s built successfully\n" "$TAG"
    
    if [ "$PUSH_TO_HUB" = "true" ]; then
        docker push "$TAG"
        printf "Image %s pushed successfully\n" "$TAG"
    fi
    
}

DOCKERDIR="$DIR/configured"
DOCKERFILE="$DOCKERDIR/Dockerfile"

# Templating is required here
# docker build --build-arg=[] is too restrictive

# Build the master image
NODETYPE="-m"
COMMENTONWORKER=""
TARGET="master"
TAG="${QSERV_IMAGE}_master"
makeDockerfile


# Build the master-multi image
NODETYPE="-c"
COMMENTONWORKER=""
TARGET="master_multi"
TAG="${QSERV_IMAGE}_master_multi"
makeDockerfile


# Build the master-shared image
NODETYPE="-s"
COMMENTONWORKER=""
TARGET="master_shared"
TAG="${QSERV_IMAGE}_master_shared"
makeDockerfile

# Build the worker image
NODETYPE=""
COMMENTONWORKER="# "
TARGET="worker"
TAG="${QSERV_IMAGE}_worker"
makeDockerfile
