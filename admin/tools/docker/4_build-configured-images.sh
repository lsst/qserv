#!/bin/sh

# LSST Data Management System
# Copyright 2014 LSST Corporation.
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
set -x

DOCKER_IMAGE="qserv/qserv:dev"

usage() {
  cat << EOD

Usage: `basename $0` [options] host

  Available options:
    -h          this message
    -i          Docker image to be used as input, default to $DOCKER_IMAGE

  Create docker images containing Qserv master and worker instances,
  use an existing Qserv Docker image as input.
  Qserv master fqdn or ip adress must be provided as unique argument.

EOD
}

# Get the options
while getopts hi: c ; do
    case $c in
            h) usage ; exit 0 ;;
            i) DOCKER_IMAGE="$OPTARG" ;;
            \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 1 ] ; then
    usage
    exit 2
fi

MASTER=$1

DIR=$(cd "$(dirname "$0")"; pwd -P)
DOCKERDIR="$DIR/configured"

# Build the master image

DOCKERFILE="$DOCKERDIR/Dockerfile"
cp "$DOCKERDIR/Dockerfile.tpl" "$DOCKERFILE"
sed -i 's%{{NODE_TYPE_OPT}}%-m%g' "$DOCKERFILE"
sed -i "s%{{DOCKER_IMAGE_OPT}}%$DOCKER_IMAGE%g" "$DOCKERFILE"
sed -i "s%{{MASTER_FQDN_OPT}}%${MASTER}%g" "$DOCKERFILE"

TAG="${DOCKER_IMAGE}_master_${MASTER}"
printf "Building master image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --no-cache=true --tag="$TAG" "$DOCKERDIR"
docker push $TAG

printf "Image %s built successfully\n" "$TAG"

# Build the worker image

cp "$DOCKERDIR/Dockerfile.tpl" "$DOCKERFILE"
sed -i 's%{{NODE_TYPE_OPT}}%%g' "$DOCKERFILE"
sed -i "s%{{DOCKER_IMAGE_OPT}}%$DOCKER_IMAGE%g" "$DOCKERFILE"
sed -i "s%{{MASTER_FQDN_OPT}}%${MASTER}%g" "$DOCKERFILE"

TAG="${DOCKER_IMAGE}_worker_${MASTER}"
printf "Building wroker image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --no-cache=true --tag="$TAG" "$DOCKERDIR"
docker push $TAG

printf "Image %s built successfully\n" "$TAG"
