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


usage() {
  cat << EOD

  Usage: $(basename "$0") [options] git-tag 

  Available options:
    -h          this message

  Create a docker image from using a git-tagged Qserv version 
  use a Docker image containing latest Qserv stack as input.

EOD
}

# Get the options
while getopts h c ; do
    case $c in
            h) usage ; exit 0 ;;
            \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 1 ] ; then
    usage
    exit 2
fi

GIT_TAG=$1

DIR=$(cd "$(dirname "$0")"; pwd -P)
DOCKERDIR="$DIR/tagged"


# Build the image

TIMESTAMP=$(date --utc)
DOCKERFILE="$DOCKERDIR/Dockerfile"
cp "$DOCKERDIR/Dockerfile.tpl" "$DOCKERFILE"
sed -i "s%{{GIT_TAG_OPT}}%${GIT_TAG}%g" "$DOCKERFILE"
# Force the build of the branch by changing timestamp
sed -i "s%{{TIMESTAMP}}%${TIMESTAMP}%g" "$DOCKERFILE"

# Docker tag must not contain '/'
VERSION=$(echo "$GIT_TAG" | tr '/' '_')
TAG="qserv/qserv:$VERSION"
printf "Building development image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --tag="$TAG" "$DOCKERDIR"
docker push "$TAG"

printf "Image %s built successfully\n" "$TAG"
