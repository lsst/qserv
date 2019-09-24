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


# Create a Qserv docker image.
# Useful to verify Qserv system-dependency list,
# indeed Linux distribution images for Docker are very minimalist

# @author  Fabrice Jammes, IN2P3/SLAC

set -eux

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/conf.sh"

TAG="$DOCKER_REPO:dev"

CACHE_OPT=''

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -C          Rebuild the images from scratch
    -h          This message

    Create a container image for Qserv and dependencies
    using 'qserv-dev' eups tag.

    Write container image tag in $DEPS_TAG_FILE, in order to allow
    lsst-dm-ci to build Qserv configured images from this container image.

EOD
}

# get the options
while getopts CDh c ; do
    case $c in
            C) CACHE_OPT="--no-cache=true" ;;
            D) ;; # legacy option; now default behavior
            h) usage ; exit 0 ;;
            \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

DOCKERDIR="$DIR/deps"

# Build the image
DEPS_TAG="$DOCKER_REPO:${DEPS_TAG_PREFIX}_$(date -u '+%Y%m%d_%H%M')"
printf "Building image %s from %s, using eups tag 'qserv-dev'\n" \
    "$DEPS_TAG" "$DOCKERDIR"
docker build $CACHE_OPT --build-arg DEPS_TAG=$DEPS_TAG --tag="$TAG" "$DOCKERDIR"

docker push "$TAG"

docker tag "$TAG" "$DEPS_TAG"
docker push "$DEPS_TAG"

printf "Image %s built successfully\n" "$DEPS_TAG"

echo "$DEPS_TAG" > "$DEPS_TAG_FILE"
