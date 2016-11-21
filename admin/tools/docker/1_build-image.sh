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

set -e

EUPS_TAG='qserv_latest'
TAG='qserv/qserv:latest'
VERSION=$(date --date='-1 month' +'%Y-%m')

usage() {
  cat << EOD

  Usage: $(basename "$0") [options]

  Available options:
    -C          Rebuild the images from scratch
    -d path     Directory containing dependency scripts, default to
                \$QSERV_DIR/admin/bootstrap
    -D          Build using 'qserv-dev' eups tag instead of 'qserv_latest'
    -h          This message

    Create Docker images from 'qserv_latest' (default) or 'qserv-dev' eups tags.

EOD
}

# get the options
while getopts Cd:Dh c ; do
    case $c in
            C) CACHE_OPT="--no-cache=true" ;;
            d) DEPS_DIR="$OPTARG" ;;
            D) EUPS_TAG="qserv-dev" ; TAG="qserv/qserv:dev" ;;
            h) usage ; exit 0 ;;
            \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if [ -z "$DEPS_DIR" -a -z "$QSERV_DIR" ] ; then
    printf "ERROR: directory containing dependency scripts not specified"
    usage
    exit 3
fi
test "$DEPS_DIR" || DEPS_DIR=$QSERV_DIR/admin/bootstrap

DIR=$(cd "$(dirname "$0")"; pwd -P)
DOCKERDIR="$DIR/latest"

# strip trailing slash
DEPS_DIR=$(echo "$DEPS_DIR" | sed 's%\(.*[^/]\)/*%\1%')

# WARN:
# Scripts used by Dockerfile:
# - must be located under the same root path than the Dockerfile,
# - must not be symlinks
# - git can't store physical links
# So, copy it from templates to SCRIPT_DIR directory
SCRIPT_DIR="$DOCKERDIR/scripts"

TPL_DEPS_SCRIPT="$DEPS_DIR/qserv-install-deps-debian8.sh"

printf "Add physical link to dependencies install script: %s\n" "$TPL_DEPS_SCRIPT"
ln -f "$TPL_DEPS_SCRIPT" "$SCRIPT_DIR/install-deps.sh"

# Build the image
printf "Building image %s from %s, using eups tag %s\n" \
    "$TAG" "$DOCKERDIR" "$EUPS_TAG"
docker build $CACHE_OPT --build-arg EUPS_TAG="$EUPS_TAG" --tag="$TAG" "$DOCKERDIR"

# Additional tag for release image
if [ "$EUPS_TAG" = 'qserv_latest' ]; then
    VERSION_TAG="qserv/qserv:$VERSION"

    docker tag "$TAG" "$VERSION_TAG"
    docker push "$TAG"
    docker push "$VERSION_TAG"

fi

printf "Image %s built successfully\n" "$TAG"
