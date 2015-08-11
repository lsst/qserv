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

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -d path     directory containing dependency scripts, default to
                \$QSERV_DIR/admin/bootstrap

  Create Docker images for Qserv release and development

EOD
}

# get the options
while getopts hd: c ; do
    case $c in
            h) usage ; exit 0 ;;
            d) DEPS_DIR="$OPTARG" ;;
            \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

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
DEPS_DIR=$(echo $DEPS_DIR | sed 's%\(.*[^/]\)/*%\1%')

# WARN:
# Scripts used by Dockerfile:
# - must be located under the same root path than the Dockerfile,
# - must not be symlinks
# - git can't store physical links
# So, copy it from templates to SCRIPT_DIR directory
SCRIPT_DIR="$DOCKERDIR/scripts"

. "$DOCKERDIR"/dist.sh
TPL_DEPS_SCRIPT="$DEPS_DIR/qserv-install-deps-"$DIST".sh"

printf "Add physical link to dependencies install script: %s\n" "$TPL_DEPS_SCRIPT"
ln -f "$TPL_DEPS_SCRIPT" "$SCRIPT_DIR/install-deps.sh"

# Build the release image
VERSION=$(basename "$DOCKERDIR")
TAG="fjammes/qserv:$VERSION"
printf "Building latest release image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --tag="$TAG" "$DOCKERDIR"

# Build the development image
DOCKERDIR="$DIR/dev"
VERSION=$(basename "$DOCKERDIR")                                                                                                                                                                                   
TAG="fjammes/qserv:$VERSION"
printf "Building development image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --tag="$TAG" "$DOCKERDIR"

printf "Image %s built successfully\n" "$TAG"
