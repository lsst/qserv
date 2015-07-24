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


# Create a Qserv docker image and run a mono-node integration test against it.
# Useful to verify Qserv system-dependency list,
# indeed Linux distribution images for Docker are very minimalist

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

usage() {
  cat << EOD

Usage: `basename $0` [options] DOCKERDIR

  Available options:
    -h          this message
    -d path     directory containing dependency scripts, default to
                \$QSERV_DIR/admin/bootstrap

  Create a docker image using DOCKERDIR/Dockerfile
  copy common ressources from DOCKERDIR/../commons, the image will
  have the same name than the parent directory of the Dockerfile,
  i.e. <VERSION>.

   Once completed, run an interactive session on this container with:
   docker run -i --hostname="qserv-host" -t "fjammes/qserv:<VERSION>" /bin/bash
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

if [ $# -lt 1 ] ; then
    usage
    exit 2
fi

if [ -z "$DEPS_DIR" -a -z "$QSERV_DIR" ] ; then
    printf "ERROR: directory containing dependency scripts not specified"
    usage
    exit 3
fi
test "$DEPS_DIR" || DEPS_DIR=$QSERV_DIR/admin/bootstrap

DOCKERDIR=$1
shift

# strip trailing slash
DOCKERDIR=$(echo $DOCKERDIR | sed 's%\(.*[^/]\)/*%\1%')
DEPS_DIR=$(echo $DEPS_DIR | sed 's%\(.*[^/]\)/*%\1%')

# WARN:
# Scripts used by Dockerfile:
# - must be located under the same root path than the Dockerfile,
# - must not be symlinks
# - git can't store physical links
# So, copy it from templates to SCRIPT_DIR directory
SCRIPT_DIR="$DOCKERDIR/scripts"

. "$SCRIPT_DIR/dist.sh"
TPL_DEPS_SCRIPT="$DEPS_DIR/qserv-install-deps-"$DIST".sh"

printf "Add physical link to dependencies install script: %s\n" "$TPL_DEPS_SCRIPT"
ln -f "$TPL_DEPS_SCRIPT" "$SCRIPT_DIR/install-deps.sh"

INSTALL_SCRIPT="install-stack.sh"
TPL_INSTALL_SCRIPT="$DOCKERDIR/../common/$INSTALL_SCRIPT"
ln -f "$TPL_INSTALL_SCRIPT" "$SCRIPT_DIR/$INSTALL_SCRIPT"

TEST_SCRIPT="mono-node-test.sh"
TPL_TEST_SCRIPT="$DOCKERDIR/../common/$TEST_SCRIPT"
ln -f "$TPL_TEST_SCRIPT" "$SCRIPT_DIR/$TEST_SCRIPT"

# Build the image
VERSION=$(basename "$DOCKERDIR")
TAG="fjammes/qserv-sysdeps:$VERSION"
printf "\n-- Building image %s from %s\n" "$TAG" "$DOCKERDIR"
docker build --tag="$TAG" "$DOCKERDIR"

# Install eups stack
# Performed outside of Dockerfile to ease debugging,
# furthermore, it need to be re-executed for each monthly release,
# (and Dockerfile doesn't do this)
MSG="Install eups stack for Qserv"
printf "\n-- $MSG\n"
printf "   Using image tagged: %s\n" "$TAG"
CID_FILE="$DOCKERDIR/docker.cid"
# Create image even if install failed to ease diagnose
docker run -it --cidfile="$CID_FILE" "$TAG" /bin/su qserv -c "/bin/sh $INSTALL_SCRIPT || echo 'INSTALL FAILED' > ERROR_LOG"

CONTAINER_ID=$(cat $CID_FILE)
rm $CID_FILE
TAG="fjammes/qserv:$VERSION"
docker commit --message="$MSG" --author="Fabrice Jammes" "$CONTAINER_ID" "$TAG"

# Run the integration test
# a hostname is required by xrootd
printf "\n-- Running mono-node integration test\n"
docker run -it  --hostname="qserv-host" -t "$TAG" /bin/su qserv -c "/bin/sh $TEST_SCRIPT"
