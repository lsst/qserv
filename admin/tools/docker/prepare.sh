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
	    d) DEPDIR="$OPTARG" ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -lt 1 ] ; then
    usage
    exit 2
fi

if [ -z "$DEPDIR" -a -z "$QSERV_DIR" ] ; then
    printf "ERROR: directory containing dependency scripts not specified" 
    usage
    exit 3
fi
test "$DEPDIR" || DEPDIR=$QSERV_DIR/admin/bootstrap

DOCKERDIR=$1
shift

# strip trailing slash
DOCKERDIR=$(echo $DOCKERDIR | sed 's%\(.*[^/]\)/*%\1%')
DEPDIR=$(echo $DEPDIR | sed 's%\(.*[^/]\)/*%\1%')

cd "$DOCKERDIR"

VERSION=$(basename "$DOCKERDIR")
. ./dep.sh
DEPFILE="$DEPDIR/qserv-install-deps-"$DEPS".sh"

printf "Loading dependencies from %s\n" "$DEPFILE" 

[ -e install-deps.sh ] || ln "$DEPFILE" "install-deps.sh" 
mkdir -p scripts

TESTFILE="mono-node-test.sh"
[ -e "scripts/$TESTFILE" ] || ln "../common/$TESTFILE" "scripts/$TESTFILE"

# Build the image
docker build --tag="fjammes/qserv:$VERSION" .

# Run the integration test
# a hostname is required by xrootd
docker run -i --hostname="qserv-host" -t "fjammes/qserv:$VERSION"
