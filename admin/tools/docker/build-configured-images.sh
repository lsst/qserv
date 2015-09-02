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

DEFAULT_IMAGE="fjammes/qserv:latest"
IMAGE="$DEFAULT_IMAGE"

usage() {
  cat << EOD

Usage: `basename $0` [options] DOCKERDIR

  Available options:
    -h          this message
    -i image    initial Docker image, must have Qserv stack installed
                default to "$DEFAULT_IMAGE"

  Create docker images containing Qserv master and worker instances,
  use a Docker image containing Qserv stack as input.

  Once completed, run an interactive session on this container with:
  docker run -i --hostname="qserv-host" -t "fjammes/qserv:<VERSION>" /bin/bash
EOD
}

# Get the options
while getopts hi: c ; do
    case $c in
            h) usage ; exit 0 ;;
            i) IMAGE="$OPTARG" ;;
            \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

SCRIPT="/qserv/scripts/configure.sh"

# Create Qserv worker image
TMP_DIR=$(mktemp -d -t docker_qserv_uid.XXXXXX)
CID_FILE="$TMP_DIR"/cid

set -x
docker run --cidfile="$CID_FILE" -u qserv "$IMAGE" $SCRIPT
set +x

CONTAINER_ID=$(<$CID_FILE)
rm $CID_FILE

MSG="Create Qserv worker image"
DEST_IMAGE="$IMAGE-worker"
docker commit --message="$MSG" --author="Fabrice Jammes" "$CONTAINER_ID" "$DEST_IMAGE"
echo "Image $DEST_IMAGE created successfully"

# Create Qserv master image
docker run --cidfile="$CID_FILE" -u qserv "$IMAGE" $SCRIPT -m

CONTAINER_ID=$(cat $CID_FILE)
rm $CID_FILE

MSG="Create Qserv master image"
DEST_IMAGE="$IMAGE-master"
docker commit --message="$MSG" --author="Fabrice Jammes" "$CONTAINER_ID" "$DEST_IMAGE"
echo "Image $DEST_IMAGE created successfully"
