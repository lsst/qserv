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


# Create a Docker image with updated uid/gid for qserv group and user
# in order to attach host directory to Docker image with correct permissions

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DEFAULT_IMAGE="fjammes/qserv:dev"
IMAGE="$DEFAULT_IMAGE"

usage() {
  cat << EOD

Usage: `basename $0` [options] DOCKERDIR

  Available options:
    -h          this message
    -i image    initial Docker image, must have Qserv stack installed
               default to "$DEFAULT_IMAGE"

  Create a Docker image with updated uid/gid for qserv group and user
  in order to attach host directory to Docker image with correct permissions

  Once completed, run an interactive session on this container with:
  docker run -i --hostname="qserv-host" -t "fjammes/qserv:<VERSION>" /bin/bash
EOD
}

# get the options
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

SCRIPT="/home/dev/scripts/change-uid.sh"

_UID=$(id -u)
GID=$(id -g)

TMP_DIR=$(mktemp -d -t docker_qserv_uid.XXXXXX)
CID_FILE="$TMP_DIR"/cid

set -x
docker run --cidfile="$CID_FILE" "$IMAGE" $SCRIPT $_UID $GID
set +x
CONTAINER_ID=$(cat $CID_FILE)
rm $CID_FILE

MSG="Change dev user (UID, GID) to ($UID, $GID)"
DEST_IMAGE="$IMAGE-uid"
docker commit --message="$MSG" --author="Fabrice Jammes" "$CONTAINER_ID" "$DEST_IMAGE"

# workaround: eups declare has to be performed after uid/gid change
# for unknown reason
SCRIPT="/home/dev/scripts/eups-declare.sh"
set -x
docker run --cidfile="$CID_FILE" -u dev "$DEST_IMAGE" sh -c $SCRIPT
set +x
CONTAINER_ID=$(cat $CID_FILE)
rm $CID_FILE

MSG="Declare Qserv source directory to eups"                                                                                                                                                                 
DEST_IMAGE="$IMAGE-uid"                                                                                                                                                                                            
docker commit --message="$MSG" --author="Fabrice Jammes" "$CONTAINER_ID" "$DEST_IMAGE"                                                                                                                             
echo "Image $DEST_IMAGE created successfully"
