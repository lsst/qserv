#!/bin/sh

# LSST Data Management System
# Copyright 2014-2015 LSST Corporation.
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


# Change qserv user uid/gid on current node
# useful to mount host volumes using docker with correct permissions

# example:
# docker run --privileged=true -e NEWUID=$UID -e NEWGID=$GID -i -t \
#     -v /qserv/data:/qserv/data fjammes/qserv:centos_7

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

usage() {
    cat << EOD

    Usage: `basename $0` [options] uid gid

    Available options:
    -h          this message

    Change uid and gid of qserv user and group
    and set file permissions consistently.

EOD
}


if [ $# -ne 2 ] ; then
    usage
    exit 2
fi

NEWUID=$1
NEWGID=$2

USER=dev
GROUP=dev

OLDUID=$(id -u $USER)
OLDGID=$(id -g $USER)

groupmod -g $NEWGID $GROUP
usermod -u $NEWUID -g $NEWGID $USER
printf "Updating file owner/group to %s,%s in /home/%s\n" ${USER} ${GROUP} ${USER}
chown -R $USER:$GROUP /home/$USER
