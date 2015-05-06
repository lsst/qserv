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


# Replicate Qserv install/configuration scripts to a number of servers.

# @author  Fabrice Jammes, IN2P3/SLAC

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR"/scripts/params.sh

SRC_LOC="$USER@ccqservbuild.in2p3.fr:$DIR/scripts"

# each server rsync scripts using current AFS user account in order to use krb token for ssh connection
sh "$DIR"/hosts.sh | /opt/shmux/bin/shmux -c "sh -c \"rsync --delete -avz -e ssh $SRC_LOC /tmp\"" -

# on each server, copy scripts to qserv account workspace
sh "$DIR"/hosts.sh | /opt/shmux/bin/shmux -c \
    "sudo -u qserv sh -c \"rsync --delete -av /tmp/scripts $INSTALL_BASE\"" -
