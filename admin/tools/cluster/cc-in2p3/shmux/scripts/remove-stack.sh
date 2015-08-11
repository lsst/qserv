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


# Remove Qserv stack on current node 

# @author  Fabrice Jammes, IN2P3/SLAC


set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/params.sh

INSTALL_DIR=$(check_path $INSTALL_DIR)

if [ -e "$INSTALL_DIR" ]; then
    chmod -R 777 "$INSTALL_DIR"
    rm -rf "$INSTALL_DIR"
else
    echo "$INSTALL_DIR doesn't exist"
fi
