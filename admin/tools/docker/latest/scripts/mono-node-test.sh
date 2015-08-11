#!/bin/bash

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

#
# Run mono-node test against Qserv version setup by default
#

# @author  Fabrice Jammes, IN2P3

set -e

. /qserv/stack/loadLSST.bash
setup qserv_distrib
QSERV_RUN_DIR=$HOME/qserv-run/mono-$(qserv-version.sh)
qserv-configure.py --all --force -R $QSERV_RUN_DIR
$QSERV_RUN_DIR/bin/qserv-start.sh
qserv-test-integration.py
$QSERV_RUN_DIR/bin/qserv-stop.sh
