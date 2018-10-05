#!/bin/bash

# This file is part of qserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# Stop worker containers on all nodes

set -e

# Load parameters of the setup into the corresponding environment
# variables

. $(dirname $0)/env_svc.sh

if [ -n "${MASTER_CONTROLLER}" ]; then
    HOST="qserv-${MASTER}"
    ssh -n $HOST 'echo "["'$MASTER'"] master controller: "$(docker stop '$MASTER_CONTAINER_NAME')" "$(docker rm '$MASTER_CONTAINER_NAME')'
fi

for WORKER in $WORKERS; do
    HOST="qserv-${WORKER}"
    ssh -n $HOST 'echo "["'$WORKER'"] worker agent: "$(docker stop '$WORKER_CONTAINER_NAME')" "$(docker rm '$WORKER_CONTAINER_NAME')'
done

if [ -n "${DB_SERVICE}" ]; then
    HOST="qserv-${MASTER}"
    ssh -n $HOST 'echo "["'$MASTER'"] database service: "$(docker stop '$DB_CONTAINER_NAME')" "$(docker rm '$DB_CONTAINER_NAME')'
fi
