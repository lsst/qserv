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

# The wrapper for running controller-side applications on master nodes

set -e

# Load parameters of the setup into the corresponding environment
# variables

. $(dirname "$0")/env.sh

TOOL="$1"
if [ -z "${TOOL}" ]; then
    >&2 echo "usage: <tool> [<parameters>] [<options>] [<flags>]"
    exit 1
fi
shift
PARAMETERS="$@"

# Make sure this command is run on one of the master nodes

if [ "lsst-qserv-${MASTER}" != "$(hostname -s)" ]; then
    >&2 echo "this tool must be run on the master: ${MASTER}"
    exit 1
fi
PID=$$
docker run \
    --rm \
    -it \
    --network host \
    -u ${CONTAINER_UID}:${CONTAINER_GID} \
    -v /etc/passwd:/etc/passwd:ro \
    -v ${CONFIG_DIR}:/qserv/replication/config:ro \
    -v ${WORK_DIR}:/qserv/work \
    -e "TOOL=${TOOL}" \
    -e "PARAMETERS=${PARAMETERS}" \
    -e "LSST_LOG_CONFIG=${LSST_LOG_CONFIG}" \
    -e "CONFIG=${CONFIG}" \
    -e "WORK_DIR=${WORK_DIR}" \
    --name "${TOOL}-${PID}" \
    "${REPLICATION_IMAGE_TAG}" \
    /qserv/bin/${TOOL} "$@" --config=${CONFIG}
