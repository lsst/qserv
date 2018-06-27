#!/bin/bash

# This file is part of {{ cookiecutter.package_name }}.
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

set -e

# Load parameters of the setup into the corresponding environment
# variables

. $(dirname $0)/env.sh

# Start workers on all nodes

for WORKER in $WORKERS; do
    WORKER_HOST="qserv-${WORKER}"
    echo "${WORKER}:"
    ssh -n $WORKER_HOST docker run \
        --detach \
        --network host \
        -u 1000:1000 \
        -v /etc/passwd:/etc/passwd:ro \
        -v ${DATA_DIR}/mysql:${DATA_DIR}/mysql \
        -v ${CONFIG_DIR}:/qserv/replication/config:ro \
        -v ${LOG_DIR}:${LOG_DIR} \
        -e "WORKER_CONTAINER_NAME=${WORKER_CONTAINER_NAME}" \
        -e "LOG_DIR=${LOG_DIR}" \
        -e "LSST_LOG_CONFIG=${LSST_LOG_CONFIG}" \
        -e "CONFIG=${CONFIG}" \
        -e "WORKER=${WORKER}" \
        --name "${WORKER_CONTAINER_NAME}" \
        $IMAGE_TAG \
        bash -c \''/qserv/bin/qserv-replica-worker ${WORKER} --config=${CONFIG} >& ${LOG_DIR}/${WORKER_CONTAINER_NAME}.log'\'
done
