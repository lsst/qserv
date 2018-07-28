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

# Start database services on the master node and ensure the they're running
# before starting workers. Otherwise workers would fail.

HOST="qserv-${MASTER}"
echo "${MASTER}: staring MariaDB service"
ssh -n $HOST docker run \
    --detach \
    --network host \
    --name "${DB_CONTAINER_NAME}" \
    -u 1000:1000 \
    -v /etc/passwd:/etc/passwd:ro \
    -v "${DATA_DIR}/mysql:${DATA_DIR}/mysql" \
    -v "${LOG_DIR}:${LOG_DIR}" \
    -e "MYSQL_ROOT_PASSWORD=${DB_ROOT_PASSWORD}" \
    "${DB_IMAGE_TAG}" \
    --port="${DB_PORT}" \
    --general-log-file="${LOG_DIR}/${DB_CONTAINER_NAME}.general.log" \
    --log-error="${LOG_DIR}/${DB_CONTAINER_NAME}.error.log" \
    --log-slow-query-log-file="${LOG_DIR}/${DB_CONTAINER_NAME}.slow-query.log" \
    --pid-file="${LOG_DIR}/${DB_CONTAINER_NAME}.pid"

# Wait before the database container started
echo "${MASTER}: waiting for the service to start"
ssh -n $HOST 'while true; do sleep 1; if [ -f "'${LOG_DIR}/${DB_CONTAINER_NAME}.pid'"]; then break; fi; done'

exit 1

# Start workers on all nodes

for WORKER in $WORKERS; do
    HOST="qserv-${WORKER}"
    echo "${WORKER}: staring worker agent"
    ssh -n $HOST docker run \
        --detach \
        --network host \
        --name "${WORKER_CONTAINER_NAME}" \
        -u 1000:1000 \
        -v /etc/passwd:/etc/passwd:ro \
        -v "${DATA_DIR}/mysql:${DATA_DIR}/mysql" \
        -v "${CONFIG_DIR}:/qserv/replication/config:ro" \
        -v "${LOG_DIR}:${LOG_DIR}" \
        -e "WORKER_CONTAINER_NAME=${WORKER_CONTAINER_NAME}" \
        -e "LOG_DIR=${LOG_DIR}" \
        -e "LSST_LOG_CONFIG=${LSST_LOG_CONFIG}" \
        -e "CONFIG=${CONFIG}" \
        -e "WORKER=${WORKER}" \
        "${IMAGE_TAG}" \
        bash -c \''/qserv/bin/qserv-replica-worker ${WORKER} --config=${CONFIG} >& ${LOG_DIR}/${WORKER_CONTAINER_NAME}.log'\'
done
