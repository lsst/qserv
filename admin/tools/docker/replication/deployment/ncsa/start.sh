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

# Start services on all nodes

set -e

# Load parameters of the setup into the corresponding environment
# variables

. $(dirname "$0")/env_svc.sh

# Start database services on the master node
#
# The general log is temporarily disabled because it results in a huge size
# of the log file.
#
#   --general-log --general-log-file="${DB_DATA_DIR}/log/${DB_CONTAINER_NAME}.general.log" \

if [ -n "${DB_SERVICE}" ]; then
    HOST="qserv-${MASTER}"
    echo "[${MASTER}] starting database service"
    ssh -n $HOST docker run \
        --detach \
        --name "${DB_CONTAINER_NAME}" \
        -u 1000:1000 \
        -v /etc/passwd:/etc/passwd:ro \
        -v "${DB_DATA_DIR}/mysql:/var/lib/mysql" \
        -v "${DB_DATA_DIR}/log:${DB_DATA_DIR}/log" \
        -e "MYSQL_ROOT_PASSWORD=${DB_ROOT_PASSWORD}" \
        -p "${DB_PORT}:${DB_PORT}/tcp" \
        "${DB_IMAGE_TAG}" \
        --port="${DB_PORT}" \
        --max-connections=4096 \
        --query-cache-size=0 \
        --log-error="${DB_DATA_DIR}/log/${DB_CONTAINER_NAME}.error.log" \
        --slow-query-log --slow-query-log-file="${DB_DATA_DIR}/log/${DB_CONTAINER_NAME}.slow-query.log" \
        --log-warnings=2 \
        --pid-file="${DB_DATA_DIR}/log/${DB_CONTAINER_NAME}.pid"
    if [ $? -ne 0 ]; then
        echo "failed to start the database container"
        exit 1
    fi
fi

# Start workers on all nodes

for WORKER in $WORKERS; do
    HOST="qserv-${WORKER}"
    echo "[${WORKER}] starting worker agent"
    ssh -n $HOST docker run \
        --detach \
        --network host \
        --name "${WORKER_CONTAINER_NAME}" \
        -u 1000:1000 \
        -v /etc/passwd:/etc/passwd:ro \
        -v "${QSERV_DATA_DIR}/mysql:${QSERV_DATA_DIR}/mysql" \
        -v "${CONFIG_DIR}:/qserv/replication/config:ro" \
        -v "${LOG_DIR}:${LOG_DIR}" \
        -e "WORKER_CONTAINER_NAME=${WORKER_CONTAINER_NAME}" \
        -e "LOG_DIR=${LOG_DIR}" \
        -e "LSST_LOG_CONFIG=${LSST_LOG_CONFIG}" \
        -e "CONFIG=${CONFIG}" \
        -e "WORKER=${WORKER}" \
        "${REPLICATION_IMAGE_TAG}" \
        bash -c \''/qserv/bin/qserv-replica-worker ${WORKER} --config=${CONFIG} >& ${LOG_DIR}/${WORKER_CONTAINER_NAME}.log'\'
done

# Start master controller

if [ -n "${MASTER_CONTROLLER}" ]; then
    HOST="qserv-${MASTER}"
    echo "[${MASTER}] starting master controller"
    OPT_MALLOC_CONF=
    OPT_LD_PRELOAD=
    if [ ! -z "${USE_JEMALLOC}" ]; then
        OPT_MALLOC_CONF=prof_leak:true,lg_prof_interval:31,lg_prof_sample:22,prof_final:true
        OPT_LD_PRELOAD=/qserv/lib/libjemalloc.so
    fi
    ssh -n $HOST docker run \
        --detach \
        --network host \
        -u 1000:1000 \
        -v /etc/passwd:/etc/passwd:ro \
        -v ${WORK_DIR}:${WORK_DIR} \
        -v ${CONFIG_DIR}:/qserv/replication/config:ro \
        -v ${LOG_DIR}:${LOG_DIR} \
        -e "TOOL=qserv-replica-master" \
        -e "PARAMETERS=${MASTER_PARAMETERS}" \
        -e "WORK_DIR=${WORK_DIR}" \
        -e "LOG_DIR=${LOG_DIR}" \
        -e "LSST_LOG_CONFIG=${LSST_LOG_CONFIG}" \
        -e "CONFIG=${CONFIG}" \
        -e "OPT_MALLOC_CONF=${OPT_MALLOC_CONF}" \
        -e "OPT_LD_PRELOAD=${OPT_LD_PRELOAD}" \
        --name "${MASTER_CONTAINER_NAME}" \
        "${REPLICATION_IMAGE_TAG}" \
        bash -c \''cd ${WORK_DIR}; MALLOC_CONF=${OPT_MALLOC_CONF} LD_PRELOAD=${OPT_LD_PRELOAD} /qserv/bin/${TOOL} ${PARAMETERS} --config=${CONFIG} >& ${LOG_DIR}/${TOOL}.log'\'
fi
