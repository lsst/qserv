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

# This script is supposed to be sourced from a client script in order
# to set up proper values of the corresponding parameters

basedir=$(dirname "$0")
if [ -z "$basedir" ] || [ "$0" = "bash" ]; then
    >&2 echo "error: variable 'basedir' is not defined"
    return 1 
fi
basedir=$(readlink -e "$basedir")
if [ ! -d "$basedir" ]; then
    >&2 echo "error: path 'basedir' is not a valid directory"
    return 1
fi

function get_param {
    local path="$basedir/$1"
    if [ ! -f "$path" ]; then
        >&2 echo "file not found: $path"
        return 1
    fi
    cat "$path"
}

# Base directory where Qserv is installed on the worker nodes
QSERV_DATA_DIR="/qserv/data"

# Base directory of the replication system on both master and worker nodes
REPLICATION_DATA_DIR="/qserv/replication"

# Base directory where the Replication system's MariaDB/MySQL service
# of the master node will create its folder 'mysql'
DB_DATA_DIR="${REPLICATION_DATA_DIR}"

# Configuration files of the Replication system's processes on both master
# and the worker nodes.   
CONFIG_DIR="${REPLICATION_DATA_DIR}/config"

# Log files of the Replication system's processes on both master
# and the worker nodes.   
LOG_DIR="${REPLICATION_DATA_DIR}/log"

# Configuration file of the Replication system's processes on both master
# and the worker nodes.   
LSST_LOG_CONFIG="${CONFIG_DIR}/log4cxx.replication.properties"

# Work directory for the applications. It can be used by applications
# to store core files, as well as various debug information.
WORK_DIR="${REPLICATION_DATA_DIR}/work"

# Tags for the relevant containers
REPLICATION_IMAGE_TAG="qserv/replica:tools"
DB_IMAGE_TAG="mariadb:10.2.16"

DB_CONTAINER_NAME="qserv-replica-mariadb"
MASTER_CONTAINER_NAME="qserv-replica-master"
WORKER_CONTAINER_NAME="qserv-replica-worker"

WORKERS="$(get_param workers)"
MASTER="$(get_param master)"

DB_PORT=23306
DB_ROOT_PASSWORD="CHANGEME"

CONFIG="mysql://qsreplica@lsst-qserv-${MASTER}:${DB_PORT}/qservReplica"

# Optional parameters of the Master Controller
MASTER_PARAMETERS="--worker-evict-timeout=3600 --health-probe-interval=120 --replication-interval=1200"

unset basedir
unset -f get_param
