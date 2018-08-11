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

# This script is supposed to be sourced from a client script in order
# to set up proper values of the corresponding parameters

basedir=$(dirname $0)
if [ -z "$basedir" ] || [ "$0" = "bash" ]; then
    (>&2 echo "error: variable 'basedir' is not defined")
    exit 1 
fi
basedir="$(readlink -e $basedir)"
if [ ! -d "$basedir" ]; then
    (>&2 echo "error: path 'basedir' is not a valid directory")
    exit 1
fi

function get_param {
    local path="$basedir/$1"
    if [ ! -f "$path" ]; then
        (>&2 echo "file not found: $path")
        exit 1
    fi
    echo "$(cat $path)"
}

# Base directory where Qserv is installed on the worker nodes
QSERV_DATA_DIR="/qserv/data"

# Base directory of the replication system on both master and worker nodes
REPLICATION_DATA_DIR="/qserv/replication"

# Base directory where the Replication system's MariaDB/MySQL service
# of the master node wil create its folder 'mysql'
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
MASTER_PARAMETERS="--worker-evict-timeout=240"

unset basedir


# Parse command-line options

HELP="
General usage:

    ${0} [OPTIONS]

General options:

    -h|--help
        print this help

Options restricting a scope of the operation:

    -w=<name>|--worker=<name>
        select one worker only

    -m|--master
        master controller

    -d|--db
        database service
"

ALL=1
WORKER=
MASTER_CONTROLLER=
DB_SERVICE=

for i in "$@"; do
    case $i in
    -w=*|--worker=*)
        ALL=
        WORKER="${i#*=}"
        if [ "${WORKER}" == "*" ]; then
            WORKER=$WORKERS
        fi
        shift
        ;;
    -m|--master)
        ALL=
        MASTER_CONTROLLER=1
        shift
        ;;
    -d|--db)
        ALL=
        DB_SERVICE=1
        shift # past argument
        ;;
    -h|--help)
        (>&2 echo "${HELP}")
        exit 2
        ;;
    *)
        (>&2 echo "error: unknown option '${i}'${HELP}")
        ;;
    esac
done
if [ ! -z "${ALL}" ]; then
    MASTER_CONTROLLER=1
    DB_SERVICE=1
else
    WORKERS=$WORKER
fi
unset HELP
unset ALL
unset WORKER
