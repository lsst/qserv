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

# Load common parameters of the setup into the corresponding environment
# variables

. $(dirname $0)/env.sh

# Parse command-line options

HELP="
General usage:

    ${0} [OPTIONS]

General options:

    -h|--help
        print this help

    -j|--jemalloc
        run an application with the JEMALLOC library and collect statistics
        into a file to be placed in the debug folder.

Options restricting a scope of the operation:

    -w=<name>|--worker=<name>
        select one worker only

    -m|--master
        master controller

    -d|--db
        database service
"

USE_JEMALLOC=

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
    -j|--jemalloc)
        USE_JEMALLOC=1
        shift
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
