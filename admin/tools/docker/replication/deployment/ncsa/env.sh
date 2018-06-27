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

DATA_DIR="$(get_param data_dir)"
CONFIG_DIR="$(get_param config_dir)"
LOG_DIR="$(get_param log_dir)"
LSST_LOG_CONFIG="${CONFIG_DIR}/$(get_param lsst_log_config)"
CONFIG="$(get_param config)"
IMAGE_TAG=$(get_param image_tag)
WORKER_CONTAINER_NAME="$(get_param worker_container_name)"
WORKERS="$(get_param workers)"
MASTERS="$(get_param masters)"

unset basedir
