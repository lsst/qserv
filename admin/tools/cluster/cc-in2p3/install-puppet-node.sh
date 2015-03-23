#!/bin/bash

# LSST Data Management System
# Copyright 2014-2015 LSST Corporation.
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


# Replicate directories to a number of servers. This is a temporary
# solution for data replication on small-scale cluster (or mosly
# for testing simple multi-node setup). This will likely be replaced
# with some cluster management-based solution as we prepare for
# actual deployment.

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

################################################################
#
# Edit variables below to specify defaults for a given cluster
#
################################################################

# Default values below are for CC-IN2P3 50 nodes cluster

# override with -M option
MASTER='ccqserv100.in2p3.fr'

# override with -s option
SHARED_DIR=/sps/lsst/Qserv/stack/

# override with -i option
INSTALL_DIR=/qserv/stack

# override with -R option
QSERV_RUN_DIR=/qserv/qserv-run

SETUP_OPTS='-t latestbuild -t qserv'

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h                this message
    -w                set up worker node, if not specified install master node
    -M hostname       DNS name of the master node for this instance,
                      default: $MASTER
    -s shared_dir     full path to stack directory which will be duplicated on
                      all nodes, default: ${SHARED_DIR}
    -i install_dir    full path to install directory, default: ${INSTALL_DIR}
    -R qserv_run_dir  full path to install directory, default: ${QSERV_RUN_DIR}
    -N                do not synchronize install with shared-dir

  Copies a LSST stack from a shared directory to a local directory, load LSST
  environment, setup qserv_distrib, and configure and start Qserv. Options
  default values can be changed by editing this script.

EOD
}

# Do not edit these default values
NODE_TYPE='master'
SYNC=true

while getopts hwM:s:i:R:N c ; do
    case $c in
            h) usage ; exit 0 ;;
            w) NODE_TYPE='worker' ;;
            M) MASTER="$OPTARG" ;;
            s) SHARED_DIR="$OPTARG" ;;
            i) INSTALL_DIR="$OPTARG" ;;
            R) QSERV_RUN_DIR="$OPTARG" ;;
            N) SYNC=false ;;
            \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

function check_path {

    case "$1" in
        /*) ;;
        *) echo "expect absolute path for $1" ; exit 2 ;;
    esac

    # strip trailing slash
    echo $1 | sed 's%\(.*[^/]\)/*%\1%'
}

SHARED_DIR=$(check_path $SHARED_DIR)
INSTALL_DIR=$(check_path $INSTALL_DIR)
QSERV_RUN_DIR=$(check_path $QSERV_RUN_DIR)

if $SYNC; then
    mkdir -p $INSTALL_DIR
    echo "Synchronize $INSTALL_DIR with $SHARED_DIR"
    rsync -avz $SHARED_DIR/ $INSTALL_DIR
fi

echo "Setup qserv_distrib in eups with options: $SETUP_OPTS"
. $INSTALL_DIR/loadLSST.bash
setup qserv_distrib $SETUP_OPTS

echo "Configure Qserv $NODE_TYPE"
qserv-configure.py --qserv-run-dir $QSERV_RUN_DIR --force \
    --prepare

# Customize meta configuration file
sed -i "s/node_type = mono/node_type = $NODE_TYPE/" \
    $QSERV_RUN_DIR/qserv-meta.conf
sed -i "s/master = 127.0.0.1/master = $MASTER/" \
    $QSERV_RUN_DIR/qserv-meta.conf

qserv-configure.py --qserv-run-dir $QSERV_RUN_DIR --force

printf "Start Qserv $NODE_TYPE using: %s\n" $QSERV_RUN_DIR/bin/qserv-start.sh
