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


# Configure Qserv on current node

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

usage() {
  cat << EOD

Usage: $(basename "$0") [options]

  Available options:
    -h          this message
    -m          configure Qserv master, instead of worker by default

  Configure a Qserv worker/master in a docker image,
  except Qserv master hostname parameter, set later at container execution.
EOD
}

NODE_TYPE="worker"

# get the options
while getopts hm c ; do
    case $c in
            h) usage ; exit 0 ;;
            m) NODE_TYPE="master" ;;
            \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

QSERV_RUN_DIR=/qserv/run
QSERV_CUSTOM_DIR=/qserv/custom
# QSERV_MASTER is set using k8s yaml configuration

. /qserv/stack/loadLSST.bash
setup qserv_distrib -t qserv-dev

# TODO: check if it is always empty as it should?
rm -rf "$QSERV_RUN_DIR/*"

echo "Configure Qserv $NODE_TYPE"
qserv-configure.py --init --force \
                   --qserv-run-dir "$QSERV_RUN_DIR" \
                   --qserv-data-dir "$QSERV_DATA_DIR"

# Customize meta configuration file
cp "$QSERV_RUN_DIR/qserv-meta.conf" /tmp/qserv-meta.conf.orig
awk \
-v NODE_TYPE_KV="node_type = $NODE_TYPE" \
-v MASTER_KV="master = $QSERV_MASTER" \
'{gsub(/node_type = mono/, NODE_TYPE_KV);
  gsub(/master = 127.0.0.1/, MASTER_KV);
  print}' /tmp/qserv-meta.conf.orig > "$QSERV_RUN_DIR/qserv-meta.conf"

echo "Configure Qserv $NODE_TYPE (master hostname: $QSERV_MASTER)"
qserv-configure.py --qserv-run-dir "$QSERV_RUN_DIR" --force

# Scisql plugin must be installed in MariaDB container
# TODO: improve mariadb container configuration (see DM-11126)
cp "$MARIADB_DIR"/lib/plugin/libscisql-scisql_*.so "$QSERV_RUN_DIR"

mkdir "$QSERV_CUSTOM_DIR"
