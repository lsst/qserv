#!/bin/sh

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


# Common parameters used by cluster management scripts

# @author  Fabrice Jammes, IN2P3/SLAC

################################################################
#
# Edit variables below to specify values for a given cluster
#
################################################################

# Default values below are for CC-IN2P3 50 nodes cluster

# Parameter used by management/build machine
#
############################################

# Cluster install script will be copied on each nodes in $INSTALL_BASE/scripts
INSTALL_BASE=/qserv

# Parameters for each cluster nodes
#
###################################

# Node to be configured as master node
MASTER='ccqserv100.in2p3.fr'

# Shared directory containing Qserv stack binaries
SHARED_DIR=/sps/lsst/Qserv/stack/

# Local directory where SHARED_DIR will be copied
INSTALL_DIR="$INSTALL_BASE"/stack

# Local directory which will contains Qserv run directory
QSERV_RUN_DIR="$INSTALL_BASE"/run

# Local directory which will contains Qserv data
QSERV_DATA_DIR="$INSTALL_BASE"/data

# eups options

# Tag related to Qserv version built from sources,
# has to be added to stack as a global eups tag available on the cluster
# Declare this tag tag in $EUPS_PATH/ups_db/global.tags, if not exists:
#
# grep -q -F 'latestbuild' $EUPS_PATH/ups_db/global.tags || echo 'latestbuild' >> $EUPS_PATH/ups_db/global.tags
#
# and then rsync stack to all nodes.
SETUP_OPT_GIT='-t latestbuild'

SETUP_OPT_RELEASE='-t qserv'

function check_path {

    case "$1" in
        /*) ;;
        *) echo "expect absolute path for $1" ; exit 2 ;;
    esac

    # strip trailing slash
    echo $1 | sed 's%\(.*[^/]\)/*%\1%'
}

