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


# Common parameters used by management scripts for docker containers

# @author  Fabrice Jammes, IN2P3/SLAC

################################################################
#
# Edit variables below to specify values for a given cluster
#
################################################################

# Default values below are for CC-IN2P3 50 nodes cluster

# Node to be configured as master node
MASTER='ccqserv100.in2p3.fr'

# Cluster install script will be copied on each nodes in $INSTALL_BASE/scripts
INSTALL_BASE=/qserv

# Local directory where SHARED_DIR will be copied
INSTALL_DIR="$INSTALL_BASE/stack"

# Local directory which will contains Qserv run directory
QSERV_RUN_DIR="$INSTALL_BASE/run"

# Local directory which will contains Qserv data
QSERV_DATA_DIR="$INSTALL_BASE/data"

# Tag related to Qserv version built from sources,
# add it to ~/.eups/startup.py
SETUP_OPT_GIT='-t git'

SETUP_OPT_RELEASE='-t qserv'
