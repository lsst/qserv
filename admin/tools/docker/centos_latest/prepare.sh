#!/bin/sh

# LSST Data Management System
# Copyright 2014 LSST Corporation.
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


# Create a Qserv docker image based on CentOS
# and run a mono-node integration test against it.
# Useful to verify Qserv system-dependency list,
# indeed Linux distribution images for Docker are very minimalist

# @author  Fabrice Jammes, IN2P3/SLAC


set -e
set -x

ln ../../../bootstrap/qserv-install-deps-sl7.sh install-deps.sh
docker build --tag="fjammes/qserv:centos_latest" .
docker run -i --hostname="qserv-host" -t "fjammes/qserv:centos_latest"

# Run an interactive session :
# docker run -i --hostname="qserv-host" -t "fjammes/qserv:centos_latest" /bin/bash
