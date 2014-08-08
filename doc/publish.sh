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


# Upload sphinx documentation to LSST web-server.
# This is a temporary solution which should be replaced by LSST
# standard procedure for publishing documention. 

# @author  Fabrice Jammes, IN2P3

# eval `ssh-agent -s`
# ssh-add ~/.ssh/id_rsa_lsst

REMOTE_HOST=lsst-dev.ncsa.illinois.edu
#VERSION=$(pkgautoversion)
VERSION=$(qserv-version.sh)
rsync -ave ssh  doc/build/html/* ${REMOTE_HOST}:public_html/qserv-doc/${VERSION}
ssh ${REMOTE_HOST} "ln -sf ${VERSION}/toplevel.html public_html/qserv-doc/index.html; ln -sf ${VERSION}/_static public_html/qserv-doc/_static"
