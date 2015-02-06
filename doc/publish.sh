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
DIR=$(cd "$(dirname "$0")"; pwd -P)
cd $DIR/..
(
echo "Generating documentation"
scons doc
)
REMOTE_HOST=lsst-db2.slac.stanford.edu
DOC_ROOT_PATH=/afs/slac/www/exp/lsst/qserv
VERSION=`${DIR}/../admin/bin/qserv-version.sh`
echo "Uploading documentation from $PWD to $REMOTE_HOST"
rsync -ave ssh  doc/build/html/* ${REMOTE_HOST}:${DOC_ROOT_PATH}/${VERSION}
ssh ${REMOTE_HOST} "ln -sf ${DOC_ROOT_PATH}/${VERSION}/toplevel.html ${DOC_ROOT_PATH}/index.html; ln -sf ${DOC_ROOT_PATH}/${VERSION}/_static ${DOC_ROOT_PATH}/_static"
