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
REMOTE_HOST=lsst-dev
#VERSION=$(pkgautoversion)
VERSION=`${DIR}/../admin/bin/qserv-version.sh`
echo "Uploading documentation from $PWD to $REMOTE_HOST"
rsync -ave ssh  doc/build/html/* ${REMOTE_HOST}:public_html/qserv-doc/${VERSION}
ssh ${REMOTE_HOST} "ln -sf ${VERSION}/toplevel.html public_html/qserv-doc/index.html; ln -sf ${VERSION}/_static public_html/qserv-doc/_static"
# alternate solution :
#lftp -u datasky,xxx sftp://datasky.in2p3.fr -e "mirror -e -R  $HOME/src/qserv/doc/build/html htdocs/qserv-doc/2014_09.0 ; quit"
cd -
