#!/bin/bash 

# LSST Data Management System
# Copyright 2013-2014 LSST Corporation.
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


# High level script which launch integration tests on a git repos.
# Usefull to find a bug in a commit list, by using "git bisect"
#
# Pre-requisites :
# - a eups stack need be to available and enabled

# @author  Fabrice Jammes, IN2P3

setup -r . &&
eupspkg -er build &&               # build
eupspkg -er install &&            # install to EUPS stack directory
eupspkg -er decl -t current &&               # declare it to EUPS
setup qserv ||
{
    echo "Unable to setup Qserv"
    return 125   
} 

qserv-kill() 
{
    killall mysqld mysql-proxy xrootd java python
}

qserv-prepare()
{
    qserv-kill 
    cd $QSERV_DIR/admin
    scons ||
    {
        echo "Unable to configure Qserv"
        return 125
    }
    qserv-start.sh
    {
        echo "Unable to start Qserv"
        return 125
    }
}

qserv-test01()
{
    qserv-prepare
    qserv-benchmark.py --case=01 -l
}

qserv-test()
{
    qserv-prepare
    qserv-testdata.py
}

qserv-kill
qserv-test01
ERROR_LINE=$(tail -n1 $QSERV_DIR/var/log/qserv-czar.log)
[[ $ERROR_LINE =~ lsst::qserv::control::AsyncQueryManager ]] && exit 1 
exit 0
