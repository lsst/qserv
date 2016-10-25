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


# Stop Qserv services
# returns:
#   * if all Qserv services are down:   0
#   * if all Qserv services are up: 127
#   * else the number of non-stopped Qserv services

# @author  Fabrice JAMMES, IN2P3


QSERV_RUN_DIR={{QSERV_RUN_DIR}}
. ${QSERV_RUN_DIR}/bin/env.sh

check_qserv_run_dir

services_rev=`echo ${SERVICES} | tr ' ' '\n' | tac`
service_nb=0
service_failed_nb=0
for service in $services_rev; do
    service_nb=$((service_nb+1))
    ${QSERV_RUN_DIR}/etc/init.d/$service stop || service_failed_nb=$((service_failed_nb+1))
done

if [ $service_failed_nb -eq $service_nb ]; then
    exit 127
else
    exit $service_failed_nb
fi
