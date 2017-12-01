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


# Start Qserv services
# returns:
#   * if all Qserv services are up:   0
#   * if all Qserv services are down: 127
#   * else the number of non-started Qserv services

# @author  Fabrice JAMMES, IN2P3

QSERV_RUN_DIR=/qserv/run
. ${QSERV_RUN_DIR}/bin/env.sh

check_qserv_run_dir

service_nb=0
service_failed_nb=0
for service in ${SERVICES}; do
    service_nb=$((service_nb+1))
    ${QSERV_RUN_DIR}/etc/init.d/$service start || service_failed_nb=$((service_failed_nb+1))
done

if [ $service_failed_nb -eq $service_nb ]; then
    exit 127
else
    exit $service_failed_nb
fi
