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


# Docker utility
# Start Qserv on current node
# and doesn't exit

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

# Make timezone adjustments (if requested)
if [ "$SET_CONTAINER_TIMEZONE" = "true" ]; then

    # These files have to be write-enabled for the current user ('qserv')

    echo ${CONTAINER_TIMEZONE} >/etc/timezone && \
    cp /usr/share/zoneinfo/${CONTAINER_TIMEZONE} /etc/localtime

    # To make things fully complete we would also need to run
    # this command. Unfortunatelly the security model of the container
    # won't allow that because the current script is being executed
    # under a non-privileged user 'qserv'. Hence disabling this for now.
    #
    # dpkg-reconfigure -f noninteractive tzdata
    
    echo "Container timezone set to: $CONTAINER_TIMEZONE"
else
    echo "Container timezone not modified"
fi

QSERV_RUN_DIR=/qserv/run
QSERV_CUSTOM_DIR=/qserv/custom

# Check $QSERV_MASTER was provided at container execution and use it
# to update container configuration
if [ -n "$QSERV_MASTER" ]
then
    cp "$QSERV_RUN_DIR/qserv-meta.conf" /tmp/qserv-meta.conf.orig
    awk -v MASTER="${QSERV_MASTER}" \
        '{gsub(/<DOCKER_ENV_QSERV_MASTER>/, MASTER);
          print}' /tmp/qserv-meta.conf.orig > "$QSERV_RUN_DIR/qserv-meta.conf"
    bash -c ". /qserv/stack/loadLSST.bash && setup qserv -t qserv-dev && \
             qserv-configure.py --qserv-run-dir '$QSERV_RUN_DIR' \
                                --qserv-custom-dir '$QSERV_CUSTOM_DIR' --etc"
else
    echo "ERROR: \$QSERV_MASTER is unset"
    exit 1
fi

# TODO: Improve secret management (see DM-11127)
# for example, pass password via k8s env variable, see
# admin/python/lsst/qserv/admin/configure.py (grep SECRET) for building it.
echo "USER:CHANGEME" > $QSERV_RUN_DIR/etc/wmgr.secret

"$QSERV_RUN_DIR"/bin/qserv-start.sh && tail -f /dev/null
