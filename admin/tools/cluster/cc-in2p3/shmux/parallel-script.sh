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


# Run Qserv install and configuration scripts on all cluster nodes
# These script sneeds to be previously installed on all nodes.

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "$DIR"/scripts/params.sh

SCRIPTS_DIR="$INSTALL_BASE"/scripts

SCRIPTS=$(ls -I params.sh -m "$DIR"/scripts/)

usage() {
  cat << EOD

Usage: `basename $0` [options] script

  script is the script to be runned,
  must be in [$SCRIPTS]

  Available options:
    -h                this message

  Run $SCRIPTS_DIR/<script> on all cluster nodes.

EOD
}

while getopts h: c ; do
    case $c in
            h) usage ; exit 0 ;;
            \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 1 ] ; then
    usage
    exit 2
fi

SCRIPT=$1

sh "$DIR"/hosts.sh | /opt/shmux/bin/shmux -c "sudo -u qserv sh $SCRIPTS_DIR/$SCRIPT" -
