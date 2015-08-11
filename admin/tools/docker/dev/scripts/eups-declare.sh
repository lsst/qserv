#!/bin/bash

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


# Declare Qserv development version to eups

# @author  Fabrice Jammes, IN2P3/SLAC

set -e

DEFAULT_SRC_DIR="$HOME/src/qserv"
SRC_DIR="$DEFAULT_SRC_DIR"

usage() {
  cat << EOD

Usage: `basename $0` [options] 

  Available options:
    -h          this message
    -s          path to Qserv source directory,
                default to $DEFAULT_SRC_DIR

  Declare Qserv development version to eups
EOD
}


# get the options
while getopts hs: c ; do
    case $c in
            h) usage ; exit 0 ;;
            s) SRC_DIR="$OPTARG" ;;
            \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

. /qserv/scripts/params.sh

. $INSTALL_DIR/loadLSST.bash

echo "Setup Qserv from source version"
eups declare $SETUP_OPT_GIT -r "$SRC_DIR"

