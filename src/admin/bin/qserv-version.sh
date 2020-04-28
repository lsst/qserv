#!/bin/bash

# LSST Data Management System
# Copyright 2013-2015 LSST Corporation.
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

# Return Qserv version or release number

# @author  Fabrice Jammes, IN2P3
QSERV_RELEASE="2016_07"

set -e

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -R          print release number

  Print Qserv (eups-based) version number,
  or Qserv release on which this version is based

EOD
}

PRINT_RELEASE=''

# get the options
while getopts hR c ; do
    case $c in
		h) usage ; exit 0 ;;
		R) PRINT_RELEASE="TRUE" ;;
		\?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

if [ -n "$PRINT_RELEASE" ] ; then
    echo "${QSERV_RELEASE}"
else
	QSERV_VERSION=$(eups list qserv -s -V)
	if [ -z "$QSERV_VERSION" ] ; then
        echo "ERROR: Unable to detect eups-based Qserv version"
        exit 1
    fi
    echo "${QSERV_VERSION}"
fi
