#!/bin/sh

# LSST Data Management System
# Copyright 2015 LSST Corporation.
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

#
# Dependencies for RedHat7-based distributions
# Tested on SL7
#

# @author  Fabrice Jammes, IN2P3

# eups
yum install --assumeyes patch bzip2 bzip2-devel

# kazoo
yum install --assumeyes python-setuptools

# lua
yum install --assumeyes readline-devel

# numpy
yum install --assumeyes numpy

# mysql
yum install --assumeyes ncurses-devel glibc-devel

# mysql-proxy
yum install --assumeyes glib2-devel

# newinstall.sh
yum install --assumeyes bash git tar make

# qserv
yum install --assumeyes openssl-devel java redhat-lsb initscripts

# sconsUtils
yum install --assumeyes gettext flex bison

# xrootd
yum install --assumeyes gcc gcc-c++ zlib-devel cmake

# zope_interface
yum install --assumeyes python-devel
