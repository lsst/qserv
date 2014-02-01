#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

# logger.py : A module with a logging interface that utilizes SWIG
# enabled Logger class.

# Package imports
from lsst.qserv.master import logger_threshold
from lsst.qserv.master import logger

def threshold_dbg():
    logger_threshold(0)

def threshold_inf():
    logger_threshold(1)

def threshold_wrn():
    logger_threshold(2)

def threshold_err():
    logger_threshold(3)

def dbg(*args):
    logger(0, ' '.join(map(str, args)))

def inf(*args):
    logger(1, ' '.join(map(str, args)))

def wrn(*args):
    logger(2, ' '.join(map(str, args)))

def err(*args):
    logger(3, ' '.join(map(str, args)))
