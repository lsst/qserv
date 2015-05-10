#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008-2014 LSST Corporation.
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

# Import new logging module
import lsst.log as newlog


def threshold_dbg():
    newlog.setLevel("", newlog.DEBUG)

def threshold_inf():
    newlog.setLevel("", newlog.INFO)

def threshold_wrn():
    newlog.setLevel("", newlog.WARN)

def threshold_err():
    newlog.setLevel("", newlog.ERROR)

def newlog_msg(level, args):
    newlog.log("", level, '%s', ' '.join(map(str, args)), depth=3)

def log_msg(level, args):
    logger(level, ' '.join(map(str, args)))

def dbg(*args):
    newlog_msg(newlog.DEBUG, args)

def inf(*args):
    newlog_msg(newlog.INFO, args)

def wrn(*args):
    newlog_msg(newlog.WARN, args)

def err(*args):
    newlog_msg(newlog.ERROR, args)
