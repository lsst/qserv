#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014 LSST Corporation.
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

"""
Exception for QservAdmin.

@author  Jacek Becla, SLAC

"""

from lsst.db.exception import produceExceptionClass

QservAdminException = produceExceptionClass('QservAdminException', [
    (3001, "AUTH_PROBLEM",      "Can't access the config file."),
    (3005, "BAD_CMD",           "Bad command, see HELP for details."),
    (3010, "CONFIG_NOT_FOUND",  "Config file not found."),
    (3015, "DB_EXISTS",         "Database already exists."),
    (3020, "DB_DOES_NOT_EXIST", "Database does not exist."),
    (3022, "DB_NAME_IS_SELF",   "Source and destination names are the same."),
    (3025, "MISSING_PARAM",     "Missing parameter."),
    (3030, "TB_EXISTS",         "Table already exists."),
    (3035, "TB_DOES_NOT_EXIST", "Table does not exist."),
    (3040, "WRONG_PARAM",       "Unrecognized parameter."),
    (3045, "WRONG_PARAM_VAL",   "Unrecognized value for parameter."),
    (9998, "NOT_IMPLEMENTED",   "Feature not implemented yet."),
    (9999, "INTERNAL",          "Internal error.")])
