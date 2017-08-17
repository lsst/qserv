#
# LSST Data Management System
# Copyright 2015 AURA/LSST.
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

"""
Module defining dbMgr class and related methods.

@author Andy Salnikov - salnikov@slac.stanford.edu
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------

# -----------------------------
# Imports for other modules --
# -----------------------------
from flask import json
from werkzeug.exceptions import HTTPException

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

# ------------------------
# Exported definitions --
# ------------------------


def errorResponse(code, errorType, message, cause=None):
    """
    Make JSON error response, structure follows our guidelines document.
    """
    excData = dict(exception=errorType, message=message)
    if cause:
        excData['cause'] = cause
    response = json.jsonify(excData)
    response.status_code = code
    return response


class ExceptionResponse(HTTPException):
    """
    Exception class that will be converted to error response.
    """

    def __init__(self, code, errorType, message, cause=None):
        response = errorResponse(code, errorType, message, cause)
        HTTPException.__init__(self, response=response)
