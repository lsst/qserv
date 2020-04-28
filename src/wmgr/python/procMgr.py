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
Module defining Flask blueprint for process management.

@author Andy Salnikov - salnikov@slac.stanford.edu
"""

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import logging
import os
import subprocess

# -----------------------------
# Imports for other modules --
# -----------------------------
from .config import Config
from .errors import ExceptionResponse
from flask import Blueprint, json, request, url_for

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

_log = logging.getLogger('procMgr')

# static list of service names for now
_svcNames = ['xrootd', 'mysqld']


def _svcDict(svc, running=None):
    """ Make service instance dict out of service name """
    svcDict = dict(name=svc, uri=url_for('.getServiceState', service=svc))
    if running is not None:
        svcDict['state'] = 'active' if running else 'stopped'
    return svcDict


def _runCmd(cmd, noexcept=True):
    """ Run command in a shell """

    try:
        _log.debug('executing command: %s', cmd)
        subprocess.check_call(cmd, close_fds=True)
        return 0
    except subprocess.CalledProcessError as exc:
        _log.debug('subprocess exception: %s', exc)
        if noexcept:
            return exc.returncode
        else:
            raise

# ------------------------
# Exported definitions --
# ------------------------


procService = Blueprint('procService', __name__, template_folder='procService')


@procService.route('', methods=['GET'])
def services():
    """ Return the list of services """

    _log.debug('request: %s', request)

    services = [_svcDict(svc) for svc in _svcNames]

    return json.jsonify(results=services)


@procService.route('/<service>', methods=['GET'])
def getServiceState(service):
    """
    Return service state.
    """

    _log.debug('request: %s', request)

    if service not in _svcNames:
        raise ExceptionResponse(404, "InvalidService", "Invalid service name " + service)

    runDir = Config.instance().runDir
    initScript = os.path.join(runDir, 'etc/init.d', service)

    cmd = [initScript, 'status']
    running = _runCmd(cmd) == 0

    return json.jsonify(result=_svcDict(service, running))


@procService.route('/<service>', methods=['PUT'])
def execAction(service):
    """
    Do something with service.

    Following parameters are expected to come in a request (in request body
    with application/x-www-form-urlencoded content like regular form):
        action: one of 'stop', 'start', 'restart' (required)
    """

    _log.debug('request: %s', request)

    if service not in _svcNames:
        raise ExceptionResponse(404, "InvalidService", "Invalid service name " + service)

    action = request.form.get('action', '').strip()
    if not action:
        raise ExceptionResponse(400, "MissingArgument", "Action argument (action) is missing")
    if action not in ['start', 'stop', 'restart']:
        raise ExceptionResponse(400, "InvalidArgument",
                                "Unexpected 'action' argument, expecting one of start, stop, restart")

    runDir = Config.instance().runDir
    initScript = os.path.join(runDir, 'etc/init.d', service)

    try:
        cmd = [initScript, action]
        _runCmd(cmd, False)
    except subprocess.CalledProcessError as exc:
        raise ExceptionResponse(409, "CommandFailure", "Failed to execute command " + action, str(exc))

    # check current status
    cmd = [initScript, 'status']
    running = _runCmd(cmd) == 0

    return json.jsonify(result=_svcDict(service, running))
