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
Module defining Auth class and related methods.

@author Andy Salnikov - salnikov@slac.stanford.edu
"""

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import logging

# -----------------------------
# Imports for other modules --
# -----------------------------
from flask import request
from .flask_httpauth import HTTPBasicAuth, HTTPDigestAuth

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

_log = logging.getLogger('auth')

# ------------------------
# Exported definitions --
# ------------------------


class Auth(object):
    """
    Main responsibility of this class is to verify that user is authenticated
    with wmgr. We support two different authentication mechanisms, basic or
    digest, selected via application configuration option AUTH_TYPE.

    Code that needs to check authentication will later call:

        auth = Auth(app.config)
        response = auth.checkAuth()

    which returns None if authentication suceeds or response object if it
    fails. Primary use of this method is as an argument to
    app.before_request() method.
    """

    def __init__(self, appConfig):
        """
        Make new instance of the class.

        @param appConfig:    application configuration instance
        """

        authType = appConfig.get('AUTH_TYPE', 'digest').lower()
        _log.debug('wmgr auth type: %s', authType)
        if authType not in ['none', 'basic', 'digest']:
            raise RuntimeError("unexpected auth type in configuration: " + authType)

        if authType == "none":

            self._secret = None

        else:

            secretFile = appConfig.get('SECRET_FILE')
            if secretFile is None:
                raise RuntimeError("secret file location is required")

            # will fail if secret file is missing
            self._secret = self.readSecret(secretFile)

            if authType == 'basic':
                self._auth = HTTPBasicAuth()
            else:
                # digest
                self._auth = HTTPDigestAuth()

    def checkAuth(self):
        """
        Check for successful authentication. This method is supposed
        to be used by app in before_request list, it returns response
        object if it fails to authenticate or None otherwise.
        """

        if self._secret:
            password = None
            auth = request.authorization
            if auth and auth.username == self._secret[0]:
                password = self._secret[1]
            if not self._auth.authenticate(auth, password):
                return self._auth.auth_error_callback()

    @staticmethod
    def readSecret(fileName):
        """
        Reads secret file and returns (user, password) tuple
        """
        _log.debug('reading wmgr secret from %s', fileName)
        try:
            secret = open(fileName).read().strip()
            if ':' not in secret:
                raise RuntimeError("invalid content of secret file (missing colon): " + fileName)
            secret = secret.split(':', 1)
            if not secret[0] or not secret[1]:
                raise RuntimeError("invalid content of secret file (empty fields): " + fileName)
            return secret
        except Exception as exc:
            raise RuntimeError("failed to read secret file: {}".format(exc))
