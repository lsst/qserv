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
Module defining Config class and related methods.

@author Andy Salnikov - salnikov@slac.stanford.edu
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import logging
import tempfile

# -----------------------------
# Imports for other modules --
# -----------------------------
from .errors import ExceptionResponse
from lsst.db.engineFactory import getEngineFromArgs
from lsst.qserv import css
from sqlalchemy.pool import NullPool

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

_log = logging.getLogger('config')

# ------------------------
# Exported definitions --
# ------------------------


class Config(object):
    """
    Special config class used to store config parameters for the whole service.
    """

    # this is the instance which holds configuration,
    # only available after initConfig() call
    _instance = None

    @staticmethod
    def init(app):
        """ Initialize configuration from application instance. """
        Config._instance = Config(app.config)

    @staticmethod
    def instance():
        """ get configuration """
        return Config._instance

    def __init__(self, appConfig):
        """ Do not instantiate Config class directly, use init/instance methods """
        self.dbHost = appConfig.get('DB_HOST')
        self.dbPort = appConfig.get('DB_PORT')
        self.dbSocket = appConfig.get('DB_SOCKET')
        # user/password for regular account
        self.dbUser = appConfig.get('DB_USER')
        self.dbPasswd = appConfig.get('DB_PASSWD')
        # user/password for privileged account
        self.dbUserPriv = appConfig.get('DB_USER_PRIV')
        self.dbPasswdPriv = appConfig.get('DB_PASSWD_PRIV')
        # parameters for mysql-proxy connection
        self.proxyHost = appConfig.get('PROXY_HOST')
        self.proxyPort = appConfig.get('PROXY_PORT')
        self.proxyUser = appConfig.get('PROXY_USER')
        self.proxyPasswd = appConfig.get('PROXY_PASSWD')
        # CSS connection info
        self.useCss = appConfig.get('USE_CSS', True)
        self.cssConfig = appConfig.get('CSS_CONFIG')
        # Location of the run directory for qserv, must contain etc/ stuff
        self.runDir = appConfig.get('RUN_DIR')
        self.tmpDir = appConfig.get('TMP_DIR', '/tmp')

        # all temporary files will be created in that location
        tempfile.tempdir = self.tmpDir

        self._db = None
        self._dbPriv = None
        self._dbProxy = None

    def dbEngine(self):
        """ Return database engine.

        Standard Pool class causes delays in releasing connections, so we
        disable connection pooling by using NullPool class. Another reason to
        disable pool is that MySQL connections cannot be shared across many
        threads.
        """
        if self._db is not None:
            return self._db

        kwargs = dict(poolclass=NullPool, query={})
        # To use LOCA DATA LOCAL INFILE we need to enable it explicitely
        kwargs['query']['local_infile'] = 1
        if self.dbHost:
            kwargs['host'] = self.dbHost
        if self.dbPort:
            kwargs['port'] = self.dbPort
        if self.dbSocket:
            kwargs['query']['unix_socket'] = self.dbSocket
        if self.dbUser:
            kwargs['username'] = self.dbUser
        _log.debug('creating new engine (password not shown) %s', kwargs)
        if self.dbPasswd:
            kwargs['password'] = self.dbPasswd
        self._db = getEngineFromArgs(**kwargs)
        return self._db

    def privDbEngine(self):
        """ Return database engine for priviledged account """

        if self._dbPriv is not None:
            return self._dbPriv

        kwargs = dict(poolclass=NullPool, query={})
        if self.dbHost:
            kwargs['host'] = self.dbHost
        if self.dbPort:
            kwargs['port'] = self.dbPort
        if self.dbSocket:
            kwargs['query']['unix_socket'] = self.dbSocket
        if self.dbUserPriv:
            kwargs['username'] = self.dbUserPriv
        _log.debug('creating new engine (password not shown) %s', kwargs)
        if self.dbPasswdPriv:
            kwargs['password'] = self.dbPasswdPriv
        self._dbPriv = getEngineFromArgs(**kwargs)
        return self._dbPriv

    def proxyDbEngine(self):
        """ Return database engine for proxy """
        if self._dbProxy is not None:
            return self._dbProxy

        kwargs = dict(poolclass=NullPool, query={})
        if self.proxyHost:
            kwargs['host'] = self.proxyHost
        if self.proxyPort:
            kwargs['port'] = self.proxyPort
        if self.proxyUser:
            kwargs['username'] = self.proxyUser
        _log.debug('creating new engine (password not shown) %s', kwargs)
        if self.proxyPasswd:
            kwargs['password'] = self.proxyPasswd
        self._dbProxy = getEngineFromArgs(**kwargs)
        return self._dbProxy

    def cssAccess(self):
        """
        Returns CssAccess instance, if CSS is disabled (via USE_CSS=False)
        it throws an ExceptionResponse exception with code 409 (CONFLICT).
        """
        if not self.useCss:
            # CSS disabled in config, return 409 (CONFLICT) code
            raise ExceptionResponse(409, "CSSDisabled",
                                    "CSS access is disabled by service configuration")
        return css.CssAccess.createFromConfig(self.cssConfig, '')
