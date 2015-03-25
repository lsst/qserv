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

#--------------------------------
#  Imports of standard modules --
#--------------------------------

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.db import db
from lsst.qserv.admin.qservAdmin import QservAdmin
from .errors import ExceptionResponse

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

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
        """ Do not instanciate Config class directly, use init/instance methods """
        self.dbHost = appConfig.get('DB_HOST')
        self.dbPort = appConfig.get('DB_PORT')
        self.dbSocket = appConfig.get('DB_SOCKET')
        # user/password for regular account
        self.dbUser = appConfig.get('DB_USER')
        self.dbPasswd = appConfig.get('DB_PASSWD')
        # user/password for privileged account
        self.dbUserPriv = appConfig.get('DB_USER_PRIV')
        self.dbPasswdPriv = appConfig.get('DB_PASSWD_PRIV')
        # CSS connection info
        self.useCss = appConfig.get('USE_CSS', True)
        self.cssConn = appConfig.get('CSS_CONN', 'localhost:12181')
        # Location of the run directory for qserv, must contain etc/ stuff
        self.runDir = appConfig.get('RUN_DIR')

        self._db = None
        self._dbPriv = None

    def dbConn(self):
        """ Return database connection, instance of db.Db type """
        kwargs = {}
        if self.dbHost: kwargs['host'] = self.dbHost
        if self.dbPort: kwargs['port'] = self.dbPort
        if self.dbSocket: kwargs['unix_socket'] = self.dbSocket
        if self.dbUser: kwargs['user'] = self.dbUser
        if self.dbPasswd: kwargs['passwd'] = self.dbPasswd
        return db.Db(**kwargs)

    def privDbConn(self):
        """ Return database connection for priviledged account """
        kwargs = {}
        if self.dbHost: kwargs['host'] = self.dbHost
        if self.dbPort: kwargs['port'] = self.dbPort
        if self.dbSocket: kwargs['unix_socket'] = self.dbSocket
        if self.dbUserPriv: kwargs['user'] = self.dbUserPriv
        if self.dbPasswdPriv: kwargs['passwd'] = self.dbPasswdPriv
        return db.Db(**kwargs)

    def qservAdmin(self):
        """
        Returns QservQdmin instance, if CSS is disabled (via USE_CSS=False)
        it throws an ExceptionResponse exception with code 409 (CONFLICT).
        """
        if not self.useCss:
            # cannot sio that, return 409 (CONFLICT) code
            raise ExceptionResponse(409, "CSSDisabled",
                                    "CSS access is disabled by service configuration")
        return QservAdmin(self.cssConn)
