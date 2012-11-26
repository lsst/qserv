#!/usr/bin/env python

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
# The guts of manipulating qserv metadata worker. 


from status import Status

class Meta(object):
    def __init__(self):
        pass

    def installMeta(self):
        return Status.ERR_NOT_IMPLEMENTED

    def destroyMeta(self):
        return Status.ERR_NOT_IMPLEMENTED

    def printMeta(self):
        return Status.ERR_NOT_IMPLEMENTED

    def registerDb(self, dbName):
        return Status.ERR_NOT_IMPLEMENTED

    def unregisterDb(self, dbName):
        return Status.ERR_NOT_IMPLEMENTED

    def listDbs(self):
        return Status.ERR_NOT_IMPLEMENTED

    ###########################################################################
    ##### connection to QMS
    ###########################################################################
    def _connectToQMS(self):
        (host, port, user, pwd) = self._getConnInfo()
        if host is None or port is None or user is None or pwd is None:
            return False
        self._logger.debug("Using connection: %s:%s, %s,pwd=%s" % \
                               (host, port, user, pwd))
        url = "http://%s:%d/%s" % (host, port, self._defaultXmlPath)
        self._logger.debug("Url is %s" % url)
        qms = xmlrpclib.Server(url)
        if self._echoTest(qms):
            return qms
        else:
            return None

    def _getConnInfo(self):
        # get if from .qmsadm, or fail
        (host, port, user, pwd) = self._getCachedConnInfo()
        if host is None or port is None or user is None or pwd is None:
            print "Missing connection information ", \
                "(hint: use -c or use .qmsadm file)"
            return (None, None, None, None)
        return (host, port, user, pwd)

    def _getCachedConnInfo(self):
        self._logger.debug("Getting cached connection info")
        config = ConfigParser.ConfigParser()
        config.read(self._dotFileName)
        s = "qmsConn"
        if not config.has_section(s):
            print "Can't find section '%s' in .qmsadm" % s
            return (None, None, None, None)
        if not config.has_option(s, "host") or \
           not config.has_option(s, "port") or \
           not config.has_option(s, "user") or \
           not config.has_option(s, "password"):
            print "Bad %s, can't find host, port, user or password" % \
                self._dotFileName
            return (None, None, None, None)
        return (config.get(s, "host"),
                config.getint(s, "port"),
                config.get(s, "user"),
                config.get(s, "password"))

    def _echoTest(self, qms):
        echostring = "QMS test string echo back. 1234567890.()''?"
        try:
            ret = qms.echo(echostring)
        except socket.error, err:
            print "Unable to connect to qms (%s)" % err
            return False
        if ret != echostring:
            print "Expected %s, got %s" % (echostring, ret)
            return False
        return True

