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
# The tool for manipulating qserv metadata worker.
# TheTool class simply parses arguments and delegates the work 
# down to the meta class.

from __future__ import with_statement
import ConfigParser
import logging
from optparse import OptionParser
import os
import socket

# Local package imports
from lsst.qserv.meta.status import Status, getErrMsg
from lsst.qserv.admin.meta import Meta

class TheTool(object):
    def __init__(self):
        self._usage = """
NAME
        metaTool - the tool for manipulating qserv metadata on worker

SYNOPSIS
        metaTool [-h|--help] [-v|--verbose] COMMAND [ARGS]

OPTIONS
   -h, --help
        Prints help information.

   -v, --verbose
        Turns on verbose mode.

COMMANDS
  installMeta
        Sets up internal qserv metadata database for given worker.

  destroyMeta
        Destroys internal qserv metadata database for given worker.

  printMeta
        Prints all metadata for given worker.

  registerDb
        Registers database for qserv use for given worker.
        Argument: <dbName>

  unregisterDb
        Unregisters database used by qserv and destroys
        corresponding export structures for that database
        for given worker. Argument: <dbName>

  listDbs
        List database names registered for qserv use for
        given worker.

EXAMPLES
Example contents of the (required) '~/.qmwadm' file:

[qmsConn]
host: lsst-db3.slac.stanford.edu
port: 7082
user: qmsUser
pass: qmsPass

[qmwConn]
db: testX
user: qmwUser
pass: qmwPass
mySqlSocket: /var/lib/mysql/mysql.sock
"""
        self._loggerName = "qmwLogger"
        self._loggerOutFile = "/tmp/qmwLogger.log"
        self._loggerLevelName = None

    def parseAndRun(self):
        parser = OptionParser(usage=self._usage)
        (options, args) = parser.parse_args()
        if len(args) < 1:
            parser.error("No command given")
        cmdN = "_cmd_" + args[0]
        if not hasattr(self, cmdN):
            parser.error("Unrecognized command: " + args[0])
        del args[0]

        self._initLogging()

        self._dotFileName = os.path.expanduser("~/.qmwadm")

        (sh,sp,su,sup,wd,wu,wp,wm) = self._getConnInfo()
        self._meta = Meta(self._loggerName, sh,sp,su,sup,wd,wu,wp,wm)

        cmd = getattr(self, cmdN)
        cmd(options, args)

    ###########################################################################
    ##### user-facing commands
    ###########################################################################
    def _cmd_installMeta(self, options, args):
        if len(args) > 0:
            raise Exception("installMeta takes no arguments.")
        self._meta.installMeta()
        print "Metadata successfully installed."

    def _cmd_destroyMeta(self, options, args):
        if len(args) > 0:
            raise Exception("destroyMeta takes no arguments.")
        self._meta.destroyMeta()
        print "All metadata destroyed!"

    def _cmd_printMeta(self, options, args):
        if len(args) > 0:
            raise Exception("printMeta takes no arguments.")
        print self._meta.printMeta()

    def _cmd_registerDb(self, options, args):
        # parse arguments
        if len(args) != 1:
            raise Exception("'registerDb' takes one argument: <dbName>")
        self._meta.registerDb(args[0])
        print "Database successfully registered."

    def _cmd_unregisterDb(self, options, args):
        if len(args) != 1:
            raise Exception("'unregisterDb' takes one argument: <dbName>")
        self._meta.unregisterDb(args[0])
        print "Database unregistered."

    def _cmd_listDbs(self, options, args):
        if len(args) != 0:
            raise Exception("'listDb takes no arguments.")
        print self._meta.listDbs()

    ###########################################################################
    ##### connection info
    ###########################################################################
    def _getConnInfo(self):
        config = ConfigParser.ConfigParser()
        config.read(self._dotFileName)
        s = "qmsConn"
        if not config.has_section(s):
            raise Exception("Bad %s, can't find section '%s'" % \
                                (self._dotFileName, s))
        if not config.has_option(s, "host") or \
           not config.has_option(s, "port") or \
           not config.has_option(s, "user") or \
           not config.has_option(s, "pass"):
            raise Exception("Bad %s, can't find host, port, user or pass"%\
                                self._dotFileName)
        (host,port,usr,pwd) = (config.get(s, "host"), config.getint(s, "port"),
                               config.get(s, "user"), config.get(s, "pass"))

        s = "qmwConn"
        if not config.has_section(s):
            raise Exception("Bad %s, can't find section '%s'" % \
                                (self._dotFileName, s))
        if not config.has_option(s, "db") or \
           not config.has_option(s, "user") or \
           not config.has_option(s, "pass") or \
           not config.has_option(s, "mySqlSocket"):
            raise Exception("Bad %s, can't find db, user, pass or mysqlSocket" % self._dotFileName)
        return (host,port,usr,pwd,
                config.get(s, "db"), config.get(s, "user"),
                config.get(s, "pass"), config.get(s, "mySqlSocket"))

    ###########################################################################
    ##### logger
    ###########################################################################
    def _initLogging(self):
        if self._loggerLevelName is None:
            level = logging.ERROR # default
        else:
            ll = {"debug":logging.DEBUG,
                  "info":logging.INFO,
                  "warning":logging.WARNING,
                  "error":logging.ERROR,
                  "critical":logging.CRITICAL}
            level = ll[self._loggerLevelName]
        self.logger = logging.getLogger(self._loggerLevelName)
        hdlr = logging.FileHandler(self._loggerOutFile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        self.logger.setLevel(level)

###############################################################################
#### main
###############################################################################
if __name__ == '__main__':
    try:
        t = TheTool()
        t.parseAndRun()
    except Exception, e:
        print "Error: ", str(e)
