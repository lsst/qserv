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
# down to the qmwImpl class.

from __future__ import with_statement
import ConfigParser
import logging
from optparse import OptionParser
import os
import socket
import xmlrpclib

# Local package imports
from lsst.qserv.qmw.status import Status    # FIXME: when ticket1944 pushed
from lsst.qserv.qmw.status import getErrMsg # FIXME: when ticket1944 pushed
from qmwImpl import QmwImpl

class TheTool(object):
    def __init__(self):
        self._dotFileName = os.path.expanduser("~/.qmwadm")
        self._qmw = QmwImpl()
        self._initLogger()

    def parseOptions(self):
        usage = """
NAME
        qmwTool - the tool for manipulating qserv metadata on worker

SYNOPSIS
        qmwTool [-h|--help] [-v|--verbose] COMMAND [ARGS]

OPTIONS
   -h, --help
        Prints help information.

   -v, --verbose
        Turns on verbose mode.

COMMANDS
  installMeta
        Sets up internal qserv metadata database.

  destroyMeta
        Destroys internal qserv metadata database.

  printMeta
        Prints all metadata for given worker.

  registerDb
        Registers database for qserv use for given worker.
        Argument: <dbName>

  unregisterDb
        Unregisters database used by qserv and destroys
        corresponding export structures for that database.
        Argument: <dbName>

  listDbs
        List database names registered for qserv use.

  createExportPaths
        Generates export paths. If no dbName is given, it will
        run for all databases registered in qserv metadata
        for given worker. Argument: [<dbName>]

  rebuildExportPaths
        Removes existing export paths and recreates them.
        If no dbName is given, it will run for all databases
        registered in qserv metadata for given worker.
        Argument: [<dbName>]

EXAMPLES
Example contents of the (required) '~/.qmwadm' file:
qmsHost:localhost
qmsPort:7082
qmsUser:qms
qmsPass:qmsPass
qmsDb:testX
qmwUser:qmw
qmwPass:qmwPass
qmwMySqlSocket:/var/lib/mysql/mysql.sock
"""
        parser = OptionParser(usage=usage)
        parser.add_option("-v", "--verbose", action="store_true",
                          dest="verbose")
        (options, args) = parser.parse_args()

        if options.verbose:
            self._logger.setLevel(logging.DEBUG)

        if len(args) < 1:
            parser.error("No command given")
        cmd = "_cmd_" + args[0]
        if not hasattr(self, cmd):
            parser.error("Unrecognized command: " + args[0])
        del args[0]
        return getattr(self, cmd), options, args

    def _initLogger(self):
        self._logger = logging.getLogger("qmwClientLogger")
        hdlr = logging.FileHandler("/tmp/qmwClient.log")
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self._logger.addHandler(hdlr)
        self._logger.setLevel(logging.ERROR)

    ###########################################################################
    ##### user-facing commands
    ###########################################################################
    def _cmd_installMeta(self, options, args):
        self._logger.debug("Installing meta")
        ret = self._qmw.installMeta()
        if ret != Status.SUCCESS: 
            print getErrMsg(ret)
        else:
            print "Metadata successfully installed."
            self._logger.debug("Metadata successfully installed.")

    def _cmd_destroyMeta(self, options, args):
        self._logger.debug("Destroying meta")
        ret = self._qmw.destroyMeta()
        if ret != Status.SUCCESS: 
            print getErrMsg(ret)
        else:
            print "All metadata destroyed!"
            self._logger.debug("All metadata destroyed")

    def _cmd_printMeta(self, options, args):
        self._logger.debug("Printing meta")
        print self._qmw.printMeta()
        self._logger.debug("Done printing meta")

    def _cmd_registerDb(self, options, args):
        self._logger.debug("registerDb")
        # parse arguments
        if len(args) != 1:
            msg = "Incorrect number of arguments (expected <dbName>)"
            self._logger.error(msg)
            print msg
            return
        dbName = args[0]
        if theOptions is None:
            return
        self._logger.debug("registerDb %s" % dbName)
        ret = self._qmw.registerDb(dbName)
        if ret != Status.SUCCESS: 
            print getErrMsg(ret)
            self._logger.error("registerDb failed")
            return
        self._logger.debug("registerDb successfully finished")
        print "Database successfully registered."

    def _cmd_unregisterDb(self, options, args):
        self._logger.debug("Unregistering db")
        if len(args) != 1:
            msg = "'unregisterDb' requires one argument: <dbName>"
            self._logger.error(msg)
            print msg
            return
        dbName = args[0]
        self._logger.debug("unregistering %s" % dbName)
        ret = self._qmw.unregisterDb(dbName)
        if ret != Status.SUCCESS: 
            print getErrMsg(ret)
            self._logger.error("unregisterDb failed")
            return
        self._logger.debug("unregisterDb successfully finished")
        print "Database unregistered."

    def _cmd_listDbs(self, options, args):
        if len(args) != 0:
            msg = "'listDb does not take any arguments."
            print msg
            return
        print self._qmw.listDbs()

###############################################################################
#### main
###############################################################################
if __name__ == '__main__':
    c = TheTool()
    (cmd, options, args) = c.parseOptions()
    cmd(options, args)
