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
# The "metaClientTool" is a tool that sends commands to qserv metadata server
# using the "client" module.

from __future__ import with_statement
import ConfigParser
import getpass
import logging
from optparse import OptionParser
import os
import re
import sys

# Local package imports
from lsst.qserv.meta.status import Status, getErrMsg
from lsst.qserv.meta.client import Client


class TheTool(object):
    def __init__(self):
        self._usage = """
NAME
        metaClientTool - the client program for Qserv Metadata Server (QMS)

SYNOPSIS
        metaClientTool [-c CONN] [-h|--help] COMMAND [ARGS]

OPTIONS
   -c
        Specifies connection. Format: user@host:port.
        If not specified, it must be provided in a file
        called ~/.qmsadm.

   -h, --help
        Prints help information. If COMMAND name is given,
        it prints help for the given COMMAND.

COMMANDS
  installMeta
        Sets up internal qserv metadata database.

  destroyMeta
        Destroys internal qserv metadata database.

  printMeta
        Prints all metadata.

  createDb
        Creates metadata about new database to be managed 
        by qserv. Arguments can come in one of two forms:
        a) file-based version: 
           <dbName> @<paramFile>
        b) key/value-based version: 
           <dbName> <param1>=<value1> <param2>=<value2> ...

  dropDb
        Removes metadata about a database that was managed
        my qserv. Arguments: <dbName>

  retrieveDbInfo
        Retrieves information about a database.
        Arguments: <dbName>

  checkDbExists
        Checks if the database exists, returns 0 or 1.
        Arguments: <dbName>

  listDbs
        Lists database names registered for qserv use.

  createTable
        Creates metadata about new table in qserv-managed 
        database. Arguments can come in one of two forms:
        a) file-base version:
           <dbName> @<paramFile>
        b) key/value-based version:
           <dbName> <param1>=<value1> <param2>=<value2> ...

  dropTable
        Removes metadata about a table.
        Arguments: <dbName> <tableName>

  retrievePartitionedTables
        Retrieves table names of all partitioned tables
        in a given database.
        Arguments: <dbName>

  retrieveTableInfo
        Retrieves information about a table.
        Arguments: <dbName> <tableName>

  getInternalQmsDbName
        Retrieves the name of internal qms database.

EXAMPLES
Example contents of the .qmsadm file:

[qmsConn]
host: lsst-db3.slac.stanford.edu
port: 4040
user: jacek
password: myPass
"""

    def parseAndRun(self):
        parser = OptionParser(usage=self._usage)
        parser.add_option("-c", dest="conn")
        (options, args) = parser.parse_args()
        if len(args) < 1:
            parser.error("No command given")
        cmdN = "_cmd_" + args[0]
        if not hasattr(self, cmdN):
            parser.error("Unrecognized command: " + args[0])
        del args[0]

        self._dotFileName = os.path.expanduser("~/.qmsadm")
        (host, port, user, pwd) = self._getConnInfo(options.conn)

        self._client = Client(host, port, user, pwd)

        cmd = getattr(self, cmdN)
        cmd(options, args)

    ###########################################################################
    ##### user-facing commands
    ###########################################################################
    def _cmd_installMeta(self, options, args):
        if len(args) > 0:
            raise Exception("installMeta takes no arguments.")
        self._client.installMeta()
        print "Metadata successfully installed."

    def _cmd_destroyMeta(self, options, args):
        if len(args) > 0:
            raise Exception("destroyMeta takes no arguments.")
        self._client.destroyMeta()
        print "All metadata destroyed!"

    def _cmd_printMeta(self, options, args):
        if len(args) > 0:
            raise Exception("printMeta takes no arguments.")
        print self._client.printMeta()

    def _cmd_createDb(self, options, args):
        # parse arguments
        if len(args) < 2:
            raise Exception("Insufficient number of arguments")
        dbName = args[0]
        theOptions = self._fetchCrXXOptions(args, "Db")
        print "the options are: ", theOptions
        # do it
        self._client.createDb(dbName, theOptions)
        print "Database successfully created."

    def _cmd_dropDb(self, options, args):
        if len(args) != 1:
            raise Exception("'dropDb' requires one argument: <dbName>")
        dbName = args[0]
        self._client.dropDb(dbName)
        print "Database dropped."

    def _cmd_retrieveDbInfo(self, options, args):
        if len(args) != 1:
            raise Exception("'retrieveDbInfo' requires one argument: <dbName>")
        values = self._client.retrieveDbInfo(args[0])
        for (k, v) in values.items():
            print "%s: %s" % (k, v)

    def _cmd_listDbs(self, options, args):
        if len(args) > 0:
            raise Exception("listDbs takes no arguments.")
        print self._client.listDbs()

    def _cmd_checkDbExists(self, options, args):
        if len(args) != 1:
            raise Exception("'checkDbExists' requires one argument: <dbName>")
        if self._client.checkDbExists(args[0]) == 1:
            print "yes"
        else:
            print "no"

    def _cmd_createTable(self, options, args):
        # parse arguments
        if len(args) < 2:
            raise Exception("Insufficient number of arguments")
        dbName = args[0]
        theOptions = self._fetchCrXXOptions(args, "Table")
        self._client.createTable(dbName, theOptions)
        print "Table successfully created."

    def _cmd_dropTable(self, options, args):
        if len(args) != 2:
            raise Exception("'dropTable' requires two arguments: ",
                            "<dbName> <tableName>")
        (dbName, tableName) = args
        self._client.dropTable(dbName, tableName)
        print "Table dropped."

    def _cmd_retrievePartitionedTables(self, options, args):
        if len(args) != 1:
            raise Exception("'retrievePartTables' requires one arguments: ",
                            "<dbName>")
        dbName = args[0]
        tNames = self._client.retrievePartTables(dbName)
        print tNames

    def _cmd_retrieveTableInfo(self, options, args):
        if len(args) != 2:
            raise Exception("'retrieveTableInfo' requires two arguments: ",
                            " <dbName> <tableName>")
        (dbName, tableName) = args
        values = self._client.retrieveTableInfo(dbName, tableName)
        for (k, v) in values.items():
            print "%s: %s" % (k, v)

    def _cmd_getInternalQmsDbName(self, options, args):
        if len(args) > 0:
            raise Exception("getInternalQmsDbName takes no arguments.")
        dbName = self._client.getInternalQmsDbName()
        print dbName

    ###########################################################################
    ##### fetch options for CreateDb and CreateTable
    ###########################################################################
    def _fetchCrXXOptions(self, args, what):
        """Fetches createDb/createTable options from either config file or
           key-value pairs."""
        if len(args) == 2 and args[1][0] == '@':
            return self._fetchCrXXOptions_cf(args[1][1:])
        return self._fetchCrXXOptions_kv(args[1:], what)

    def _fetchCrXXOptions_cf(self, fName):
        """It reads the config file for createDb or createTable command,
           and returns key-value pair dictionary (flat, e.g., sections
           are ignored."""
        if not os.access(fName, os.R_OK):
            raise Exception("Specified config file '%s' not found." % fName)
        config = ConfigParser.ConfigParser()
        config.optionxform = str # case sensitive
        config.read(fName)
        xx = {}
        for section in config.sections():
            for option in config.options(section):
                xx[option] = config.get(section, option)
        return xx

    def _fetchCrXXOptions_kv(self, args, what):
        xx = {}
        for arg in args:
            if not '=' in arg:
                msg = ("Invalid param: '%s' expected one of the following "
                       "formats:\n"
                       "  a) create%s <dbName> @<paramFile>\n"
                       "  b) create%s <dbName> <param1>=<value1> "
                       "<param2>=<value2> ...") % (arg, what, what)
                raise Exception(msg)
            k, v = arg.split("=", 1)
            xx[k] = v
        return xx

    ###########################################################################
    ##### connection info
    ###########################################################################
    def _getConnInfo(self, connInfoStr):
        if connInfoStr is None:
            # get if from .qmsadm, or fail
            (host, port, user, pwd) = self._getCachedConnInfo()
            if host is None or port is None or user is None or pwd is None:
                raise Exception("Missing connection information ", \
                    "(hint: use -c or use .qmsadm file)")
            return (host, port, user, pwd)
        # use what user provided via -c option
        m = re.match(r'([\w.]+)@([\w.-]+):([\d]+)', connInfoStr)
        if not m:
            raise Exception("Failed to parse: %s, expected: user@host:port" % \
                                connInfoStr)
        # and ask for password
        pwd = self._getPasswordFromUser()
        (host, port, user) = (m.group(2), m.group(3), m.group(1))
        return (host, port, user, pwd)

    def _getPasswordFromUser(self):
        pass1 = getpass.getpass()
        pass2 = getpass.getpass("Confirm password: ")
        if pass1 != pass2:
            raise Exception("Passwords do not match")
        return pass1

    def _getCachedConnInfo(self):
        config = ConfigParser.ConfigParser()
        config.read(self._dotFileName)
        s = "qmsConn"
        if not config.has_section(s):
            raise Exception("Can't find section '%s' in .qmsadm" % s)
        if not config.has_option(s, "host") or \
           not config.has_option(s, "port") or \
           not config.has_option(s, "user") or \
           not config.has_option(s, "password"):
            raise Exception("Bad %s, can't find host, port, user or password" \
                                % self._dotFileName)
        return (config.get(s, "host"), config.getint(s, "port"),
                config.get(s, "user"), config.get(s, "password"))

###############################################################################
#### main
###############################################################################
if __name__ == '__main__':
    try:
        t = TheTool()
        t.parseAndRun()
    except Exception, e:
        print "Error: ", str(e)
