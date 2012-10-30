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
# The "client" module was designed to implement a client for the
# qserv metadata server.

from __future__ import with_statement
import ConfigParser
import getpass
import logging
from optparse import OptionParser
import os
import re
import socket
import stat
import xmlrpclib

# Local package imports
from lsst.qserv.metadata import qmsInterface
from lsst.qserv.metadata.qmsStatus import QmsStatus
from lsst.qserv.metadata.qmsStatus import getErrMsg

class Client(object):
    def __init__(self):
        self._dotFileName = os.path.expanduser("~/.qmsadm")
        self._defaultXmlPath = "qms"
        self._initLogger()

        self._createDbOptions = {
            "db_info":("partitioning", "partitioningStrategy")}
        self._createDbPSOptions = {
            "sphBox":("nStripes", 
                      "nSubStripes", 
                      "defaultOverlap_fuzziness",
                      "defaultOverlap_nearNeighbor")}
        self._createTbOptions = {
            "table_info":("tableName",
                          "partitioning",
                          "schemaFile",
                          "clusteredIndex")}
        self._createTbPSOptions = {
            "sphBox":("overlap",
                      "phiColName", 
                      "thetaColName", 
                      "logicalPart",
                      "physChunking")}

    def parseOptions(self):
        usage = """
NAME
       qmsClient - the client program for Qserv Metadata Server (QMS)

SYNOPSIS
       qmsClient [-c CONN] [-v|--verbose] [-h|--help] COMMAND [ARGS]

OPTIONS
   -c
        Specifies connection. Format: user@host:port.
        If not specified, it must be provided in a file
        called ~/.qmsadm.

   -h, --help
        Prints help information. If COMMAND name is given,
        it prints help for the given COMMAND.

   -v, --verbose
        Turns on verbose mode.

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

  retrieveTableInfo
        Retrieves information about a table.
        Arguments: <dbName> <tableName>

EXAMPLES:
Example contents of the .qmsadm file:

[qmsConn]
host: lsst-db3.slac.stanford.edu
port: 4040
user: jacek
password: myPass
"""
        parser = OptionParser(usage=usage)
        parser.add_option("-c", dest="conn")
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
        self._logger = logging.getLogger("qmsClientLogger")
        hdlr = logging.FileHandler("/tmp/qmsClient.log")
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self._logger.addHandler(hdlr)
        self._logger.setLevel(logging.ERROR)

    ###########################################################################
    ##### user-facing commands
    ###########################################################################
    def _cmd_installMeta(self, options, args):
        self._logger.debug("Installing meta")
        qms = self._connectToQMS()
        if qms is None:
            return
        ret = qms.installMeta()
        if ret != QmsStatus.SUCCESS: 
            print getErrMsg(ret)
        else:
            print "Metadata successfully installed."
            self._logger.debug("Metadata successfully installed.")

    def _cmd_destroyMeta(self, options, args):
        self._logger.debug("Destroying meta")
        qms = self._connectToQMS()
        if qms is None:
            return
        ret = qms.destroyMeta()
        if ret != QmsStatus.SUCCESS: 
            print getErrMsg(ret)
        else: 
            print "All metadata destroyed!"
            self._logger.debug("All metadata destroyed")

    def _cmd_printMeta(self, options, args):
        self._logger.debug("Printing meta")
        qms = self._connectToQMS()
        if qms is None:
            return
        print qms.printMeta()
        self._logger.debug("Done printing meta")

    def _cmd_createDb(self, options, args):
        self._logger.debug("createDb")
        # parse arguments
        if len(args) < 2:
            msg = "Insufficient number of arguments"
            self._logger.error(msg)
            print msg
            return
        dbName = args[0]
        theOptions = self._validateCrXXParams(args, "Db")
        if theOptions is None:
            return
        # connect to qms
        qms = self._connectToQMS()
        if qms is None:
            return
        # do it
        self._logger.debug("createDb %s, options are: " % dbName)
        self._logger.debug(theOptions)
        ret = qms.createDb(dbName, theOptions)
        if ret != QmsStatus.SUCCESS: 
            print getErrMsg(ret)
            self._logger.error("createDb failed")
            return
        self._logger.debug("createDb successfully finished")
        print "Database successfully created."

    def _cmd_dropDb(self, options, args):
        self._logger.debug("Dropping db")
        if len(args) != 1:
            msg = "'dropDb' requires one argument: <dbName>"
            self._logger.error(msg)
            print msg
            return
        dbName = args[0]
        qms = self._connectToQMS()
        if qms is None:
            return
        self._logger.debug("dropping %s" % dbName)
        ret = qms.dropDb(dbName)
        if ret != QmsStatus.SUCCESS: 
            print getErrMsg(ret)
            self._logger.error("dropDb failed")
            return
        self._logger.debug("dropDb successfully finished")
        print "Database dropped."

    def _cmd_retrieveDbInfo(self, options, args):
        self._logger.debug("Retrieve db info")
        if len(args) != 1:
            msg = "'retrieveDbInfo' requires one argument: <dbName>"
            self._logger.error(msg)
            print msg
            return
        qms = self._connectToQMS()
        if qms is None:
            return
        (retStat, values) = qms.retrieveDbInfo(args[0])
        if retStat != QmsStatus.SUCCESS:
            print getErrMsg(retStat)
            return
        for (k, v) in values.items():
            print "%s: %s" % (k, v)
        self._logger.debug("Done retrieving db info")

    def _cmd_listDbs(self, options, args):
        self._logger.debug("List databases")
        qms = self._connectToQMS()
        if qms is None:
            return
        print qms.listDbs()
        self._logger.debug("Done listing databases")

    def _cmd_checkDbExists(self, options, args):
        self._logger.debug("Check if db exists")
        if len(args) != 1:
            msg = "'checkDbExists' requires one argument: <dbName>"
            self._logger.error(msg)
            print msg
            return
        qms = self._connectToQMS()
        if qms is None:
            return
        if qms.checkDbExists(args[0]) == 1:
            print "yes"
        else:
            print "no"
        self._logger.debug("Done listing databases")

    def _cmd_createTable(self, options, args):
        self._logger.debug("createTable")
        # parse arguments
        if len(args) < 2:
            msg = "Insufficient number of arguments"
            self._logger.error(msg)
            print msg
            return
        dbName = args[0]
        # connect to qms
        qms = self._connectToQMS()
        if qms is None:
            return
        # check if db exists
        if qms.checkDbExists(dbName) == 0:
            print "Database '%s' does not exist" % dbName
            self._logger.error("Database '%s' does not exist" % dbName)
            return
        # check partitioning scheme
        (retStat, values) = qms.retrieveDbInfo(dbName)
        if retStat != QmsStatus.SUCCESS:
            print getErrMsg(retStat)
            return
        ps = values["partitioningStrategy"]
        # now that we know partitioning strategy, validate the parameters
        theOptions = self._validateCrXXParams(args, "Table", ps)
        if theOptions is None:
            return
        # read schema file and pass it
        schemaFileName = theOptions["schemaFile"]
        del theOptions["schemaFile"]
        schemaStr = open(schemaFileName, 'r').read()
        # do it
        self._logger.debug("createTable %s.%s, options are: " % \
                               (dbName, theOptions["tableName"]))
        self._logger.debug(theOptions)
        ret = qms.createTable(dbName, theOptions, schemaStr)
        if ret != QmsStatus.SUCCESS: 
            print getErrMsg(ret)
            self._logger.error("createTable failed")
            return
        self._logger.debug("createTable successfully finished")
        print "Table successfully created."

    def _cmd_dropTable(self, options, args):
        self._logger.debug("DropTable")
        if len(args) != 2:
            msg = "'dropTable' requires two arguments: <dbName> <tableName>"
            self._logger.error(msg)
            print msg
            return
        (dbName, tableName) = args
        qms = self._connectToQMS()
        if qms is None:
            return
        self._logger.debug("dropping table %s.%s" % (dbName, tableName))
        ret = qms.dropTable(dbName, tableName)
        if ret != QmsStatus.SUCCESS: 
            print getErrMsg(ret)
            self._logger.error("dropTable failed")
            return
        self._logger.debug("dropTable successfully finished")
        print "Table dropped."

    def _cmd_retrieveTableInfo(self, options, args):
        self._logger.debug("RetrieveTableInfo")
        if len(args) != 2:
            msg = ("'retrieveTableInfo' requires two arguments: <dbName> "
                   "<tableName>")
            self._logger.error(msg)
            print msg
            return
        (dbName, tableName) = args
        qms = self._connectToQMS()
        if qms is None:
            return
        self._logger.debug("retrievTableInfo for %s.%s" % (dbName, tableName))
        (retStat, values) = qms.retrieveTableInfo(dbName, tableName)
        if retStat != QmsStatus.SUCCESS: 
            print getErrMsg(retStat)
            return
        for (k, v) in values.items():
            print "%s: %s" % (k, v)
        self._logger.debug("Done retrieving table info")

    ###########################################################################
    ##### validating parameters for CreateTable and CreateDb
    ###########################################################################
    def _validateCrXXParams(self, args, what, ps=None):
        """Validates parameters for createTable and createDb"""
        if len(args) == 2 and args[1][0] == '@':
            theOptions = self._readCreateXXConfigFile(args[1][1:], what, ps)
        else:
            theOptions = self._getCreateXXOptions_kv(args[1:], what, ps)
        if what == "Table":
            schemaFileName = theOptions["schemaFile"]
            if not os.access(schemaFileName, os.R_OK):
                print getErrMsg(QmsStatus.ERR_SCHEMA_FILE)
                print "the file was: ", schemaFileName
                return None
            tNameFromSchema = self._extractTableName(schemaFileName)
            if not tNameFromSchema:
                print getErrMsg(QmsStatus.ERR_NO_TABLE_IN_SCHEMA)
                print "the file was: ", schemaFileName
                return None
            if tNameFromSchema != theOptions["tableName"]:
                print ("tableName from schema file does not match provided "
                       "table name: '%s' != '%s'") % \
                       (tNameFromSchema, theOptions["tableName"])
                return None
        return theOptions

    def _readCreateXXConfigFile(self, fName, what, partStrategy=None):
        """It reads the config file for createDb or createTable command,
           validates it and returns dictionary containing key-value pars"""
        errMsg = "Problems with config file '%s':" % fName
        if not os.access(fName, os.R_OK):
            print errMsg, "specified config file '%s' not found." % fName
            return
        config = ConfigParser.ConfigParser()
        config.optionxform = str # case sensitive
        config.read(fName)
        if what == "Db":
            xxOpts = self._createDbOptions
            psOpts = self._createDbPSOptions
        elif what == "Table":
            xxOpts = self._createTbOptions
            psOpts = self._createTbPSOptions
        else:
            assert(1)

        finalDict = {}
        for (section, values) in xxOpts.items():
            if config.has_option(section, "partitioning"):
                partOff = config.get(section, "partitioning") == "off"

        # check if required non-partition specific options found
        for (section, values) in xxOpts.items():
            if not config.has_section(section):
                print errMsg, "required section '%s' not found" % section
                return
            for o in values:
                isException = False
                if not config.has_option(section, o):
                    # "partitioning is an optional parameter
                    if o == "partitioning":
                        isException = True
                    # if partitioning is "off", partitioningStrategy does not
                    # need to be specified
                    if o == "partitioningStrategy" and partOff:
                        isException = True
                    if not isException:
                        print errMsg, "required option '%s' in section '%s' " \
                            % (o, section), " not found"
                        return
                if not isException:
                    finalDict[o] = config.get(section, o)
                if o == "partitioning":
                    myPartStrategy = config.get(section, o)
        if not partOff:
            if not partStrategy:
                for (section, values) in xxOpts.items():
                    if config.has_option(section, "partitioningStrategy"):
                        partStrategy = config.get(section, 
                                                "partitioningStrategy")
            if not partStrategy:
                print errMsg, "can't determine partitiong strategy"
                return
            # check if partition-strategy specific options found
            for o in psOpts[partStrategy]:
                if not config.has_option(partStrategy, o):
                    print errMsg, "required option '%s' in section '%s' " \
                        "not found" % (o, partStrategy)
                    return
                finalDict[o] = config.get(partStrategy, o)

        if what == "Table":
            finalDict["partitioningStrategy"] = partStrategy

        # FIXME: note, we are currently not detecting extra options
        # that user might have put in the file that we do not support
        return finalDict

    def _extractTableName(self, fName):
        f = open(fName, 'r')
        findIt = re.compile(r'CREATE TABLE *`?\'?\"?(\w+)', re.IGNORECASE)
        theName = None
        for line in f:
            m = findIt.match(line)
            if m:
                theName = m.group(1)
                break
        f.close()
        return theName

    def _getCreateXXOptions_kv(self, args, what, partStrategy=None):
        if what == "Db":
            xxOpts = self._createDbOptions
            psOpts = self._createDbPSOptions
        elif what == "Table":
            xxOpts = self._createTbOptions
            psOpts = self._createTbPSOptions
        else:
            assert(1)

        xx = {}
        for arg in args:
            if not '=' in arg:
                msg = ("Invalid param: '%s' expected one of the following "
                       "formats:\n"
                       "  a) create%s <dbName> @<paramFile>\n"
                       "  b) create%s <dbName> <param1>=<value1> "
                       "<param2>=<value2> ...") % (arg, what, what)
                self._logger.error(msg)
                print msg
                return None
            k, v = arg.split("=", 1)
            xx[k] = v
        if what == "Table":
            xx["partitioningStrategy"] = partStrategy
        # add default value for missing options
        if not xx.has_key("clusteredIndex"):
            print ("param 'clusteredIndex' not found using default: NULL")
            xx["clusteredIndex"] = "NULL"
        if not xx.has_key("partitioning"):
            print ("param 'partitioning' not found using default: off")
            xx["partitioning"] = "off"
        # do final validation
        if not self._validateKVOptions(xx, xxOpts, psOpts):
            return None
        return xx

    def _validateKVOptions(self, x, xxOpts, psOpts):
        for (theName, theOpts) in xxOpts.items():
            for o in theOpts:
                if not x.has_key(o):
                    print ("Can't find required param '%s'") % o
                    return False

        if x["partitioning"] == "on":
            if not x.has_key("partitioningStrategy"):
                print ("partitioningStrategy option is required if "
                       "partitioning is on")
                return False
        elif x["partitioning"] == "off":
            return True
        else:
            print ("Unrecognized value for param 'partitioning' (%s), "
                   "supported on/off") % x["partitioning"]
            return False
        psFound = False
        for (psName, theOpts) in psOpts.items():
            if x["partitioningStrategy"] == psName:
                psFound = True
                for o in theOpts:
                    if not x.has_key(o):
                        print("Can't find param '%s' required for partitioning"
                               " strategy '%s'") % (o, psName)
                        return False
        if not psFound:
            print "Unrecongnized partitioning strategy '%s', support:'sphBox"%\
                x["partitioningStrategy"]
            return False
        return True

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
        if options.conn is None:
            # get if from .qmsadm, or fail
            (host, port, user, pwd) = self._getCachedConnInfo()
            if host is None or port is None or user is None or pwd is None:
                print "Missing connection information ", \
                    "(hint: use -c or use .qmsadm file)"
                return (None, None, None, None)
            return (host, port, user, pwd)
        # use what user provided via -c option
        m = re.match(r'([\w.]+)@([\w.-]+):([\d]+)', options.conn)
        if not m:
            print "Failed to parse: %s, expected: user@host:port"%options.conn
            return (None, None, None, None)
        # and ask for password
        pwd = self._getPasswordFromUser()
        if pwd is None:
            return (None, None, None, None)
        (host, port, user) = (m.group(2), m.group(3), m.group(1))
        if host is None or port is None or user is None or pwd is None:
            return (None, None, None, None)
        return (host, port, user, pwd)

    def _getPasswordFromUser(self):
        pass1 = getpass.getpass()
        pass2 = getpass.getpass("Confirm password: ")
        if pass1 != pass2:
            print "Passwords do not match"
            return None
        return pass1

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

###############################################################################
#### main
###############################################################################
if __name__ == '__main__':
    c = Client()
    (cmd, options, args) = c.parseOptions()
    cmd(options, args)
