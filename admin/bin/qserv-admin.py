#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2014 AURA/LSST.
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

"""
qserv client program used by all users that talk to qserv. A thin
shell that parses commands, reads all input data in the form of config
files into arrays, and calls corresponding function.

@author  Jacek Becla, SLAC

Known issues and todos:
 - deal with user authentication
 - many commands still need to be implemented
 - need to separate dangerous admin commands like DROP EVERYTHING
"""

# standard library imports
import ConfigParser
import logging
from optparse import OptionParser
import os
import re
import readline
import sys

# local imports
from lsst.qserv.css.kvInterface import KvException
from lsst.qserv.admin.qservAdmin import QservAdmin, NodeState
from lsst.qserv.admin.qservAdminException import QservAdminException

####################################################################################
class CommandParser(object):
    """
    Parse commands and calls appropriate function from qservAdmin.
    """

    requiredOpts = {
        "createDb" : [ # Db opts
            "storageClass",
            "partitioning",
            "partitioningStrategy"
            ],
        "createDbSphBox" : [
            "nStripes",
            "nSubStripes",
            "overlap"],
        "createTable" : [
            "tableName",
            "partitioning",
            "schema", # "schemaFile" can be read into "schema"
            "clusteredIndex",
            "match",
            "isView"],
        "createTableSphBox" : [
            "dirTable",
            # "overlap" # overlap should inherit from db if unspecified.
            ],
        "createTableDir" : [
            "dirColName",
            "latColName",
            "lonColName"
            ],
        "createChildTable" : [
            "dirColName",
            ],
        "createChildTableExtra" : [
            "latColName",
            "lonColName"],
        "createTableMatch" : [
            "dirTable1",
            "dirColName1",
            "dirTable2",
            "dirColName2"],
        "createNode" : [
            "type",
            "host",
            "port",
            "state"],
        "updateNode" : [
            "state"],
        }

    def __init__(self, connInfo):
        """
        Initialize shared metadata, including list of supported commands.

        @param connInfo     Connection information.
        """
        self._initLogging()
        self._funcMap = {
            'CREATE':  self._parseCreate,
            'DELETE':  self._parseDelete,
            'DROP':    self._parseDrop,
            'DUMP':    self._parseDump,
            'EXIT':    self._justExit,
            'HELP':    self._printHelp,
            'QUIT':    self._justExit,
            'RELEASE': self._parseRelease,
            'RESTORE': self._restore,
            'SHOW':    self._parseShow,
            'UPDATE':  self._parseUpdate
            }
        self._impl = QservAdmin(connInfo)
        self._supportedCommands = """
  Supported commands:
    CREATE DATABASE <dbName> <configFile>;
    CREATE DATABASE <dbName> LIKE <dbName2>;
    CREATE TABLE <dbName>.<tableName> <configFile>;
    CREATE TABLE <dbName>.<tableName> LIKE <dbName2>.<tableName2>;
    CREATE NODE <nodeName> <key=value ...>;  # keys: type, host, port, state
    UPDATE NODE <nodeName> state=value;  # value: ACTIVE, INACTIVE
    DELETE NODE <nodeName>;
    DROP DATABASE <dbName>;
    DROP EVERYTHING;
    DUMP EVERYTHING [<outFile>];
    RESTORE <inFile>;
    SHOW DATABASES;
    SHOW NODES;
    QUIT;
    EXIT;
    ...more coming soon
"""
        # only prompt if input comes from terminal
        self._prompt = "qserv > " if sys.stdin.isatty() else ""


    def receiveCommands(self):
        """
        Receive user commands. End of command is determined by ';'. Multiple
        commands per line are allowed. Multi-line commands are allowed. To
        terminate: CTRL-D, or 'exit;' or 'quit;'.
        """
        line = ''
        cmd = ''
        prompt = self._prompt
        while True:
            line = raw_input(prompt).decode("utf-8").strip()
            cmd += line + ' '
            if prompt:
                prompt = self._prompt if line.endswith(';') else "~ "
            while re.search(';', cmd):
                pos = cmd.index(';')
                try:
                    self.parse(cmd[:pos])
                except (QservAdminException, KvException) as e:
                    self._logger.error(e.__str__())
                    print "ERROR: ", e.__str__()
                cmd = cmd[pos+1:]

    def parse(self, cmd):
        """
        Parse, and dispatch to subparsers based on first word. Raise
        exceptions on errors.
        """
        cmd = cmd.strip()
        # ignore empty commands, these can be generated by typing ;;
        if len(cmd) == 0: return
        tokens = cmd.split()
        t = tokens[0].upper()
        if t in self._funcMap:
            self._funcMap[t](tokens[1:])
        else:
            raise QservAdminException(QservAdminException.NOT_IMPLEMENTED, cmd)

    def _parseCreate(self, tokens):
        """
        Subparser - handles all CREATE requests.
        """
        t = tokens[0].upper()
        if t == 'DATABASE':
            self._parseCreateDatabase(tokens[1:])
        elif t == 'TABLE':
            self._parseCreateTable(tokens[1:])
        elif t == 'NODE':
            self._parseCreateNode(tokens[1:])
        else:
            raise QservAdminException(QservAdminException.BAD_CMD)

    def _parseCreateDatabase(self, tokens):
        """
        Subparser - handles all CREATE DATABASE requests.
        Throws KvException, QservAdminException
        """
        l = len(tokens)
        if l == 2:
            dbName = tokens[0]
            configFile = tokens[1]
            options = self._fetchOptionsFromConfigFile(configFile)
            options = self._processDbOptions(options)
            self._impl.createDb(dbName, options)
        elif l == 3:
            if tokens[1].upper() != 'LIKE':
                raise QservAdminException(QservAdminException.BAD_CMD,
                                    "Expected 'LIKE', found: '%s'." % tokens[1])
            dbName = tokens[0]
            dbName2 = tokens[2]
            self._impl.createDbLike(dbName, dbName2)
        else:
            raise QservAdminException(QservAdminException.BAD_CMD,
                                "Unexpected number of arguments.")

    def _parseCreateTable(self, tokens):
        """
        Subparser - handles all CREATE TABLE requests.
        Throws KvException, QservAdminException.
        """
        l = len(tokens)
        if l == 2:
            (dbTbName, configFile) = tokens
            if '.' not in dbTbName:
                raise QservAdminException(QservAdminException.BAD_CMD,
                   "Invalid argument '%s', should be <dbName>.<tbName>" % dbTbName)
            (dbName, tbName) = dbTbName.split('.')
            options = self._fetchOptionsFromConfigFile(configFile)
            options = self._processTbOptions(dbName, options)
            self._impl.createTable(dbName, tbName, options)
        elif l == 3:
            (dbTbName, likeToken, dbTbName2) = tokens
            if likeToken.upper() != 'LIKE':
                raise QservAdminException(QservAdminException.BAD_CMD,
                                    "Expected 'LIKE', found: '%s'." % tokens[2])
            if '.' not in dbTbName:
                raise QservAdminException(QservAdminException.BAD_CMD,
                   "Invalid argument '%s', should be <dbName>.<tbName>" % dbTbName)
            (dbName, tbName) = dbTbName.split('.')
            if '.' not in dbTbName2:
                raise QservAdminException(QservAdminException.BAD_CMD,
                   "Invalid argument '%s', should be <dbName>.<tbName>" % dbTbName2)
            (dbName2, tbName2) = dbTbName2.split('.')
            self._impl.createTableLike(dbName, tbName, dbName2, tbName2,
                                       options)
        else:
            raise QservAdminException(QservAdminException.BAD_CMD,
                                "Unexpected number of arguments.")

    def _parseCreateNode(self, tokens):
        """
        Subparser - handles all CREATE NODE requests.
        Throws KvException, QservAdminException.
        """
        if len(tokens) < 2:
            raise QservAdminException(QservAdminException.BAD_CMD,
                                "Unexpected number of arguments.")

        requiredKeys = self.requiredOpts['createNode']

        # parse command, 'type' and 'state' are optional, rest is required
        options = {'nodeName': tokens[0], 'type': 'worker', 'state': NodeState.ACTIVE}

        for key, value in self._parseKVList(tokens[1:], requiredKeys):
            options[key] = value

        # check that all required options are there
        self._checkExist(options, requiredKeys)

        # call CSS to do the rest, remap options to argument names
        options['nodeType'] = options['type']
        del options['type']
        self._impl.addNode(**options)

    def _parseDrop(self, tokens):
        """
        Subparser - handles all DROP requests.
        Throws KvException, QservAdminException.
        """
        t = tokens[0].upper()
        l = len(tokens)
        if t == 'DATABASE':
            if l != 2:
                raise QservAdminException(QservAdminException.BAD_CMD,
                                    "unexpected number of arguments")
            self._impl.dropDb(tokens[1])
        elif t == 'TABLE':
            raise QservAdminException(QservAdminException.NOT_IMPLEMENTED,
                                      "DROP TABLE")
        elif t == 'EVERYTHING':
            self._impl.dropEverything()
        else:
            raise QservAdminException(QservAdminException.BAD_CMD)

    def _parseDelete(self, tokens):
        """
        Subparser - handles all DELETE requests.
        Throws KvException, QservAdminException.
        """
        t = tokens[0].upper()
        l = len(tokens)
        if t == 'NODE':
            if l != 2:
                raise QservAdminException(QservAdminException.BAD_CMD,
                                    "unexpected number of arguments")
            self._impl.deleteNode(tokens[1])
        else:
            raise QservAdminException(QservAdminException.BAD_CMD)

    def _parseDump(self, tokens):
        """
        Subparser, handle all DUMP requests.
        """
        if len(tokens) > 2:
            raise QservAdminException(QservAdminException.WRONG_PARAM)
        t = tokens[0].upper()
        dest = tokens[1] if len(tokens) == 2 else None
        if t == 'EVERYTHING':
            self._impl.dumpEverything(dest)
        else:
            raise QservAdminException(QservAdminException.BAD_CMD)

    def _justExit(self, tokens):
        raise SystemExit()

    def _printHelp(self, tokens):
        """
        Print available commands.
        """
        print self._supportedCommands

    def _parseRelease(self, tokens):
        """
        Subparser - handles all RELEASE requests.
        """
        raise QservAdminException(QservAdminException.NOT_IMPLEMENTED, "RELEASE")

    def _restore(self, tokens):
        """
        Restore all data from the file fileName.
        """
        self._impl.restore(tokens[0])

    def _parseShow(self, tokens):
        """
        Subparser, handle all SHOW requests.
        """
        t = tokens[0].upper()
        if t == 'DATABASES':
            self._impl.showDatabases()
        elif t == 'NODES':
            self._parseShowNodes()
        else:
            raise QservAdminException(QservAdminException.BAD_CMD)

    def _parseShowNodes(self):
        """
        Subparser, handle all SHOW NODES requests.
        """

        nodes = self._impl.getNodes()
        if not nodes:
            print "No nodes defined in CSS"
        for node in sorted(nodes.keys()):
            options = sorted(nodes[node].items())
            options = ["{0}={1}".format(k, v) for k, v in options]
            print "{0} {1}".format(node, ' '.join(options))

    def _parseUpdate(self, tokens):
        """
        Subparser - handles all UPDATE requests.
        """
        t = tokens[0].upper()
        if t == 'NODE':
            self._parseUpdateNode(tokens[1:])
        else:
            raise QservAdminException(QservAdminException.BAD_CMD)

    def _parseUpdateNode(self, tokens):
        """
        Subparser - handles all UPDATE NODE requests.
        Throws KvException, QservAdminException.
        """
        if len(tokens) < 2:
            raise QservAdminException(QservAdminException.BAD_CMD,
                                "Unexpected number of arguments.")

        requiredKeys = self.requiredOpts['updateNode']

        # parse key=value pairs
        options = {'nodes': [tokens[0]]}

        for key, value in self._parseKVList(tokens[1:], requiredKeys):
            options[key] = value

        # check that all required options are there
        self._checkExist(options, requiredKeys)

        # call CSS to do the rest, remap options to argument names
        self._impl.setNodeState(**options)

    def _parseKVList(self, tokens, possibleKeys=None):
        """
        Parse the series of key=value strings, returns the list of (key, value)
        tuples. If possibleKeys is not None then keys are checked agains that
        list, exception is raised if key is not in the list.
        """
        kvList = []
        for opt in tokens:
            if '=' not in opt:
                raise QservAdminException(QservAdminException.BAD_CMD,
                   "Invalid argument '%s', should be <key>=<value>" % opt)
            key, value = opt.split('=', 1)

            if possibleKeys is not None and key not in possibleKeys:
                raise QservAdminException(QservAdminException.BAD_CMD,
                   "Unrecognized option key '%s', possible keys: %s" % \
                   (key, ' '.join(possibleKeys)))

            kvList.append((key, value))

        return kvList

    def _createDb(self, dbName, configFile):
        """
        Create database through config file.
        """
        self._logger.info("Creating db '%s' using config '%s'" % \
                              (dbName, configFile))
        options = self._fetchOptionsFromConfigFile(configFile)
        self._impl.createDb(dbName, options)

    def _fetchOptionsFromConfigFile(self, fName):
        """
        Read config file <fName> for createDb and createTable command,
        and return key-value pair dictionary (flat, e.g., sections are
        ignored.)
        """
        if not os.path.exists(fName):
            raise QservAdminException(QservAdminException.CONFIG_NOT_FOUND, fName)
        if not os.access(fName, os.R_OK):
            raise QservAdminException(QservAdminException.AUTH_PROBLEM, fName)
        config = ConfigParser.ConfigParser()
        config.optionxform = str # case sensitive
        config.read(fName)
        xx = {}
        for section in config.sections():
            for option in config.options(section):
                xx[option] = config.get(section, option)
        return xx

    def _checkExist(self, opts, required):
        for r in required:
            if r not in opts:
                raise QservAdminException(QservAdminException.MISSING_PARAM, r)

    def _processDbOptions(self, opts):
        """
        Validate options used by createDb, add default values for missing
        parameters.
        """
        self._setDefault(opts, "partitioning", "0")
        self._setDefault(opts, "clusteredIndex", "")
        self._setDefault(opts, "storageClass", "L2", force=True)
        # these are required options for createDb
        self._checkExist(opts, CommandParser.requiredOpts["createDb"])
        if opts["partitioning"] != "0":
            if opts["partitioningStrategy"].lower() == "sphBox".lower():
                self._checkExist(opts,
                                 CommandParser.requiredOpts["createDbSphBox"])
            else:
                raise QservAdminException(QservAdminException.WRONG_PARAM,
                                          opts["partitioningStrategy"])
        return opts

    def _setDefault(self, opts, key, defaultValue, force=False):
        if not opts.has_key(key):
            self._logger.info(
                "param '" + key + "' not found, will use default: " + str(defaultValue))
            opts[key] = defaultValue
        elif force and opts[key] != defaultValue:
            raise QservAdminException(
                QservAdminException.WRONG_PARAM,
                "Got '%s' expected '%s'" % (opts[key], defaultValue))

    def _processTbOptions(self, dbName, opts):
        """
        Validate options used by createTable, add default values for missing
        parameters.
        """
        self._setDefault(opts, "partitioning", "0")
        self._setDefault(opts, "clusteredIndex", "NULL")
        self._setDefault(opts, "match", "0")
        self._setDefault(opts, "isView", "0")
        if opts.has_key("schemaFile"):
            if opts.has_key("schema"):
                self._logger.info("Both schema and schemaFile specified. " +
                                  "Ignoring schemaFile")
            else: # Create schema field from file.
                self._logger.info("Importing schema from " + opts["schemaFile"])
                data = open(opts["schemaFile"]).read()
                # strip CREATE TABLE and all table options (see dataLoader.py)
                i = data.find('(')
                j = data.rfind(')')
                opts["schema"] = data[i:j + 1]

        elif not opts.has_key("schema"):
            raise QservAdminException(QservAdminException.MISSING_PARAM,
                                      "Missing 'schema' or 'schemaFile'")
        # these are required options for createTable
        self._checkExist(opts, CommandParser.requiredOpts["createTable"])
        if opts["partitioning"] != "0":
            self._processTbPartitionOpts(dbName, opts)
        return opts

    def _processTbPartitionOpts(self, dbName, opts):
        # only sphBox allowed
        self._checkExist(opts,
                         CommandParser.requiredOpts["createTableSphBox"])
        if opts.get("match", "0") != "0":
            pass # Defer to qservAdmin to check opts
        else:
            # Fill-in dirDb
            if "dirDb" not in opts:
                opts["dirDb"] = dbName
            # Fill-in dirTable
            if "dirTable" not in opts:
                opts["dirTable"] = opts["tableName"]
            if opts["tableName"] == opts["dirTable"]:
                self._checkExist(opts, CommandParser.requiredOpts["createTableDir"])
            else: # child table
                self._checkExist(opts, CommandParser.requiredOpts["createChildTable"])
        return opts


    def _initLogging(self):
        self._logger = logging.getLogger("QADM")
        kL = os.getenv('KAZOO_LOGGING')
        if kL: logging.getLogger("kazoo.client").setLevel(int(kL))
        if kL: self._logger.setLevel(int(kL))

####################################################################################
class WordCompleter(object):
    """
    Set auto-completion for commonly used words.
    """
    def __init__(self, words):
        self.words = words

    def complete(self, text, state):
        results = [word+' ' for word in self.words
                   if word.startswith(text.upper())] + [None]
        return results[state]

readline.parse_and_bind("tab: complete")
words = ['CONFIG',
         'CREATE',
         'DATABASE',
         'DATABASES',
         'DROP',
         'DUMP',
         'INTO',
         'LIKE',
         'LOAD',
         'RELEASE',
         'SHOW',
         'TABLE']
completer = WordCompleter(words)
readline.set_completer(completer.complete)

####################################################################################

def getOptions():
    usage = \
"""

NAME
        qserv-admin - the client program for Central State System (CSS)

SYNOPSIS
        qserv-admin [OPTIONS] [command ...]

OPTIONS
   -v
        Verbosity threshold. Logging messages which are less severe than
        provided will be ignored. Expected value range: 0=50: (CRITICAL=50,
        ERROR=40, WARNING=30, INFO=20, DEBUG=10). Default value is ERROR.
   -f
        Name of the output log file. If not specified, the output goes to stderr.
   -c
        Connection information (hostname:port)

If commands are provided then they are executed and application will exit.
Every argument is treated as separate command, use quotes if command contains
spaces or special characters. If any command fails then all other commands are
ignored and non-zero code is returned to caller.
"""

    parser = OptionParser(usage=usage)
    parser.add_option("-v", dest="verbT", default=40) # default is ERROR
    parser.add_option("-f", dest="logF", default=None)
    parser.add_option("-c", dest="connI", default = '127.0.0.1:12181')
                      # default for kazoo (single node, local))
    (options, args) = parser.parse_args()
    if int(options.verbT) > 50: options.verbT = 50
    if int(options.verbT) <  0: options.verbT = 0
    return (int(options.verbT), options.logF, options.connI, args)

####################################################################################
def main():

    (verbosity, logFileName, connInfo, args) = getOptions()

    # configure logging
    if logFileName:
        logging.basicConfig(
            filename=logFileName,
            format='%(asctime)s %(name)s %(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=verbosity)
    else:
        logging.basicConfig(
            format='%(asctime)s %(name)s %(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=verbosity)

    parser = CommandParser(connInfo)
    if args:
        for cmd in args:
            # strip semicolons just in case
            cmd = cmd.strip().rstrip(';')
            try:
                parser.parse(cmd)
            except (QservAdminException, KvException) as e:
                print "ERROR: ", e.__str__()
                sys.exit(1)
    else:
        try:
            # wait for commands and process
            parser.receiveCommands()
        except(KeyboardInterrupt, SystemExit, EOFError):
            print ""

if __name__ == "__main__":
    main()
