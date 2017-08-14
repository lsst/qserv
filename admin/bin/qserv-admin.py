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
from __future__ import absolute_import, division, print_function

# standard library imports
from builtins import input
import configparser
import logging
from argparse import ArgumentParser
import os
import re
import readline
import sys

# local imports
import lsst.log
from lsst.qserv import css
from lsst.qserv.css import cssConfig


# exception classes used in this utility

class _ToolError(RuntimeError):
    pass


class _NotImplementedError(_ToolError):

    def __init__(self, cmd):
        _ToolError.__init__(self, "Command not implemented: {}".format(cmd))


class _IllegalCommandError(_ToolError):

    def __init__(self, cmd):
        _ToolError.__init__(self, "Unexpected command or option, see HELP for details: {}".format(cmd))


class _WrongParamError(_ToolError):

    def __init__(self, parm):
        _ToolError.__init__(self, "Unrecognized parameter: {}".format(parm))


class _MissingParamError(_ToolError):

    def __init__(self, parm):
        _ToolError.__init__(self, "Missing parameter: {}".format(parm))


class _MissingConfigError(_ToolError):

    def __init__(self, config):
        _ToolError.__init__(self, "Config file not found: {}".format(config))


class _UnreadableConfigError(_ToolError):

    def __init__(self, config):
        _ToolError.__init__(self, "Can't access the config file: {}".format(config))


class CommandParser(object):
    """
    Parse commands and calls appropriate function from qservAdmin.
    """

    requiredOpts = {
        "createDb": [  # Db opts
            "storageClass",
            "partitioning",
            "partitioningStrategy"],
        "createDbSphBox": [
            "nStripes",
            "nSubStripes",
            "overlap"],
        "createTable": [
            "tableName",
            "partitioning",
            "schema",  # "schemaFile" can be read into "schema"
            "clusteredIndex",
            "match",
            "isView"],
        "createTableSphBox": [
            "dirTable",
            # "overlap" # overlap should inherit from db if unspecified
        ],
        "createTableDir": [
            "dirColName",
            "latColName",
            "lonColName"],
        "createChildTable": [
            "dirColName"],
        "createChildTableExtra": [
            "latColName",
            "lonColName"],
        "createTableMatch": [
            "dirTable1",
            "dirColName1",
            "dirTable2",
            "dirColName2"],
        "createNode": [
            "type",
            "host",
            "port",
            "state"],
        "updateNode": [
            "state"],
    }

    def __init__(self, connInfo):
        """
        Initialize shared metadata, including list of supported commands.

        @param connInfo     Connection information.
        """
        self._initLogging()
        self._funcMap = {
            'CREATE': self._parseCreate,
            'DELETE': self._parseDelete,
            'DROP': self._parseDrop,
            'DUMP': self._parseDump,
            'EXIT': self._justExit,
            'HELP': self._printHelp,
            'QUIT': self._justExit,
            'RELEASE': self._parseRelease,
            'RESTORE': self._restore,
            'SHOW': self._parseShow,
            'UPDATE': self._parseUpdate
        }
        config = cssConfig.configFromUrl(connInfo)
        self._css = css.CssAccess.createFromConfig(config, "")
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
    DROP TABLE <dbName>.<tableName>;
    DROP EVERYTHING;
    DUMP EVERYTHING [<outFile>];
    RESTORE <inFile>;
    SHOW DATABASES;
    SHOW NODES;
    CREATE CHUNK <dbName>.<tableName> <chunk> <nodeName>;
    DELETE CHUNK <dbName>.<tableName> <chunk>;
    SHOW CHUNKS <dbName>.<tableName>;
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
            line = input(prompt).strip()
            cmd += line + ' '
            if prompt:
                prompt = self._prompt if line.endswith(';') else "~ "
            while re.search(';', cmd):
                pos = cmd.index(';')
                try:
                    self.parse(cmd[:pos])
                except (_ToolError, css.CssError) as exc:
                    self._logger.error("%s", exc)
                cmd = cmd[pos+1:]

    def parse(self, cmd):
        """
        Parse, and dispatch to subparsers based on first word. Raise
        exceptions on errors.
        """
        cmd = cmd.strip()
        # ignore empty commands, these can be generated by typing ;;
        if len(cmd) == 0:
            return
        tokens = cmd.split()
        t = tokens[0].upper()
        if t in self._funcMap:
            self._funcMap[t](tokens[1:])
        else:
            raise _NotImplementedError(cmd)

    @staticmethod
    def _parseDbTable(val):
        """
        Parse the dot-separate paremeter specifying the database and
        the table names. Example:

           Db1.Table2

        Return a sequence of two names: (<db>,<table>)
        Throw an exception if a wrong sring passed into the method.
        """
        tokens = val.split('.')
        if len(tokens) != 2:
            raise _WrongParamError(val)

        return tokens

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
        elif t == 'CHUNK':
            self._parseCreateChunk(tokens[1:])
        else:
            raise _IllegalCommandError(t)

    def _parseCreateDatabase(self, tokens):
        """
        Subparser - handles all CREATE DATABASE requests.
        Throws css.CssError, _ToolError
        """
        l = len(tokens)
        if l == 2:
            dbName = tokens[0]
            configFile = tokens[1]
            options = self._fetchOptionsFromConfigFile(configFile)
            options = self._processDbOptions(options)
            striping = css.StripingParams(int(options['nStripes']), int(options['nSubStripes']),
                                          0, float(options['overlap']))
            self._css.createDb(dbName, striping, options['storageClass'], 'RELEASED')
        elif l == 3:
            if tokens[1].upper() != 'LIKE':
                raise _IllegalCommandError("Expected 'LIKE', found: '{0}'.".format(tokens[1]))
            dbName = tokens[0]
            dbName2 = tokens[2]
            self._css.createDbLike(dbName, dbName2)
        else:
            raise _IllegalCommandError("Unexpected number of arguments.")

    def _parseCreateTable(self, tokens):
        """
        Subparser - handles all CREATE TABLE requests.
        Throws css.CssError, _ToolError.
        """
        l = len(tokens)
        if l == 2:
            (dbTbName, configFile) = tokens
            dbName, tbName = CommandParser._parseDbTable(dbTbName)
            options = self._fetchOptionsFromConfigFile(configFile)
            options = self._processTbOptions(dbName, options)
            if options.get('match', False):
                matchParams = css.MatchTableParams(options['dirTable1'], options['dirColName1'],
                                                   options['dirTable2'], options['dirColName2'],
                                                   options['flagColName'])
                self._css.createMatchTable(dbName, tbName, options['schema'], matchParams)
            else:
                if 'dirTable' in options:
                    # partitioned table
                    params = css.PartTableParams(options.get('dirDb', ""), options['dirTable'],
                                                 options['dirColName'], options['latColName'],
                                                 options['lonColName'], options.get('overlap', 0.0),
                                                 True, bool(int(options['subChunks'])))
                    sParams = css.ScanTableParams(
                        bool(options.get('lockInMem', False)), int(options.get('scanRating', 0)))
                else:
                    params = css.PartTableParams()
                    sParams = css.ScanTableParams()
                self._css.createTable(dbName, tbName, options['schema'], params, sParams)
        else:
            raise _IllegalCommandError("Unexpected number of arguments.")

    def _parseCreateNode(self, tokens):
        """
        Subparser - handles all CREATE NODE requests.
        Throws css.CssError, _ToolError.
        """
        if len(tokens) < 2:
            raise _IllegalCommandError("Unexpected number of arguments.")

        requiredKeys = self.requiredOpts['createNode']

        # parse command, 'type' and 'state' are optional, rest is required
        options = {'nodeName': tokens[0], 'type': 'worker', 'state': "ACTIVE"}

        for key, value in self._parseKVList(tokens[1:], requiredKeys):
            options[key] = value

        # check that all required options are there
        self._checkExist(options, requiredKeys)

        # call CSS to do the rest, remap options to argument names
        params = css.NodeParams(options['type'], options['host'],
                                int(options.get('port', 0)), options['state'])
        self._css.addNode(options['nodeName'], params)

    def _parseCreateChunk(self, tokens):
        """
        Subparser - handles all CREATE CHUNK requests.
        Throws css.CssError, _ToolError.
        """
        if len(tokens) != 3:
            raise _IllegalCommandError("Unexpected number of arguments.")

        (dbTbName, chunkIdStr, nodeName) = tokens
        if '.' not in dbTbName:
            raise _IllegalCommandError("Invalid argument '{0}', should be <dbName>.<tbName>".format(dbTbName))

        dbName, tbName = CommandParser._parseDbTable(dbTbName)

        if not chunkIdStr.isdigit():
            raise _IllegalCommandError(
                "Invalid argument '{0}', should be chunk identifier".format(chunkIdStr))

        chunkId = int(chunkIdStr)

        self._css.addChunk(dbName, tbName, chunkId, (nodeName,))

    def _parseDrop(self, tokens):
        """
        Subparser - handles all DROP requests.
        Throws css.CssError, _ToolError.
        """
        t = tokens[0].upper()
        l = len(tokens)
        if t == 'DATABASE':
            if l != 2:
                raise _IllegalCommandError("unexpected number of arguments")
            self._css.dropDb(tokens[1])
        elif t == 'TABLE':
            if l != 2:
                raise _IllegalCommandError("unexpected number of arguments")
            if '.' not in tokens[1]:
                raise _IllegalCommandError(
                    "Invalid argument '{0}', should be <dbName>.<tbName>".format(tokens[1]))
            dbName, tbName = CommandParser._parseDbTable(tokens[1])
            self._css.dropTable(dbName, tbName)
        elif t == 'EVERYTHING':
            raise _NotImplementedError(t)
        else:
            raise _IllegalCommandError(t)

    def _parseDelete(self, tokens):
        """
        Subparser - handles all DELETE requests.
        Throws css.CssError, _ToolError.
        """
        t = tokens[0].upper()
        l = len(tokens)
        if t == 'NODE':
            if l != 2:
                raise _IllegalCommandError("unexpected number of arguments")
            self._css.deleteNode(tokens[1])
        elif t == 'CHUNK':
            self._deleteChunk(tokens[1:])
        else:
            raise _IllegalCommandError(t)

    def _deleteChunk(self, tokens):
        """
        Subparser - handles all DELETE CHUNK requests.
        Throws css.CssError, _ToolError.
        """
        if len(tokens) != 2:
            raise _IllegalCommandError("unexpected number of arguments")

        dbTbName, chunkIdStr = tokens

        if '.' not in dbTbName:
            raise _IllegalCommandError("Invalid argument '{0}', should be <dbName>.<tbName>".format(dbTbName))

        dbName, tbName = CommandParser._parseDbTable(dbTbName)

        if not chunkIdStr.isdigit():
            raise _IllegalCommandError("Invalid argument '{0}', should be a chunk number".format(chunkIdStr))

        chunkId = int(chunkIdStr)

        self._css.deleteChunk(dbName, tbName, chunkId)

    def _parseDump(self, tokens):
        """
        Subparser, handle all DUMP requests.
        """
        if len(tokens) > 2:
            raise _IllegalCommandError("unexpected number of arguments")
        t = tokens[0].upper()
        if len(tokens) == 2:
            # this may throw
            dest = open(tokens[1], 'w')
        else:
            dest = sys.stdout
        if t == 'EVERYTHING':
            res = self._css.getKvI().dumpKV()
            dest.write(res)
            dest.write("\n")
        else:
            raise _IllegalCommandError(t)

    def _justExit(self, tokens):
        raise SystemExit()

    def _printHelp(self, tokens):
        """
        Print available commands.
        """
        print(self._supportedCommands)

    def _parseRelease(self, tokens):
        """
        Subparser - handles all RELEASE requests.
        """
        raise _NotImplementedError("RELEASE")

    def _restore(self, tokens):
        """
        Restore all data from the file fileName.
        """
        raise _NotImplementedError("RESTORE")

    def _parseShow(self, tokens):
        """
        Subparser, handle all SHOW requests.
        """
        t = tokens[0].upper()
        if t == 'DATABASES':
            names = self._css.getDbNames()
            for name in names:
                print(name)
        elif t == 'NODES':
            self._parseShowNodes()
        elif t == 'CHUNKS':
            self._parseShowChunks(tokens[1:])
        else:
            raise _IllegalCommandError(t)

    def _parseShowNodes(self):
        """
        Subparser, handle all SHOW NODES requests.
        """
        nodes = self._css.getAllNodeParams()
        if not nodes:
            print("No nodes defined in CSS")
        for node, params in sorted(nodes.items()):
            print("{0} type={1} host={2} port={3} state={4}".format(node, params.type, params.host,
                                                                    params.port, params.state))

    def _parseShowChunks(self, tokens):
        """
        Subparser, handle all SHOW CHUNKS requests.
        """

        if len(tokens) != 1:
            raise _IllegalCommandError("Unexpected number of arguments.")

        (dbTbName,) = tokens
        if '.' not in dbTbName:
            raise _IllegalCommandError("Invalid argument '{0}', should be <dbName>.<tbName>".format(dbTbName))

        dbName, tbName = CommandParser._parseDbTable(dbTbName)

        chunks = self._css.getChunks(dbName, tbName)
        for chunk, nodes in chunks.items():
            print("chunk: {0}".format(chunk))
            for node in nodes:
                print("    worker node: {0}".format(node))

    def _parseUpdate(self, tokens):
        """
        Subparser - handles all UPDATE requests.
        """
        t = tokens[0].upper()
        if t == 'NODE':
            self._parseUpdateNode(tokens[1:])
        else:
            raise _IllegalCommandError(t)

    def _parseUpdateNode(self, tokens):
        """
        Subparser - handles all UPDATE NODE requests.
        Throws css.CssError, _ToolError.
        """
        if len(tokens) < 2:
            raise _IllegalCommandError("Unexpected number of arguments.")

        requiredKeys = self.requiredOpts['updateNode']
        options = {}
        for key, value in self._parseKVList(tokens[1:], requiredKeys):
            options[key] = value

        # check that all required options are there
        self._checkExist(options, requiredKeys)

        # call CSS to do the rest, remap options to argument names
        self._css.setNodeStatus(tokens[0], options['state'])

    def _parseKVList(self, tokens, possibleKeys=None):
        """
        Parse the series of key=value strings, returns the list of (key, value)
        tuples. If possibleKeys is not None then keys are checked agains that
        list, exception is raised if key is not in the list.
        """
        kvList = []
        for opt in tokens:
            if '=' not in opt:
                raise _IllegalCommandError("Invalid argument '{0}', should be <key>=<value>".format(opt))
            key, value = opt.split('=', 1)

            if possibleKeys is not None and key not in possibleKeys:
                raise _IllegalCommandError("Unrecognized option key '{0}', possible keys: {0}".format(
                    (key, ' '.join(possibleKeys))))

            kvList.append((key, value))

        return kvList

    def _fetchOptionsFromConfigFile(self, fName):
        """
        Read config file <fName> for createDb and createTable command,
        and return key-value pair dictionary (flat, e.g., sections are
        ignored.)
        """
        if not os.path.exists(fName):
            raise _MissingConfigError(fName)
        if not os.access(fName, os.R_OK):
            raise _UnreadableConfigError(fName)
        config = configparser.ConfigParser()
        config.optionxform = str  # case sensitive
        config.read(fName)
        xx = {}
        for section in config.sections():
            for option in config.options(section):
                xx[option] = config.get(section, option)
        return xx

    def _checkExist(self, opts, required):
        for r in required:
            if r not in opts:
                raise _MissingParamError(r)

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
                raise _WrongParamError(opts["partitioningStrategy"])
        return opts

    def _setDefault(self, opts, key, defaultValue, force=False):
        if key not in opts:
            self._logger.info("param '%s' not found, will use default: %s", key, defaultValue)
            opts[key] = defaultValue
        elif force and opts[key] != defaultValue:
            raise _WrongParamError("Got '{0}' expected '{1}'".format(opts[key], defaultValue))

    def _processTbOptions(self, dbName, opts):
        """
        Validate options used by createTable, add default values for missing
        parameters.
        """
        self._setDefault(opts, "partitioning", 0)
        self._setDefault(opts, "clusteredIndex", "NULL")
        self._setDefault(opts, "match", 0)
        self._setDefault(opts, "isView", 0)
        if "schemaFile" in opts:
            if "schema" in opts:
                self._logger.info("Both schema and schemaFile specified. " +
                                  "Ignoring schemaFile")
            else:  # Create schema field from file.
                self._logger.info("Importing schema from " + opts["schemaFile"])
                data = open(opts["schemaFile"]).read()
                # strip CREATE TABLE and all table options (see dataLoader.py)
                i = data.find('(')
                j = data.rfind(')')
                opts["schema"] = data[i:j + 1]

        elif "schema" not in opts:
            raise _MissingParamError("Missing 'schema' or 'schemaFile'")
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
            pass  # Defer to qservAdmin to check opts
        else:
            # Fill-in dirDb
            if "dirDb" not in opts:
                opts["dirDb"] = dbName
            # Fill-in dirTable
            if "dirTable" not in opts:
                opts["dirTable"] = opts["tableName"]
            if opts["tableName"] == opts["dirTable"]:
                self._checkExist(opts, CommandParser.requiredOpts["createTableDir"])
            else:  # child table
                self._checkExist(opts, CommandParser.requiredOpts["createChildTable"])

        return opts

    def _initLogging(self):
        self._logger = logging.getLogger("QADM")


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

#


def getOptions():
    description = \
        """
Client application for Central State System (CSS)

If commands are provided then they are executed and application will exit.
Every argument is treated as separate command, use quotes if command contains
spaces or special characters. If any command fails then all other commands are
ignored and non-zero code is returned to caller.

If invoked without command list then commands are read from standard input,
prompt is printed for each command if input comes from a terminal. Type 'help;'
to print the list of supported commands.
"""

    parser = ArgumentParser(description=description)
    parser.add_argument("-v", dest="verbosity", default="WARN", choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        help="Verbosity threshold, def: %(default)s")
    parser.add_argument("-c", dest="connection", default='mysql://qsmaster@127.0.0.1:13306/qservCssData',
                        help="CSS connection information, def: %(default)s")
    parser.add_argument('commands', metavar='commands', nargs='*', default=[],
                        help='list of commands to be executed')

    args = parser.parse_args()

    return args

#


def main():

    args = getOptions()

    # configure logging
    lsst.log.setLevel('', getattr(lsst.log, args.verbosity))

    # redirect Python logging to LSST logger
    pylgr = logging.getLogger()
    pylgr.addHandler(lsst.log.LogHandler())

    parser = CommandParser(args.connection)
    if args.commands:
        for cmd in args.commands:
            # strip semicolons just in case
            cmd = cmd.strip().rstrip(';')
            try:
                parser.parse(cmd)
            except (_ToolError, css.CssError) as e:
                print("ERROR: ", e)
                sys.exit(1)
    else:
        try:
            # wait for commands and process
            parser.receiveCommands()
        except(KeyboardInterrupt, SystemExit, EOFError):
            print()


if __name__ == "__main__":
    main()
