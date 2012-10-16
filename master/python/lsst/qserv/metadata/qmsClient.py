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

    def parseOptions(self):
        usage = """
NAME
       qmsClient - the client program for Qserv Metadata Server (QMS)

SYNOPSIS:
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

  createDb
        Creates metadata about new database to be managed 
        by qserv. Arguments: <dbName> <configFile>

  listDbs
        Lists database names registered for qserv use.

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

        if len(args) < 1:
            parser.error("No command given")
        cmd = "_cmd_" + args[0]
        if not hasattr(self, cmd):
            parser.error("Unrecognized command: " + args[0])
        del args[0]
        return getattr(self, cmd), options, args

    ############################################################################
    ##### user-facing commands
    ############################################################################
    def _cmd_installMeta(self, options, args):
        qms = self._connectToQMS()
        if qms is None:
            return
        ret = qms.installMeta()
        if ret != QmsStatus.SUCCESS: print getErrMsg(ret)
        else: print "Metadata successfully installed."

    def _cmd_destroyMeta(self, options, args):
        qms = self._connectToQMS()
        if qms is None:
            return
        ret = qms.destroyMeta()
        if ret != QmsStatus.SUCCESS: print getErrMsg(ret)
        else: print "All metadata destroyed!"

    def _cmd_createDb(self, options, args):
        if len(args) != 2:
            print "'createDb' requires two arguments: <dbName> <configFile>"
            return
        (dbName, confFile) = args
        kvPairs = self._readCreateDbConfigFile(confFile)
        if kvPairs is None:
            return
        qms = self._connectToQMS()
        if qms is None:
            return
        print "createDb %s, options are: " % dbName
        print kvPairs
        ret = qms.createDb(dbName)
        #if ret != QmsStatus.SUCCESS: print getErrMsg(ret)
        print "not fully implemented yet"

    def _cmd_listDbs(self, options, args):
        print "listDbs, dburl is:", options, conn, " not implemented"

    ############################################################################
    ##### config file
    ############################################################################
    def _readCreateDbConfigFile(self, fName):
        errMsg = "Problems with config file '%s':" % fName
        if not os.access(fName, os.R_OK):
            print errMsg, "specified config file '%s' not found." % fName
            return
        section = "partitioning"
        config = ConfigParser.ConfigParser()
        config.read(fName)
        if not config.has_section(section):
            print errMsg, "section '%s' not found" % section
            return
        partStrategy = config.get(section, "partitioningstrategy")
        if partStrategy is None:
            print errMsg, "required option 'PartitionigStrategy' not found."
            return
        if partStrategy == "sphBox":
            for option in config.options(section):
                if option == "nstripes":
                    nStripes = config.get(section, option)
                elif option == "nsubstripes":
                    nSubStripes = config.get(section, option)
                elif option == "defaultoverlap_fuzziness":
                    defOvF = config.get(section, option)
                elif option == "defaultoverlap_nearneighbor":
                    defOvN = config.get(section, option)
                elif option == "partitioningstrategy":
                    pass
                else:
                    print errMsg, "unrecognized option '%s'." % option
                    return
        elif pStrategy == "None":
            pass # no options here yet
        return config.items(section)

    ############################################################################
    ##### connection to QMS
    ############################################################################
    def _connectToQMS(self):
        (host, port, user, pwd) = self._getConnInfo()
        if host is None or port is None or user is None or pwd is None:
            return False
        print "Using connection: %s:%s, %s,pwd=%s" % (host, port, user, pwd)
        url = "http://%s:%d/%s" % (host, port, self._defaultXmlPath)
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
            print "Failed to parse: %s, expected: user@host:port" % options.conn
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

        if not os.access(self._dotFileName, os.R_OK):
            return None
        f = open(cFile, "r")
        lines = f.readlines()
        f.close()
        if len(lines) < 1:
            return None
        if len(lines) != 2:
            return None
        cUrl = lines[0].strip()
        cPwd = lines[1].strip('\n')
        if cUrl != url:
            return None
        return cPwd

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

################################################################################
#### main
################################################################################
if __name__ == '__main__':
    c = Client()
    (cmd, options, args) = c.parseOptions()
    cmd(options, args)

