#!/usr/bin/env python

#
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

# qservTool - a module for testing and performing
# development/administrative actions on qserv.
#
# Tasks that test specific parts of qserv code and do not involve
# end-to-end query processing should reside here.  Administrative and
# development actions performed on an existing qserv instance should
# also go here.  Some functionality from startQserv.py probably needs
# to be moved here.

import unittest
from optparse import OptionParser
import sys

# Package imports
from lsst.qserv.czar import appInterface as app
from lsst.qserv.czar import config
from lsst.qserv.czar.testHintedParser import TestHintedParser
from lsst.qserv.czar.testAppInterface import TestAppInterface

def main():

    parser = OptionParser()
    parser.add_option("--qhelp", action="store_true",
                      dest="appHelp", default=False,
                      help="Get qserv frontend help.")
    parser.add_option("-c", "--config", dest="configFile", default=None,
                      help="Use config file. Can also be specified with\n" +
                      "%s as an environment variable." % config.envFilenameVar)
    parser.add_option("--check",
                      dest="checkTaskId", default=None, metavar="ID",
                      help="Check status of task with id ID.")
    parser.add_option("-q", "--query",
                      dest="queryFile", default=None, metavar="FILENAME",
                      help="Invoke a query stored in FILENAME.\n" +
                      "'-' will read from standard input.")
    parser.add_option("--hintfile",
                      dest="queryHints", default=None, metavar="FILENAME",
                      help="Use FILENAME to get hints. Use with --query.")

    parser.add_option("--hintTest",
                      dest="hintTest", default=None, metavar="NAME",
                      help="Run a hintedParser test named NAME.")
    parser.add_option("--test",
                      dest="testName", default=None, metavar="NAME",
                      help="Run a appInterface test named NAME.")

    (options, args) = parser.parse_args()

    # Modifying options
    if options.configFile:
        config.load(options.configFile)
    else:
        config.load()
    print "Configuration:"
    config.printTo(sys.stdout)

    # Action options
    if options.appHelp == True:
        a = app.AppInterface()
        print a.help()
        return
    elif options.checkTaskId:
        a = app.AppInterface()
        print a.check(options.checkTaskId)
        return
    elif options.queryFile:
        q = options.queryFile
        a = app.AppInterface()
        # don't use hints right now.
        if q == "-":
            q = ""
            for l in sys.stdin:
                q += l
        else:
            q = open(options.queryFile).read()
        print a.query(q)
        return

    elif options.testName:
        suite = unittest.TestSuite()
        suite.addTest(TestAppInterface('test' + options.testName))
        unittest.TextTestRunner(verbosity=2).run(suite)

    elif options.hintTest:
        # from lsst.qserv.czar import testHintedParser
        # t = testHintedParser.TestHintedParser()
        suite = unittest.TestSuite()
        suite.addTest(TestHintedParser('test' + options.hintTest))
        unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        print "No action specified.  Need --help ?"
        pass
    return

if __name__ == '__main__':
    main()
