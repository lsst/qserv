#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008-2014 AURA/LSST.
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

# startCzar.py -- This is a "driver" program that provides a
# command-line interface to start qserv or its tests.  Specifically,
# you can:
# * Run tests (invoke cases in testHintedParser.py and testAppInterface.py)
# * Start up a qserv frontend for testing.  Does not include starting
#   up the required xrootd/cmsd instances.

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import unittest
from optparse import OptionParser

# ----------------------------
# Imports for other modules --
# ----------------------------
import lsst.log as log

from lsst.qserv.czar.appTest import TestAppFunctions
from lsst.qserv.czar import server
from lsst.qserv.czar import app
from lsst.qserv.czar import client
from lsst.qserv.czar import config


def runParserTest():
    """Invokes the test cases in the lsst.qserv.czar.testparser module
    """
    suite = unittest.TestLoader().loadTestsFromTestCase(TestAppFunctions)
    unittest.TextTestRunner(verbosity=2).run(suite)
    pass

def runNamedTest(name):
    suite = unittest.TestSuite()
    suite.addTest(TestAppFunctions('test'+name))
    unittest.TextTestRunner(verbosity=2).run(suite)
    pass

def makeIndexes():
    log.warn("makeIndexes() called, but not implemented")
    pass

def main():
    parser = OptionParser()

    parser.add_option("-t", "--test", action="store_true",
                      dest="test", default=False,
                      help="Run tests instead of starting the server")
    parser.add_option("-T",
                      dest="testName", default=None, metavar="NAME",
                      help="Run a test named NAME.")
    parser.add_option("--sanity-client", action="store_true",
                      dest="sanityClient", default=False,
                      help="Sanity-check a running server.")
    parser.add_option("--index", action="store_true",
                      dest="makeIndex", default=False,
                      help="Rebuild indexes.")
    parser.add_option("-c", "--config", dest="configFile", default=None,
                      help="Use config file. Can also be specified with\n" +
                      "%s as an environment variable." % config.envFilenameVar)
    parser.add_option("-n", "--name", dest="czarName", default=None,
                      help="Czar name, used for registration in query metadata database, "
                      "by default host name and port number are used for czar name.")
    (options, args) = parser.parse_args()

    # Modifying options
    if options.configFile:
        config.load(options.configFile)
    else:
        config.load()

    # Configure logging
    logConfig = config.config.get('log', 'logConfig')
    if logConfig:
        log.configure(logConfig)
    else:
        log.configure()

    log.debug("Configuration:\n%s", config.toString())

    if options.test == True:
        # not working
        runParserTest()
        return
    elif options.testName:
        # not working
        runNamedTest(options.testName)
        return
    elif options.sanityClient:
        # not working
        client.runSanityClient()
        return
    elif options.makeIndex:
        # not working
        makeIndexes()
        return
    else:
        server.runServer(options.czarName)
    return

if __name__ == '__main__':
    main()
