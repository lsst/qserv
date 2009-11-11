#!/usr/bin/env python
import unittest
from optparse import OptionParser

from lsst.qserv.master.testparser import TestAppFunctions
from lsst.qserv.master import server
from lsst.qserv.master import app


def runParserTest():
    """Invokes the test cases in the lsst.qserv.master.testparser module
    """
    suite = unittest.TestLoader().loadTestsFromTestCase(TestAppFunctions)
    unittest.TextTestRunner(verbosity=2).run(suite)
    pass

def runNamedTest(name):
    suite = unittest.TestSuite()
    suite.addTest(TestAppFunctions('test'+name))
    unittest.TextTestRunner(verbosity=2).run(suite)
    pass
                  

def resetTables():
    p = app.Persistence()
    p.activate()
    p.makeTables()
    pass

def main():    
    parser = OptionParser()
    parser.add_option("--reset-tables", action="store_true", 
                      dest="resettables", default=False, 
                      help="Reset tables instead of starting the server")
    parser.add_option("-t", "--test", action="store_true", 
                      dest="test", default=False, 
                      help="Run tests instead of starting the server")
    parser.add_option("-T",
                      dest="testName", default=None, metavar="NAME", 
                      help="Run a test named NAME.")

    (options, args) = parser.parse_args()

    if options.resettables == True:
        resetTables()
        return
    elif options.test == True:
        runParserTest()
        return
    elif options.testName:
        runNamedTest(options.testName)
        return
    else:
        server.runServer()
    return

if __name__ == '__main__':
    main()
