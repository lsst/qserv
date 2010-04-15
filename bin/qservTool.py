#!/usr/bin/env python
import unittest
from optparse import OptionParser
import sys

# Package imports
from lsst.qserv.master import appInterface as app
from lsst.qserv.master import config

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
    else:
        print "No action specified.  Need --help ?"
        pass
    return

if __name__ == '__main__':
    main()
