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
# The "client" module was designed to implement a client for the
# qserv metadata server.


# Standard Python imports
from optparse import OptionParser
import xmlrpclib

# Local package imports
from lsst.qserv.metadata import qmsInterface
from lsst.qserv.metadata.qmsStatus import QmsStatus
from lsst.qserv.metadata.qmsStatus import getErrMsg

qmsHost = "localhost"
qmsPort = 7082
defaultXmlPath = "x"

url = "http://%s:%d/%s" % (qmsHost, qmsPort, defaultXmlPath)
qms = xmlrpclib.Server(url)

def runEchoTest():
    echostring = "QMS test string echo back. 1234567890.()''?"
    ret = qms.echo(echostring)
    if ret != echostring:
        print "Expected %s, got %s" % (echostring, ret)
        return False
    print "Echo test passed"
    return True

def main():
    parser = OptionParser()
    parser.add_option("--qhelp", action="store_true", 
                      dest="qmsHelp", default=False, 
                      help="Get qms frontend help.")
    parser.add_option("--echoTest",
                      dest="echoTest", default=False,
                      help="Run echo test.")
    parser.add_option("--persInit",
                      dest="persInit", default=False,
                      help="Persistent initialization.")

    (options, args) = parser.parse_args()

    if options.qmsHelp:
        print qmsInterface.QmsInterface().help()
        return
    if options.echoTest:
        runEchoTest()
        return
    if options.persInit:
        ret = qms.persistentInit()
        if ret != QmsStatus.SUCCESS:
            print getErrMsg(ret)

        return

if __name__ == '__main__':
    main()
