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

# startQMS.py -- This is a "driver" program that provides a
# command-line interface to start qserv metadata server.

from optparse import OptionParser
import os
import sys

from lsst.qserv.metadata import qms
from lsst.qserv.master import config


def main():    
    parser = OptionParser()

    parser.add_option("-c", "--config", dest="configFile", default=None,
                      help="Use config file. Can also be specified with\n" +
                      "%s as an environment variable." % config.envFilenameVar)
    (options, args) = parser.parse_args()

    confFile = options.configFile
    if confFile:
        if not os.access(confFile, os.R_OK):
            raise Exception("Can't read config file %s" % confFile)
        config.load(confFile)
    else:
        config.load()
    print "Configuration:"
    config.printTo(sys.stdout)

    qms.runServer()

if __name__ == '__main__':
    main()
