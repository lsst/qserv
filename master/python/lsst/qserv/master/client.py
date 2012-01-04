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

# The "client" module was designed to implement a qserv client.  This
# code should be used for:
# * sanity-checking and testing of the XML-RPC and HTTP top-level
# interfaces to qserv. 
# * modeling example interactions with qserv that do not use the
# mysqlproxy interface.
#
# This code is unfinished and not in active use. (7/27/2010: danielw)

# Standard Python imports
import xmlrpclib

# Local package imports
import server 

def runSanityClient(): 
    """Top-level function that checks the running server.
    Meant to be invoked from the driver program."""
    url = "http://localhost:%d/%s" % (server.defaultPort, server.defaultXmlPath)
    res = sanityCheckServer(url)
    print "Sanity check okay? ", str(res)


def sanityCheckServer(url):
    echostring = "QSERV test string echo back. 1234567890.()''?"
    s = xmlrpclib.Server(url)
    ret = s.echo(echostring)
    if ret != echostring:
        print "Expected %s, got %s" % (echostring, ret)
        return False
    return True
