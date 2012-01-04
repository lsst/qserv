#!/usr/bin/python

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


import time

from twisted.web import xmlrpc, server, resource
from twisted.internet.threads import deferToThread


x = 0
def nextX():
    global x
    x = x+1
    if x > 3:
        x = 1
    return x


class Example(xmlrpc.XMLRPC):

    def xmlrpc_echo(self, x):
        return x


    def xmlrpc_submitQuery(self, query, conditions):
        print "query '", query, "' submitted, conditions: ", conditions
        #time.sleep(5)
        #print "done sleeping"
        x = nextX()
        return ["dummyResults%s" % x, "dummyResults%sLock" % x]

if __name__ == '__main__':
    from twisted.internet import reactor

    r = Example()
    root = resource.Resource()
    root.putChild("x", r)
    reactor.listenTCP(7080, server.Site(root))
    reactor.run()


