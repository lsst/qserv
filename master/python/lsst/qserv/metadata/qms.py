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

# qms.py : This module implements Qserv Metadata Server (HTTP and 
# XML-RPC interfacing logic using the Twisted networking library.
# It exposes the functionality from the QmsInterface class.

# Standard Python imports
from itertools import ifilter
import logging 
import os
import sys
import time

# Twisted imports
from twisted.internet import reactor
import twisted.web.resource 
import twisted.web.static
import twisted.web
import twisted.web.server 
from twisted.web import xmlrpc

# Package imports
from qmsInterface import QmsInterface
from lsst.qserv.master import config

# Module settings
defaultPort = 8001
defaultPath = "c"
defaultXmlPath = "qms"

class ClientResource(twisted.web.resource.Resource):
    def __init__(self, interface):
        twisted.web.resource.Resource.__init__(self)
        self.interface = interface
        
    def render_GET(self, request):
        print "rendering get"
        if "action" in request.args:
            action = request.args["action"][0]
            flattenedargs = dict(map(lambda t:(t[0],t[1][0]), request.args.items()))
            return self.interface.execute(action, flattenedargs, lambda x:None)
        return "Error, no action found"
    
    def getChild(self, name, request):
        print "trying to get child ", name
        if name == '':
            return self

        return twisted.web.resource.Resource.getChild(
            self, name, request)

def printdir(obj):
    for f in dir(obj):
        print "%s -- %s" % (f, getattr(obj,f))

class FunctionResource(twisted.web.resource.Resource):
    isLeaf = True
    def __init__(self, func=lambda args:"Null function"):
        twisted.web.resource.Resource.__init__(self)
        self.function = func
        
    def render_GET(self, request):
        print "rendering get, args are", request.args
        print "postpath is", request.postpath
        return self.function(request)
    
    def getChild(self, name, request):
        if name == '':
            return self
        return twisted.web.resource.Resource.getChild(
            self, name, request)

class XmlRpcInterface(xmlrpc.XMLRPC):
    def __init__(self, qmsInterface):
        xmlrpc.XMLRPC.__init__(self)
        self.qmsInterface = qmsInterface
        self._bindQmsInterface()

    def _bindQmsInterface(self):
        """Import the QmsInterface functions for publishing."""
        prefix = 'xmlrpc'
        map(lambda x: setattr(self, "_".join([prefix,x]), 
                              getattr(self.qmsInterface, x)), 
            self.qmsInterface.publishable)
        print "contents:"," ".join(filter(lambda x:"xmlrpc_" in x, dir(self)))

    def xmlrpc_echo(self, echostr):
        "Echo a string back (useful for sanity checking)."
        s = str(echostr)
        return s


class HttpInterface:
    def __init__(self, qmsInterface):
        self.qmsInterface = qmsInterface
        okname = ifilter(lambda x: "_" not in x, dir(self))
        self.publishable = filter(lambda x: hasattr(getattr(self,x), 'func_doc'), 
                                  okname)

    def help(self, req):
        """A brief help message showing available commands"""
        r = "" ## self._handyHeader()
        r += "\n<pre>available commands:\n"
        sorted =  map(lambda x: (x, getattr(self, x)), self.publishable)
        sorted.sort()
        for (k,v) in sorted:
            r += "%-20s : %s\n" %(k, v.func_doc)
        r += "</pre>\n"
        return r


class Master:
    def __init__(self):
        try:
            cfg = config.config
            self.port = cfg.getint("qmsFrontend","port")
        except:
            print "Bad or missing port for server. Using", defaultPort
            self.port = defaultPort
        pass

    def listen(self):
        root = twisted.web.resource.Resource()
        twisted.web.static.loadMimeTypes() # load from /etc/mime.types
        
        mi = QmsInterface()
        c = HttpInterface(mi)
        xml = XmlRpcInterface(mi)
        # not sure I need the sub-pat http interface
        root.putChild(defaultPath, ClientResource(c))
        root.putChild(defaultXmlPath, xml)

        # publish the client functions
        for x in c.publishable:
            root.putChild(x, FunctionResource(getattr(c, x)))

        # init listening
        reactor.listenTCP(self.port, twisted.web.server.Site(root))

        print "Starting Qserv Metadata Server (QMS) on port: %d" % self.port
        reactor.run()
   
def runServer():
    m = Master()
    m.listen()

if __name__ == "__main__":
    runServer()
