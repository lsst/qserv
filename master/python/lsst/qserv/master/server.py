#
# LSST Data Management System
# Copyright 2008-2013 LSST Corporation.
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

# server.py : This module implements the HTTP and XML-RPC interfacing
# logic using the Twisted networking library.  The XML-RPC interface
# exposes functionality from the AppInterface class, while the HTTP
# interface has not yet been updated to do the same.

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
from appInterface import AppInterface
import config
from lsst.qserv.meta.status import QmsException

# Module settings
defaultPort = 8000
defaultPath = "c"
defaultXmlPath = "x"
concurrencyLimit = 50


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
    def __init__(self, appInterface):
        xmlrpc.XMLRPC.__init__(self)
        self.appInterface = appInterface
        self._bindAppInterface()
        pass

    def _bindAppInterface(self):
        """Import the appInterface functions for publishing."""
        prefix = 'xmlrpc'
        map(lambda x: setattr(self, "_".join([prefix,x]),
                              getattr(self.appInterface, x)),
            self.appInterface.publishable)
        print "contents:"," ".join(filter(lambda x:"xmlrpc_" in x, dir(self)))
        pass

    def xmlrpc_echo(self, echostr):
        "Echo a string back (useful for sanity checking)."
        s = str(echostr)
        return s


class HttpInterface:
    def __init__(self, appInterface):
        self.appInterface = appInterface
        okname = ifilter(lambda x: "_" not in x, dir(self))
        self.publishable = filter(lambda x: hasattr(getattr(self,x), 'func_doc'),
                                  okname)
    def query(self, req):
        "Issue a query. Params: q=querystring, h=hintstring"
        print req
        flatargs = dict(map(lambda t:(t[0],t[1][0]), req.args.items()))
        #printdir(arg)

        if 'q' in req.args:
            h = flatargs.get('h', None)
            resp = ""
            if h:
                warning = "FIXME: No unmarshalling code for hints."
                warning += "WARNING, no partitions will be used."
                resp += warning
                print warning
                h = None

            taskId = self.appInterface.query(flatargs('q'), h)
            resp += "Server processed, q='" + flatargs['q'] + "' your id is %d" % (taskId)
            return resp
        else:
            return "no query in string, try q='select...'"

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


    def check(self, req):
        "Check status of query or task. Params: h=handle"
        print req
        key = 'id'
        flatargs = dict(map(lambda t:(t[0],t[1][0]), req.args.items()))
        if key in req.args and int(flatargs[key]):
            res = self.appInterface.check(flatargs[key])
            resp = ["checking status of query with handle %s" % flatargs[key],
                    " would return \%done got %d", res]
            return "\n".join(resp)

        else:
            return "\n".join([ "no handle in string, try %s=<taskId> " % key,
                               "where <taskId> was returned from the initial query."])
    def results(self, req):
        "Get results location for a query or task. Params: h=handle"
        print req
        flatargs = dict(map(lambda t:(t[0],t[1][0]), req.args.items()))
        key = 'id'
        if key in req.args:
            return "\n".join(["retrieving status of query with handle %s" % flatargs[key],
                              " would return db handle ",
                              self.appInterface(flatargs[key])])
        else:
            return "\n".join([ "no handle in string, try h=handle ",
                               "where handle is the handle you got back from your initial query."])


    def reset(self, req):
        "Resets/restarts server uncleanly. No params."
        return self.appInterface.reset() # Should not return

    def stop(self, req):
        "Unceremoniously stop the server."
        return self.appInterface.stop()
        reactor.stop()
    pass


class Master:

    def __init__(self):
        try:
            cfg = config.config
            self.port = cfg.getint("frontend","port")
        except:
            print "Bad or missing port for server. Using",defaultPort
            self.port = defaultPort
        pass

    def listen(self):
        twisted.internet.reactor.suggestThreadPoolSize(concurrencyLimit)
        root = twisted.web.resource.Resource()
        twisted.web.static.loadMimeTypes() # load from /etc/mime.types

        ai = AppInterface(reactor)
        c = HttpInterface(ai)
        xml = XmlRpcInterface(ai)

        # initialize metadata cache
        try:
            ai.initMetadataCache()
        except QmsException as qe:
            print qe.getErrMsg()
            return

        # not sure I need the sub-pat http interface
        root.putChild(defaultPath, ClientResource(c))
        root.putChild(defaultXmlPath, xml)

        # publish the client functions
        for x in c.publishable:
            root.putChild(x, FunctionResource(getattr(c, x)))

        # init listening
        reactor.listenTCP(self.port, twisted.web.server.Site(root))

        print "Starting Qserv interface on port: %d"% self.port

        # Insert a memento so we can check if the reactor is running.
        reactor.lsstRunning = True
        reactor.run()
        pass

def runServer():
    m = Master()
    m.listen()
    pass


if __name__ == "__main__":
    runServer()
