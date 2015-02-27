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

# server.py : This module implements XML-RPC and HTTP interfacing
# logic using the Twisted networking library.  All methods on AppInterface
# whose names do not include '_' are exported on both interfaces, along
# with a few server utility commands.

# Standard Python imports
from itertools import ifilter, imap
import inspect
import os
import sys
import string
import time

# Twisted imports
from twisted.internet import reactor
import twisted.web
import twisted.web.resource
import twisted.web.server
import twisted.web.static
import twisted.web.xmlrpc

# Package imports
import logger
from appInterface import AppInterface
import config

# Module settings
defaultPort = 8000
defaultHttpPath = "c"
defaultXmlPath = "x"
concurrencyLimit = 50


class XmlRpcInterface(twisted.web.xmlrpc.XMLRPC):
    def __init__(self, endpoints):
        twisted.web.xmlrpc.XMLRPC.__init__(self)
        for endpoint in endpoints:
            setattr(self, "_".join(["xmlrpc", endpoint.__name__]), endpoint)
        self.help = [(endpoint.__name__, endpoint.__doc__) for endpoint in endpoints]
        self.help.append(("help", "Provide a summary of available commands."))
        self.help.sort()

    def xmlrpc_help(self):
        return self.help


class FunctionResource(twisted.web.resource.Resource):
    isLeaf = True
    doc = string.Template("<html><body>\n$body\n</body></html>")
    def __init__(self, endpoint):
        twisted.web.resource.Resource.__init__(self)
        self.func = endpoint

    def render_GET(self, request):
        body = str(self.func(*map(lambda x: request.args[x][0],
            ifilter(lambda x: x != "self",
                inspect.getargspec(self.func).args))))
        return FunctionResource.doc.substitute(body=body)


class HttpInterface(twisted.web.resource.Resource):
    def __init__(self, endpoints):
        twisted.web.resource.Resource.__init__(self)
        for endpoint in endpoints:
            self.putChild(endpoint.__name__, FunctionResource(endpoint))
        help = [(endpoint.__name__, endpoint.__doc__) for endpoint in endpoints]
        help.append(("help", "Provide a summary of available commands."))
        help.sort()
        helpList = string.Template("<dl>\n$items</dl>")
        helpItem = string.Template("<dt>$name</dt><dd>$doc</dd>\n")
        items = ""
        for (name, doc) in help:
            items += helpItem.substitute(name=name, doc=doc)
        self.helpstr = helpList.substitute(items=items)
        self.putChild("help", FunctionResource(lambda: self.helpstr))


class Czar:
    def __init__(self):
        try:
            cfg = config.config
            self.port = cfg.getint("frontend", "port")
        except:
            logger.wrn("Bad or missing port for server. Using", defaultPort)
            self.port = defaultPort

    def listen(self):
        self.ai = AppInterface(reactor.callInThread)
        endpoints = self.endpoints()
        root = twisted.web.resource.Resource()
        root.putChild(defaultXmlPath, XmlRpcInterface(endpoints))
        root.putChild(defaultHttpPath, HttpInterface(endpoints))
        twisted.web.static.loadMimeTypes() # load from /etc/mime.types
        reactor.suggestThreadPoolSize(concurrencyLimit)
        reactor.addSystemEventTrigger('before', 'shutdown', self.ai.cancelEverything)
        reactor.listenTCP(self.port, twisted.web.server.Site(root))
        logger.inf("Starting Qserv interface on port: %d"% self.port)
        reactor.run() # won't return until reactor.stop() is called

    def endpoints(self):
        endpoints = filter(inspect.ismethod,
            imap(lambda x: getattr(self.ai, x),
                ifilter(lambda x: '_' not in x,
                    dir(self.ai))))
        endpoints.extend([self.echo, self.stop, self.reset])
        logger.inf("endpoints:", endpoints)
        return endpoints

    def echo(self, echostr):
        "Echo a string back (useful for sanity checking)."
        return str(echostr)

    def stop(self):
        "Unceremoniously stop the server."
        reactor.stop()

    def reset(self):
        "Resets/restarts server uncleanly. No params."
        args = sys.argv #take the original arguments
        args.insert(0, sys.executable) # add python
        os.execv(sys.executable, args) # replace self with new python.
        # Normally we won't get here -- os.execv should overwrite us.
        logger.err("Reset failed:", sys.executable, str(args))


def runServer():
    cz = Czar()
    cz.listen()


if __name__ == "__main__":
    runServer()
