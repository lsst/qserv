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

# Package imports
import app


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



class ClientInterface:
    def __init__(self):
        self.tracker = app.TaskTracker()
        okname = ifilter(lambda x: "_" not in x, dir(self))
        self.publishable = filter(lambda x: hasattr(getattr(self,x), 'func_doc'), 
                                  okname)
        
        pass

    def query(self, req):
        "Issue a query. Params: q=querystring"
        print req
        flatargs = dict(map(lambda t:(t[0],t[1][0]), req.args.items()))
        #printdir(arg)
        if 'q' in req.args:
            a = app.QueryAction(flatargs['q'])
            id = self.tracker.track("myquery", a, flatargs['q'])
            stats = time.qServQueryTimer[time.qServRunningName]
            stats["appInvokeStart"] = time.time()
            if 'lsstRunning' in dir(reactor):
                reactor.callInThread(a.invoke3)
            else:
                a.invoke3()
            stats["appInvokeFinish"] = time.time()
            return "Server processed, q='" + flatargs['q'] + "' your id is %d" % (id)
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
        flatargs = dict(map(lambda t:(t[0],t[1][0]), req.args.items()))
        if 'h' in req.args and int:
            a = app.CheckAction(self.tracker, flatargs['h'])
            a.invoke()
            
            return "\n".join(["checking status of query with handle %s" % flatargs['h'],
                              " would return \%done got %d", a.results])

        else:
            return "\n".join([ "no handle in string, try h=handle ",
                               "where handle is the handle you got back from your initial query."])
    def results(self, req):
        "Get results location for a query or task. Params: h=handle"
        print req
        flatargs = dict(map(lambda t:(t[0],t[1][0]), req.args.items()))
        if 'h' in req.args:
            return "\n".join(["retrieving status of query with handle %s" % flatargs['h'],
                              " would return db handle ",
                              app.results(self.tracker, flatargs['h']) ])

        else:
            return "\n".join([ "no handle in string, try h=handle ",
                               "where handle is the handle you got back from your initial query."])


    def reset(self, req):
        "Resets/restarts server uncleanly. No params."
        args = sys.argv #take the original arguments
        args.insert(0, sys.executable) # add python
        os.execv(sys.executable, args) # replace self with new python.
        print "Reset failed:",sys.executable, str(args)
        return # This will not return.  os.execv should overwrite us.

    def stop(self, req):
        "Unceremoniously stop the server."
        reactor.stop()
    pass

class Master:

    def __init__(self):
        self.port = 8000
        pass

    def listen(self):
        root = twisted.web.resource.Resource()
        twisted.web.static.loadMimeTypes() # load from /etc/mime.types

        c = ClientInterface()
        
        # not sure I need this now.
        root.putChild("c", ClientResource(ClientInterface()))

        # publish the the client functions
        c.publishable
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
