#!/usr/bin/python

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
        def hardWork():
            print "query '", query, "' submitted, conditions: ", conditions
            #time.sleep(5)
            #print "done sleeping"
            return "dummyResults%s" % nextX()
        return deferToThread(hardWork)


if __name__ == '__main__':
    from twisted.internet import reactor

    r = Example()
    root = resource.Resource()
    root.putChild("x", r)
    reactor.listenTCP(7080, server.Site(root))
    reactor.run()


