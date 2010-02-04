#!/usr/bin/python

import time

from twisted.web import xmlrpc, server

class Example(xmlrpc.XMLRPC):

    def xmlrpc_echo(self, x):
        return x

    def xmlrpc_submitQuery(self, query, conditions):
        print "query '", query, "' submitted, conditions: ", conditions
        #time.sleep(5)
        print "done sleeping"
        return "resultTableName"

if __name__ == '__main__':
    from twisted.internet import reactor
    r = Example()
    reactor.listenTCP(7080, server.Site(r))
    reactor.run()


