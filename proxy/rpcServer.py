#!/usr/bin/python


from twisted.web import xmlrpc, server

class Example(xmlrpc.XMLRPC):

   def xmlrpc_echo(self, x):
       print "in echo, arg is ", x
       """
       Return all passed args.
       """
       return x


if __name__ == '__main__':
   from twisted.internet import reactor
   r = Example()
   reactor.listenTCP(7080, server.Site(r))
   reactor.run()


