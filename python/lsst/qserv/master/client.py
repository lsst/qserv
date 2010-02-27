#!/usr/bin/env python

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
