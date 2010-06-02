#!/usr/bin/env python

# Standard Python imports
import unittest
##import os
import sys
import time

# Package imports
import lsst.qserv.master
from lsst.qserv.master import appInterface as app
from lsst.qserv.master import config

class TestAppInterface(unittest.TestCase):
    def setUp(self):
        pass

    def testFuzz(self, seed=None):
        """Fuzz-test the app interface.
        This consists of feeding oddly-shaped garbage to the app interface
        and seeing if we get an error message or a crash.
        Goal: Test the outward-facing, non-developer interfaces.
        """
        self._applyQueryFromFile("/etc/motd")

    def _makeBadHint(self):
        "This should be randomized."
        d = {"I":32,"like":44,"peaches,":66, 
             11: "and", 22:"peaches",33:"like",44:"me"}
        return d

    def _applyQueryFromFile(self, filename):
        x = self._readFile(filename)
        if x:
            a = app.AppInterface()
            q = x
            result = a.queryNow(q, self._makeBadHint())
        print result

    def _readFile(self, filename):
        try:
            x = open(filename, "r").read()
            return x
        except:
            return None
    
            
        
