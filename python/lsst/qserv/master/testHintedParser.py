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
class TestHintedParser(unittest.TestCase):

    def setUp(self):
        pass
    
    def testBasic(self):
#        config.load()
        a = app.AppInterface()
        a.query("SELECT * FROM Object WHERE bMag2 > 21.2;", None)
        print "Done Query."
        pass
    

