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
        bq = ("SELECT * FROM LSST.Object WHERE bMag2 > 21.2;", None)
        hq = ("""SELECT * from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.4 AND decl BETWEEN -10 AND -2
        ;""",                     
              ["areaSpec_box", "1.5", "-10", "4.4", "-2"])
        self.basicQuery = bq
        self.hintQuery = hq
        pass
    
    def testBasic(self):
        """This is a non-spatially-restricted query that might
        be really expensive."""
        a = app.AppInterface()
        result = a.queryNow(*self.basicQuery)
        
        print "Done Query."
        print result
        pass

    def testBoxHints(self):
        a = app.AppInterface()
        result = a.queryNow(*self.hintQuery)
        print "Done Query."
        print result
        pass
        

