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
        hq2 = ("""SELECT * from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.1 AND decl BETWEEN -10 AND -2
        ;""",                     
              ["areaSpec_box", "1.5", "-10", "4.1", "-2"])
        self.basicQuery = bq
        self.hintQuery = hq
        self.hintQuery2 = hq2
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

    def testParallelQuery(self):
        a = app.AppInterface()
        id1 = a.query(*self.hintQuery)
        id2 = a.query(*self.hintQuery2)
        r1 = a.joinQuery(id1)
        print "Done hinted query."
        print a.resultTableString(r1)

        r2 = a.joinQuery(id2)
        print "Done hinted query."
        print a.resultTableString(r2)
        
    def testParallelQuery2(self):
        a = app.AppInterface()
        id1 = a.query(*self.basicQuery)
        id2 = a.query(*self.hintQuery2)
        r1 = a.joinQuery(id1)
        print "Done fullsky query."
        print a.resultTableString(r1)

        r2 = a.joinQuery(id2)
        print "Done hinted query."
        print a.resultTableString(r2)
        

