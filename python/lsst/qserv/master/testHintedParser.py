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
        hq2 = ("""SELECT pm_raErr from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.1 AND decl BETWEEN -10 AND -2
        ;""",                     
              ["areaSpec_box", "1.5", "-10", "4.1", "-2"])
        ahq1 = ("""SELECT sum(pm_raErr) from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.1 AND decl BETWEEN -10 AND -2
        ;""",                     
              ["areaSpec_box", "1.5", "-10", "4.1", "-2"])
        ahq2 = ("""SELECT sum(pm_raErr),count(pm_raErr),avg(pm_raErr) from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.1 AND decl BETWEEN -10 AND -2
        ;""",                     
              ["areaSpec_box", "1.5", "-10", "4.1", "-2"])
        gbq1 = ("""SELECT avg(pm_raErr),chunkId from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 34.1 AND decl BETWEEN -25 AND -2
        GROUP BY chunkId
        ;""", ["areaSpec_box", "1.5", "-25", "34.1", "-2"])
        self.basicQuery = bq
        self.hintQuery = hq
        self.hintQuery2 = hq2
        self.aggQuery1 = ahq1
        self.aggQuery2 = ahq2
        self.groupByQuery1 = gbq1
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
        
    def testAggQuery(self):
        a = app.AppInterface()
        id1 = a.query(*self.aggQuery1)
        r1 = a.joinQuery(id1)
        print "Done aggregate query."
        print a.resultTableString(r1)

    def testAvgQuery(self):
        a = app.AppInterface()
        id1 = a.query(*self.hintQuery2)
        id2 = a.query(*self.aggQuery2)
        r1 = a.joinQuery(id1)
        r2 = a.joinQuery(id2)
        print "Done aggregate query."
        print a.resultTableString(r1)
        print a.resultTableString(r2)
        
    def testGroupByQuery(self):
        a = app.AppInterface()
        id1 = a.query(*self.groupByQuery1)
        r1 = a.joinQuery(id1)
        print "Done avg groupby query."
        print a.resultTableString(r1)
