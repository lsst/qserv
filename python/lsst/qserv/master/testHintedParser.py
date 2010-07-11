#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#


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
        bqPt1 = ("SELECT * FROM LSST.Object WHERE rRadius_SG > 500;", 
                 {"box": "148, 1.6, 152, 2.3"})

        hq = ("""SELECT * from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.4 AND decl BETWEEN -10 AND -2
        ;""",                     
              {"box": "1.5,-10,4.4,-2"})
        hq2 = ("""SELECT pm_raErr from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.1 AND decl BETWEEN -10 AND -2
        ;""",                     
              {"box": "1.5, -10, 4.1, -2"})
        ahq1 = ("""SELECT sum(pm_raErr) from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.1 AND decl BETWEEN -10 AND -2
        ;""",                     
              {"box": "1.5, -10, 4.1, -2"})
        ahq2 = ("""SELECT sum(pm_raErr),count(pm_raErr),avg(pm_raErr) from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 4.1 AND decl BETWEEN -10 AND -2
        ;""",                     
              {"box": "1.5, -10, 4.1, -2"})
        gbq1 = ("""SELECT avg(pm_raErr),chunkId from LSST.Object 
        WHERE ra BETWEEN 1.5 AND 34.1 AND decl BETWEEN -25 AND -2
        GROUP BY chunkId
        ;""", {"box": "1.5, -25, 34.1, -2"})
        nnq1 = ("""
SELECT o1.id as o1id,o2.id as o2id,
       LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
 AS dist FROM Object AS o1, Object AS o2 
 WHERE ABS(o1.decl-o2.decl) < 1
     AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 1
     AND o1.id != o2.id;""", {"box":"1.5, -25, 34.1, -2"})
        self.osjoinQ = (
            """
SELECT o1.objectId, o1.rRadius_SG, s.sourceId, s.psfFlux, s.apFlux 
FROM LSST.Object o1, LSST.Source s 
WHERE o1.objectId=s.objectId AND o1.rRadius_SG > 600;""", 
                   {"box": "148, 1.6, 152, 2.3"})
        allskyPt1 = ("SELECT * FROM Object WHERE rRadius_SG > 500;", 
                     {"db": "LSST"})
                   
        self.basicQuery = bq
        self.basicPt1Query = bqPt1
        self.hintQuery = hq
        self.hintQuery2 = hq2
        self.aggQuery1 = ahq1
        self.aggQuery2 = ahq2
        self.groupByQuery1 = gbq1
        self.nearNeighQuery1 = nnq1
        self.allskyPt1 = allskyPt1
        pass
                
    def _performTestQueryAction(self, queryTuple):
        a = app.AppInterface()
        result = a.queryNow(*queryTuple)
        print "Done Query."
        print result
        pass
        
    def testBasic(self):
        """This is a non-spatially-restricted query that might
        be really expensive."""
        return self._performTestQueryAction(self.basicQuery)

    def testBasicPt1(self):
        """This is a somewhat spatially-restricted query that uses PT1 
        Object schema"""
        return self._performTestQueryAction(self.basicPt1Query)

    def testSourceObjJoin(self):
        """This is a Object-source join.
        Using PT1 Object schema"""
        return self._performTestQueryAction(self.osjoinQ)

    def testBoxHints(self):
        return self._performTestQueryAction(self.hintQuery)

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

    def testNnQuery(self):
        a = app.AppInterface()
        id1 = a.query(*self.nearNeighQuery1)
        r1 = a.joinQuery(id1)
        print "Done near neighbor query."
        print a.resultTableString(r1)

    def testAllskyPt1(self):
        """This is a non spatially-restricted query that uses PT1 
        Object schema"""
        return self._performTestQueryAction(self.allskyPt1)

    def testObjIdHint(self):
        return self._performTestQueryAction((
                "SELECT * FROM Object WHERE objectId =428457347514371;", 
                     {"db": "LSST", "objectId" : "428457347514371"}))

