#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008-2014 AURA/LSST.
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

# appTest - Test functions that operate on the appInterface level.
#           Queries need to get updated so that they can be applied
#           on well-known well-defined datasets. 2012 Summer release
#           may be a good target.
#
#           FIXME: query update and grouping
#           FIXME: Needs to be re-organized with TestHintedParser so that
#                  they perform orthogonal duties.

# Standard Python imports
import unittest
import time

# Package imports
import app
import server
import logger

distFuncStr = "scisql_angSep(%s,%s,%s,%s)"
def nnDist(t1, t2, ra, decl):
    return ("%s.%s" % (t1, ra),
            "%s.%s" % (t1, decl),
            "%s.%s" % (t2, ra),
            "%s.%s" % (t2, decl))
def betweenBound(raref, declref, ramin, ramax, declmin,declmax):
    return "%s BETWEEN %f AND %f AND %s BETWEEN %f AND %f" % (
        raref, ramin, ramax, declref, declmin,declmax)

def qservBound(raref, declref, ramin, ramax, declmin,declmax):
    return "qserv_areaspec_box(%f,%f,%f,%f)" % (
        ramin, declmin, ramax, declmax)

### Queries need to get grouped according to test data.

nearNeighborQueryAlias = """SELECT o1.id,o2.id,%s AS dist
FROM Object AS o1, Object AS o2
WHERE dist < 25 AND o1.id != o2.id;""" % (distFuncStr %
                                          nnDist("o1", "o2", "ra_PS", "decl_PS"))


##nearNeighborQueryMySql = """SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl)
###  AS dist FROM Object AS o1, Object AS o2 WHERE o1.ra between 10.5 and 11.5 and o2.decl between 9.7 and 10 AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 1 AND o1.id != o2.id;"""
nearNeighborQueryMySql = """SELECT o1.id as o1id,o2.id as o2id,%s AS dist
  FROM Object AS o1, Object AS o2
  WHERE o1.ra between 30.5 and 31.5 and o2.decl between -20 and -19.2
  AND %s < 1 AND o1.id != o2.id;""" % (distFuncStr
                                       % nnDist("o1", "o2", "ra_PS", "decl_PS"),
                                       distFuncStr
                                       % nnDist("o1", "o2", "ra_PS", "decl_PS"))

nearNeighborQueryMySqlTemplate = """SELECT o1.id as o1id,o2.id as o2id,%s
  AS dist FROM Object AS o1, Object AS o2
  WHERE o1.ra between ${ramin} and ${ramax}
    AND o2.decl between ${declmin} and ${-19.2}
  AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 1 AND o1.id != o2.id;"""

nearNeighborQuery = nearNeighborQueryMySql
slowNearNeighborQuery = """SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl)
  AS dist FROM Object AS o1, Object AS o2
  WHERE ABS(o1.decl-o2.decl) < 0.001 AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 0.001 AND o1.id != o2.id;"""
selectSmallQuery = """SELECT * FROM Object
WHERE ra BETWEEN 20 AND 20.2
AND decl BETWEEN 2 AND 2.2;"""
tableScanQuery = """SELECT * FROM Object
WHERE bMag2 > 21.2;
"""
# Distance of 0.002 produces a selectivity of 10% on USNO.
nnSelectPart = "SELECT o1.id,o2.id,spdist(o1.ra, o1.decl, o2.ra, o2.decl)"

def randomBoundedNearNeigbor(raSize=1.0, declSize=1.0, seed=None):
    pass


def flatten(someList):
    if(type(someList) == type([])):
        logger.inf(someList, "is a list outer")
        r = []
        for e in someList:
            if(type(e) == type([])):
                logger.inf(e, "is a list")
                r.extend(flatten(e))
            else:
                r.append(e)
        return r
    logger.inf(someList, "isn't a list")
    return "Error"

class TestAppFunctions(unittest.TestCase):

    def setUp(self):
        pass

    def testQueryInsert(self):
        """Insert a query task into the tracker.
        """
        t = app.TaskTracker()
        qtext = "select * from blah;"
        task = app.QueryAction(qtext)
        id = t.track("myquery", task, qtext)
        logger.inf("persisted as id ", id)
        self.assert_(True) # placeholder

    def testServerQuery(self):
        """Apply a canned query using the server's client interface
        """
        self._invokeTimedServerQuery(nearNeighborQuery, "Light 1 sq.degree near-neighbor query")

    def testServerSlowQuery(self):
        "Apply a canned query to the server. (all chunks,subchunks"
        query = slowNearNeighborQuery
        self._invokeTimedServerQuery(query, "Mr.Slow")
        self.assert_(True) # placeholder

    def testSlow(self):
        "Alias for ServerSlowQuery"
        return self.testServerSlowQuery()

    def testHigh(self):
        "Test a high volume (amt of data touched) query"
        self._invokeTimedServerQuery(tableScanQuery, "HighVolTableScan")
        self.assert_(True)
        pass

    def testLow(self):
        "Test a low volume (amt of data touched) query"
        self._invokeTimedServerQuery(selectSmallQuery, "LowVolSpatial")
        self.assert_(True)
        pass

    def testCss(self):
        "Test the czar css module"
        import css
        css.selftest()

    def clearTables(self):
        p = app.Persistence()
        p.activate()
        p.makeTables() # start anew

    def _invokeTimedServerQuery(self, q, name="untitled"):
        global queryTimer, runningName
        runningName = name
        if "queryTimer" not in dir():
            queryTimer = {}
        # These next two are a little sleazy.
        time.qServQueryTimer = queryTimer #Put into time package
        time.qServRunningName = runningName #Put into time package

        queryTimer[name] = {}
        stats = queryTimer[name]
        stats["overallStart"] = time.time()
        stats["clientInterfaceInitStart"] = time.time()
        ci = server.AppInterface()
        class Dummy:
            pass
        arg = Dummy()
        arg.args = {'q':[q]}
        stats["clientInterfaceInitFinish"] = time.time()
        stats["interfaceQueryStart"] = time.time()
        logger.inf(ci.query(arg, None))
        stats["interfaceQueryFinish"] = time.time()
        stats["overallFinish"] = time.time()
        out = open("qservMaster_timing.py","a")
        out.write(time.strftime("timing_%y%m%d_%H%M=" + str(queryTimer)+"\n"))
        #logger.inf(queryTimer)
        self.assert_(True) # placeholder

    pass


if __name__ == '__main__':
    unittest.main()


