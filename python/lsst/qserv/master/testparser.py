#!/usr/bin/env python

# Standard Python imports
import unittest
import time

# Package imports
import app
import server
import sqlparser

nearNeighborQueryAlias = """SELECT o1.id,o2.id,spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 WHERE dist < 25 AND o1.id != o2.id;"""
## tuson124 is down. :( and this version selects chunks on it.
##nearNeighborQueryMySql = """SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
###  AS dist FROM Object AS o1, Object AS o2 WHERE o1.ra between 10.5 and 11.5 and o2.decl between 9.7 and 10 AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 1 AND o1.id != o2.id;"""
nearNeighborQueryMySql = """SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 WHERE o1.ra between 30.5 and 31.5 and o2.decl between -20 and -19.2 AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 1 AND o1.id != o2.id;"""
nearNeighborQueryMySqlTemplate = """SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 
  WHERE o1.ra between ${ramin} and ${ramax} 
    AND o2.decl between ${declmin} and ${-19.2}
  AND LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 1 AND o1.id != o2.id;"""

nearNeighborQuery = nearNeighborQueryMySql
slowNearNeighborQuery = """SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 
  WHERE LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 0.001 AND o1.id != o2.id;"""
# Distance of 0.002 produces a selectivity of 10% on USNO.
nnSelectPart = "SELECT o1.id,o2.id,spdist(o1.ra, o1.decl, o2.ra, o2.decl)"

def randomBoundedNearNeigbor(raSize=1.0, declSize=1.0, seed=None):
    pass


def flatten(someList):
    if(type(someList) == type([])):
        print someList, "is a list outer"
        r = []
        for e in someList:
            if(type(e) == type([])):
                print e, "is a list"
                r.extend(flatten(e))
            else:
                r.append(e)
        return r
    print someList, "isn't a list"
    return "Error"
        
class TestAppFunctions(unittest.TestCase):

    def setUp(self):
        pass
    
    def testGrammarSelectPart(self):
        """Test select-portion parsing"""
        g = sqlparser.Grammar()
        res = g.selectPart.parseString(nnSelectPart)
        self.assert_("spdist" in res[:])
        res = g.identExpr.parseString("spdist(a.b, c.d)")
        self.assert_("spdist" in res[:])
        res = g.functionExpr.parseString("spdist(a.b, c.d)")
        
    def testGrammarParseNearNeighbor(self):
        """Tries to parse a near neighbor query."""
        g = sqlparser.Grammar()
        res = g.simpleSQL.parseString(nearNeighborQuery)
        
        pass

    def testQueryInsert(self):
        """Insert a query task into the tracker.
        """
        t = app.TaskTracker()
        qtext = "select * from blah;"
        task = app.QueryAction(qtext)
        id = t.track("myquery", task, qtext)
        print "persisted as id ", id
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
        ci = server.ClientInterface()
        class Dummy:
            pass
        arg = Dummy()
        arg.args = {'q':[q]}
        stats["clientInterfaceInitFinish"] = time.time()
        stats["interfaceQueryStart"] = time.time()
        print ci.query(arg)
        stats["interfaceQueryFinish"] = time.time()
        stats["overallFinish"] = time.time()
        out = open("qservMaster_timing.py","a")
        out.write(time.strftime("timing_%y%m%d_%H%M=" + str(queryTimer)+"\n"))
        #print queryTimer
        self.assert_(True) # placeholder
    
    pass


if __name__ == '__main__':
    unittest.main()


