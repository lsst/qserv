#!/usr/bin/python
import unittest

import app
import server
import sqlparser

nearNeighborQueryAlias = """SELECT o1.id,o2.id,spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 WHERE dist < 25 AND o1.id != o2.id;"""
nearNeighborQueryMySql = """SELECT o1.id as o1id,o2.id as o2id,LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) 
  AS dist FROM Object AS o1, Object AS o2 WHERE LSST.spdist(o1.ra, o1.decl, o2.ra, o2.decl) < 25 AND o1.id != o2.id;"""
nearNeighborQuery = nearNeighborQueryMySql
nnSelectPart = "SELECT o1.id,o2.id,spdist(o1.ra, o1.decl, o2.ra, o2.decl)"

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
        ci = server.ClientInterface()
        class Dummy:
            pass
        arg = Dummy()
        #arg.args = {'q':['select * from Object where ra between 2 and 5 and decl between 1 and 10;',]} 
        arg.args = {'q':[nearNeighborQuery]}
        print ci.query(arg)
        self.assert_(True) # placeholder

    def clearTables(self):
        p = app.Persistence()
        p.activate()
        p.makeTables() # start anew
    
    pass


if __name__ == '__main__':
    unittest.main()


