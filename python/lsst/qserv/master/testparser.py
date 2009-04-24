#!/usr/bin/python
import unittest

import app
import server

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
        print "persisted as id ", id
        self.assert_(True) # placeholder

    def testServerQuery(self):
        """Apply a canned query using the server's client interface
        """
        ci = server.ClientInterface()
        class Dummy:
            pass
        arg = Dummy()
        arg.args = {'q':['select * from obj where ra between 2 and 5 and decl between 1 and 10;',]}
        print ci.query(arg)
        self.assert_(True) # placeholder

    def clearTables(self):
        p = app.Persistence()
        p.activate()
        p.makeTables() # start anew
    
    pass


if __name__ == '__main__':
    unittest.main()


