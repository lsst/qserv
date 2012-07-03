#! /usr/bin/env python
import lsst.qserv.master as qMaster
import lsst.qserv.master.config as config
import unittest


class AppTest(unittest.TestCase):
    """Tests... This is a catch-all for driving the query 
    parse/generate/manipulate code.
    """
    def setUp(self):
        global _options
        pass

    def tearDown(self):
        pass

    def test_parse(self):
        query = " SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), x_chunkId FROM Object GROUP BY x_chunkId;";

        qConfig = self._prepareConfig()
        sess = qMaster.newSession(qConfig)
        qMaster.setupQuery(sess, query)
        cvec = qMaster.getConstraints(1)
        def vecGen(constr):
            s = constr.paramsSize()
            for i in range(s):
                yield constr.paramsGet(i)
            pass
        def vecConGen(cvec):
            sz = cvec.size()
            for i in range(sz):
                c = cvec.get(i)                
                yield c.name + "-->" + ",".join(vecGen(c))
        print "\n".join(vecConGen(cvec))

        for i in range(3):
            self._addChunk(1, i)

        pass
        self.assertEqual(1,1)

    def _addChunk(self, session, base):
        cs = qMaster.ChunkSpec()
        cs.chunkId = 1000 + base
        for i in range(base * 10, 10 + (base *10)):
            cs.addSubChunk(i)
        qMaster.addChunk(session, cs)

        
    def _prepareConfig(self):
        qMaster.config.load() # Init.
        cfg = qMaster.config.getStringMap()
        cfg["table.defaultdb"] = "ddb"
        cfg["query.hints"] = "box,0,0,5,1;circle,1,1,1;"
        return cfg

    
def main():
    global _options
    unittest.main()


if __name__ == "__main__":
    main()
