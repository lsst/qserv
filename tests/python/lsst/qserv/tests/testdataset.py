
import logging

from lsst.qserv.admin import commons
from benchmark import Benchmark
import unittest

class TestDataSet(unittest.TestCase):

    def setUp(self):
        self.config = commons.getConfig()
        self.logger = logging.getLogger()
        self.modeList = ['mysql','qserv']
        self.loadData = True

    def _runTestCase(self, case_id):
        bench = Benchmark(case_id, out_dirname_prefix = self.config['qserv']['tmp_dir'])
        bench.run(self.modeList, self.loadData)
        failed_queries = bench.analyzeQueryResults()
        nb_failed_queries = len(failed_queries)
        if  nb_failed_queries != 0:
            msg = "Queries with different results between Qserv and MySQL : %s" % failed_queries
            self.logger.error(msg)
            self.fail(msg)
   
    def test_case01(self):
        case_id = "01"
        self._runTestCase(case_id)

    def test_case02(self):
        case_id = "02"
        self._runTestCase(case_id)

    def test_case03(self):
        case_id = "03"
        self._runTestCase(case_id)

def suite():
    #suite = unittest.TestSuite()
    #suite.addTest(TestDataSet('test_case01'))    
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDataSet)
    return suite


