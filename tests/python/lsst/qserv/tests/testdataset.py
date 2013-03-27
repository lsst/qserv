
from lsst.qserv.admin import commons
from benchmark import Benchmark
import unittest

class TestDataSet(unittest.TestCase):

    def setUp(self):
        self.config = commons.read_user_config()
        self.modeList = ['mysql','qserv']
        self.loadData = True

    def _runTestCase(self, case_id):
        bench = Benchmark(case_id, out_dirname_prefix = self.config['qserv']['tmp_dir'])
        bench.run(self.modeList, self.loadData)
        ok = bench.areQueryResultsEquals()
        self.assertTrue(ok)
   
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
    suite = unittest.TestSuite()
    #suite.addTest(TestDataSet('test_case01'))    
    suite = unittest.TestLoader().loadTestsFromTestCase(TestDataSet)
    return suite

unittest.TextTestRunner(verbosity=2).run(suite())

