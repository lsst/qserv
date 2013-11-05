#!/usr/bin/env python



from lsst.qserv.admin import commons
from lsst.qserv.tests.testdataset import TestDataSet, suite

import logging
import unittest

if __name__ == '__main__':
    config = commons.read_user_config()
    commons.init_default_logger("qserv-test-dataset-all", logging.INFO, config['qserv']['log_dir'])
    unittest.TextTestRunner(verbosity=2).run(suite())
