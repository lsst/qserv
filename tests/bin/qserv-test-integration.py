#!/usr/bin/env python
from lsst.qserv.admin import commons
from lsst.qserv.admin import logger
from lsst.qserv.tests import benchmark
from lsst.qserv.tests.testdataset import TestDataSet, suite

import argparse
import logging
import os
import unittest

def parseArgs():

    parser = argparse.ArgumentParser(
            description='''Qserv integration tests suite. Relies on python unit
testing framework, provide test meta-data which can be used for example in a
continuous integration framework or by a cluster management tool. Configuration values
are read from ~/.lsst/qserv.conf.''',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

    parser = benchmark.add_generic_arguments(parser)

    args = parser.parse_args()

    args.verbose_level = logger.verbose_dict[args.verbose_str]

    return args

if __name__ == '__main__':
    args = parseArgs()

    benchmark.init(args, logfile="qserv-test-integration")
    unittest.TextTestRunner(verbosity=2).run(suite())
