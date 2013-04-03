#!/usr/bin/env python

from  lsst.qserv.tests.testqservdataloader import TestQservDataLoader, suite
import unittest

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
