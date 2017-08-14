#!/usr/bin/env python

"""
This is a unit test for CSS node definitions.

@author  Andy Salnikov, SLAC

"""

import logging
import unittest

import lsst.log
from lsst.qserv import css
from lsst.qserv.admin import watcherLib

# redirect Python logging to LSST logger
lsst.log.setLevel('', lsst.log.INFO)
pylgr = logging.getLogger()
pylgr.setLevel(logging.DEBUG)
pylgr.addHandler(lsst.log.LogHandler())


def _makeWcss(initData):
    initData = initData.format(version=css.VERSION)
    config = dict(technology='mem', data=initData)
    return watcherLib.WatcherCss(config)


class TestExecutor(watcherLib.IExecutor):

    ''' Implementation IExecutor used for testing '''

    def __init__(self, returnVal=True):
        self.lastCall = None
        self.lastOptions = None
        self.returnVal = returnVal

    def createDb(self, dbName, options):
        self.lastCall = "CREATE DATABASE " + dbName
        self.lastOptions = options
        if self.returnVal is None:
            raise Exception(self.lastCall)
        return self.returnVal

    def dropDb(self, dbName, options):
        self.lastCall = "DROP DATABASE " + dbName
        self.lastOptions = options
        if self.returnVal is None:
            raise Exception(self.lastCall)
        return self.returnVal

    def createTable(self, dbName, tableName, options):
        self.lastCall = "CREATE TABLE " + dbName + "." + tableName
        self.lastOptions = options
        if self.returnVal is None:
            raise Exception(self.lastCall)
        return self.returnVal

    def dropTable(self, dbName, tableName, options):
        self.lastCall = "DROP TABLE " + dbName + "." + tableName
        self.lastOptions = options
        if self.returnVal is None:
            raise Exception(self.lastCall)
        return self.returnVal


class TestWatcherLib(unittest.TestCase):

    def test_01_getDbs(self):
        """ Test for getting db status """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tREADY
/DBS/DB2\tDO_NOT_USE
/DBS/DB3\tPENDING_CREATE:12345
/DBS/DB4\tPENDING_DROP:12345
/DBS/DB5\tFAILED:Test failed
"""

        wCss = _makeWcss(initData)

        dbs = wCss.getDbs()
        self.assertEqual(len(dbs), 5)
        self.assertIn("DB1", dbs)
        self.assertIn("DB2", dbs)
        self.assertIn("DB3", dbs)
        self.assertIn("DB4", dbs)
        self.assertIn("DB5", dbs)
        self.assertEqual(dbs["DB1"], "READY")
        self.assertEqual(dbs["DB2"], "DO_NOT_USE")
        self.assertEqual(dbs["DB3"], "PENDING_CREATE:12345")
        self.assertEqual(dbs["DB4"], "PENDING_DROP:12345")
        self.assertEqual(dbs["DB5"], "FAILED:Test failed")

    def test_02_getTables(self):
        """ Test for getting table status """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tREADY
/DBS/DB1/TABLES\t\\N
/DBS/DB1/TABLES/TABLE1\tREADY
/DBS/DB1/TABLES/TABLE2\tFAILED:Not again
/DBS/DB2\tREADY
/DBS/DB2/TABLES\t\\N
/DBS/DB2/TABLES/TABLE1\tPENDING_DELETE:
/DBS/DB2/TABLES/TABLE2\tDO_NOT_USE
"""

        wCss = _makeWcss(initData)

        tables = wCss.getTables()
        self.assertEqual(len(tables), 4)
        self.assertIn(("DB1", "TABLE1"), tables)
        self.assertIn(("DB1", "TABLE2"), tables)
        self.assertIn(("DB2", "TABLE1"), tables)
        self.assertIn(("DB2", "TABLE2"), tables)
        self.assertEqual(tables[("DB1", "TABLE1")], "READY")
        self.assertEqual(tables[("DB1", "TABLE2")], "FAILED:Not again")
        self.assertEqual(tables[("DB2", "TABLE1")], "PENDING_DELETE:")
        self.assertEqual(tables[("DB2", "TABLE2")], "DO_NOT_USE")

    def test_10_createDb(self):
        """ Test for creating database """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tPENDING_CREATE:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor()
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "CREATE DATABASE DB1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getDbs()['DB1'], css.KEY_STATUS_READY)

    def test_11_dropDb(self):
        """ Test for dropping database """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tPENDING_DROP:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor()
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "DROP DATABASE DB1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertNotIn('DB1', wCss.getDbs())

    def test_12_createTable(self):
        """ Test for creating table """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tREADY
/DBS/DB1/TABLES\t\\N
/DBS/DB1/TABLES/TABLE1\tPENDING_CREATE:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor()
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "CREATE TABLE DB1.TABLE1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getTables()[('DB1', 'TABLE1')], css.KEY_STATUS_READY)

    def test_13_dropTable(self):
        """ Test for dropping table """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tREADY
/DBS/DB1/TABLES\t\\N
/DBS/DB1/TABLES/TABLE1\tPENDING_DROP:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor()
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "DROP TABLE DB1.TABLE1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertNotIn(('DB1', 'TABLE1'), wCss.getTables())

    def test_20_createDbFail(self):
        """ Test for creating database """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tPENDING_CREATE:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor(None)
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "CREATE DATABASE DB1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getDbs()['DB1'], "FAILED:CREATE DATABASE DB1")

    def test_21_dropDbFail(self):
        """ Test for dropping database """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tPENDING_DROP:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor(None)
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "DROP DATABASE DB1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getDbs()['DB1'], "FAILED:DROP DATABASE DB1")

    def test_22_createTableFail(self):
        """ Test for creating table """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tREADY
/DBS/DB1/TABLES\t\\N
/DBS/DB1/TABLES/TABLE1\tPENDING_CREATE:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor(None)
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "CREATE TABLE DB1.TABLE1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getTables()[('DB1', 'TABLE1')], "FAILED:CREATE TABLE DB1.TABLE1")

    def test_23_dropTable(self):
        """ Test for dropping table """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tREADY
/DBS/DB1/TABLES\t\\N
/DBS/DB1/TABLES/TABLE1\tPENDING_DROP:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor(None)
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "DROP TABLE DB1.TABLE1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getTables()[('DB1', 'TABLE1')], "FAILED:DROP TABLE DB1.TABLE1")

    def test_30_createDbSkip(self):
        """ Test for creating database """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tPENDING_CREATE:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor(False)
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "CREATE DATABASE DB1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getDbs()['DB1'], "PENDING_CREATE:12345")

    def test_31_dropDbSkip(self):
        """ Test for dropping database """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tPENDING_DROP:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor(False)
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "DROP DATABASE DB1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getDbs()['DB1'], "PENDING_DROP:12345")

    def test_32_createTableSkip(self):
        """ Test for creating table """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tREADY
/DBS/DB1/TABLES\t\\N
/DBS/DB1/TABLES/TABLE1\tPENDING_CREATE:12345
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor(False)
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "CREATE TABLE DB1.TABLE1")
        self.assertEqual(executor.lastOptions, "12345")
        self.assertEqual(wCss.getTables()[('DB1', 'TABLE1')], "PENDING_CREATE:12345")

    def test_33_dropSkip(self):
        """ Test for dropping table """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/DB1\tREADY
/DBS/DB1/TABLES\t\\N
/DBS/DB1/TABLES/TABLE1\tPENDING_DROP:12345:qid=24
"""

        wCss = _makeWcss(initData)
        executor = TestExecutor(False)
        watcher = watcherLib.Watcher(wCss, executor)
        watcher.run(True)
        self.assertEqual(executor.lastCall, "DROP TABLE DB1.TABLE1")
        self.assertEqual(executor.lastOptions, "12345:qid=24")
        self.assertEqual(wCss.getTables()[('DB1', 'TABLE1')], "PENDING_DROP:12345:qid=24")


#

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestWatcherLib)
    unittest.TextTestRunner(verbosity=3).run(suite)
