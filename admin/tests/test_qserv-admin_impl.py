#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
This is a unittest for qserv-admin_impl

@author  Jacek Becla, SLAC

"""

import logging
import unittest

from lsst.qserv.css.kvInterface import CssException
from qserv-admin_impl import QservAdminImpl


class TestQservAdminImpl(unittest.TestCase):
    def setUp(self):
        self._impl = QservAdminImpl('127.0.0.1:2181')
        self._baseDir = "client/examples"

    def testCreateDb(self):
        dd = {"dbGroup": "L2",
              "partitioning": "on",
              "partitioningStrategy": "sphBox",
              "nStripes": "10",
              "nSubStripes": "23",
              "overlap": "0.0001",
              "objIdIndex": "0.25"}
        self._impl.createDb("db1a", dd)
        self.assertRaises(CssException, self._impl.createDb, "db1a", dd)
        self._impl.createDbLike("db1b", "db1a")
        # attempt to create db that already exists
        self.assertRaises(CssException, self._impl.createDbLike, "db1b", "db1a")
        # attempt to create db like non-existing db
        self.assertRaises(CssException, self._impl.createDbLike, "db1b", "xxxx")
        # attempt to create db like self
        self.assertRaises(CssException, self._impl.createDbLike, "db1a", "db1a")
        self.assertRaises(CssException, self._impl.createDbLike, "xxxx", "xxxx")

        self._impl.createDb("db2", dd)
        self._impl.dumpEverything()
        self._impl.dropDb("db1a")
        self._impl.dropDb("db1b")
        self._impl.dropDb("db2")
        self._impl.createDb("dbA", dd)
        self._impl.dropDb("dbA")
        self._impl.createDb("dbA", dd)

        print "=====>> create table Object"
        s = "%s/tbSchema_Object.sql" % self._baseDir
        dd = { "tableName":    "Object",
               "compression":  "1",
               "drivingTable": "Object",
               "isRefMatch":   " 0",
               "keyColName":   "ra_PS",
               "lonColName":   "ra_PS",
               "latColName":   "decl_PS",
               "objIdColName": "objectId",
               "schema":       "(objectId BIGINT, ra_PS DOUBLE, decl_PS DOUBLE)",
               "subChunks":    "0" }
        self._impl.createTable("dbA", "Summer2012", dd)
        self._impl.dumpEverything()
        self._impl.dropDb("dbA")

####################################################################################
def setLogging():
    logging.basicConfig(
        #filename="/tmp/testQservAdminImpl.log",
        format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
        datefmt='%m/%d/%Y %I:%M:%S', 
        level=logging.DEBUG)
    logging.getLogger("kazoo.client").setLevel(logging.ERROR)

def main():
    setLogging()
    unittest.main()

if __name__ == "__main__":
    main()
