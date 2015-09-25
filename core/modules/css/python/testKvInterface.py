#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2015 AURA/LSST.
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
This is a unittest for the Central State System Interface class.

@author  Jacek Becla, SLAC

"""
from __future__ import print_function

import copy
import ConfigParser
import lsst.qserv.css
from lsst.qserv.css import cssLib
import os
import random
import time
import unittest
import uuid

def getConfig():
    """
    @brief loads the configuration information needed to test KvInterface with a running mysql server
    @return a formatted MySqlConfig object
    """
    cfgFile = os.path.join(os.path.expanduser("~"), ".lsst", "KvInterfaceImplMySql-testRemote.txt")
    cfgParser = ConfigParser.ConfigParser()
    filesRead = cfgParser.read(cfgFile)
    if not cfgFile in filesRead:
        return None
    cfg = cssLib.MySqlConfig()
    cfg.username = cfgParser.get('mysql', 'user')
    cfg.password = cfgParser.get('mysql', 'passwd')
    cfg.hostname = cfgParser.get('mysql', 'host')
    cfg.port = cfgParser.getint('mysql', 'port')
    cfg.dbName = str("tempName" + str(uuid.uuid1()).replace('-', ''))
    return cfg


class TestKvInterface(unittest.TestCase):
    def setUp(self):
        self.dbCfg = getConfig()
        self.connectionCfg = getConfig()
        self.connectionCfg.dbName = '' # no db name for connecting to mysql.

        schemaFile = open('../../admin/templates/configuration/tmp/configure/sql/CssData.sql', 'r')
        schema = schemaFile.read()
        errObj = cssLib.SqlErrorObject()
        # replace production schema name with test schema:
        schema = schema.replace("qservCssData", self.dbCfg.dbName)
        self.sqlConn = cssLib.SqlConnection(self.connectionCfg)
        self.sqlConn.runQuery(schema, errObj)
        if errObj.isSet():
            raise RuntimeError("setupDatabase error: "  + errObj.printErrMsg())

        self._kvI = cssLib.KvInterfaceImplMySql(self.dbCfg)

    def tearDown(self):
        statement = "DROP DATABASE " + self.dbCfg.dbName
        errObj = cssLib.SqlErrorObject()
        self.sqlConn.runQuery(statement, errObj)
        if errObj.isSet():
            raise RuntimeError("cleanupDatabase error: "  + errObj.printErrMsg())

    def testCreateGetSetDelete(self):
        # first delete everything
        if self._kvI.exists("/unittest"):
            self._kvI.deleteKey("/unittest")
        # try second time, just for fun, should raise:
        self.assertRaises(lsst.qserv.css.CssError, self._kvI.deleteKey, "/unittest")
        # define key/value for testing
        k1 = "/unittest/my/first/testX"
        k2 = "/unittest/my/testY"
        v1 = "aaa"
        v2 = "AAA"
        # create the key
        self._kvI.create(k1, v1)
        # and another one
        self._kvI.create(k2, v2)
        # get the first one
        v1a = self._kvI.get(k1)
        assert(v1a == v1)
        # try to create it again, this should fail
        self.assertRaises(lsst.qserv.css.KeyExistsError, self._kvI.create, k1, v1)

        # set the value to something else
        self._kvI.set(k1, v2)
        # get it
        v2a = self._kvI.get(k1)
        assert(v2a == v2)
        # delete it
        self._kvI.deleteKey(k1)
        # try deleting it again, this should fail
        self.assertRaises(lsst.qserv.css.NoSuchKey, self._kvI.deleteKey, k1)
        # try to get it, it should fail
        self.assertRaises(lsst.qserv.css.NoSuchKey, self._kvI.get, k1)
        # set it
        self._kvI.set(k1, v1)
        # get the second key
        v2a = self._kvI.get(k2)
        assert(v2a == v2)
        # test getChildren
        # try an invalid key first
        self.assertRaises(lsst.qserv.css.CssError, self._kvI.getChildren, "/unittest/")
        self.assertEqual(self._kvI.getChildren("/unittest"), ("my", ))
        self.assertRaises(lsst.qserv.css.NoSuchKey, self._kvI.getChildren, "/whatever")
        # try to delete invalid key
        self.assertRaises(lsst.qserv.css.NoSuchKey, self._kvI.deleteKey, "/whatever")
        # # print everything
        # self._kvI.dumpAll()

    def testPerformance(self):
        n = 10 # set it to something larger for real test...
        start = time.clock()
        for i in range(1,n+1):
            k = "/unittest/node_%02i" % i
            #print ("creating %s --> %i" % (k, i))
            self._kvI.create(k, str(i))
        elapsed = time.clock()-start
        print("It took " + str(elapsed) + " to create " + str(n) + " entries.")
        self._kvI.deleteKey("/unittest")

def main():
    unittest.main()

if __name__ == "__main__":
    requiredRunDir = "lsst/qserv/core/modules"
    if not os.getcwd().endswith(requiredRunDir):
        raise BaseException("should be running from directory: " + requiredRunDir)
    main()
