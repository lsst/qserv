#!/usr/bin/env python

# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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
#
# Tool for testing qserv metadata server and the client library.
# It is a thin shell that parses arguments, reads auth file,
# and tries to break qms in some expected and unexpected ways.


import os
import ConfigParser
import unittest

from lsst.qserv.meta.status import Status, QmsException
from lsst.qserv.meta.client import Client

host = None
port = None
user = None
pwd = None
baseDir = None
class TestMeta(unittest.TestCase):
    def setUp(self):
        global host, port, user, pwd, baseDir
        if baseDir is None:
            # find what the baseDir is
            baseDir = os.getenv("metaTest_baseDir")
            if baseDir is None:
                baseDir = os.getcwd()
            baseDir += "/meta/examples"
            print "Using baseDir: ", baseDir
            # read in authorization info
            (host, port, user, pwd) = self._getCachedConnInfo()

        self._baseDir = baseDir
        self._client = Client(host, port, user, pwd)
        # cleanup
        try: self._client.destroyMeta()
        except QmsException: pass

    def tearDown(self):
        try: self._client.destroyMeta()
        except QmsException: pass

    ###########################################################################
    #### test_basics
    ###########################################################################
    def test_basics(self):
        print "=====>> print meta (but meta does not exist)"
        self.assertRaises(QmsException, self._client.printMeta)

        print "=====>> install meta"
        self._client.installMeta()

        print "=====>> list dbs"
        print self._client.listDbs()

        print "=====>> print meta"
        print self._client.printMeta()

    ###########################################################################
    #### test_createDbsAndTables
    ###########################################################################
    def test_createDbsAndTables(self):
        print "=====>> install meta"
        self._client.installMeta()

        print "=====>> create partitioned db"
        dd = {"partitioning": "on",
              "partitioningStrategy": "sphBox",
              "nStripes": "10",
              "nSubStripes": "23",
              "defaultOverlap_fuzziness": "0.0001",
              "defaultOverlap_nearNeighbor": "0.25"}
        self._client.createDb("Summer2012", dd)

        print "=====>> print meta"
        print self._client.printMeta()

        print "=====>> try create db (already exists)"
        self.assertRaises(QmsException, self._client.createDb, "Summer2012",dd)

        print "=====>> create non-partitioned db"
        dd = {"partitioning": "off"}
        self._client.createDb("NonPart", dd)

        print "=====>> list dbs"
        print self._client.listDbs()

        print "=====>> check if db exists (it does)"
        self.assertTrue(self._client.checkDbExists("Summer2012"))

        print "=====>> check if db exists (it does not)"
        self.assertFalse(self._client.checkDbExists("xerd"))

        print "=====>> retrieveDb info for partitioned db"
        print self._client.retrieveDbInfo("Summer2012")

        print "=====>> retrieveDb info for non-partitioned db"
        print self._client.retrieveDbInfo("NonPart")

        print "=====>> create table Object"
        s = "%s/tbSchema_Object.sql" % self._baseDir
        dd = { "tableName": "Object",
               "partitioning": "on",
               "schemaFile": s,
               "clusteredIndex": "IDX_objectId",
               "overlap": "0.025",
               "phiColName": "ra_PS",
               "thetaColName": "decl_PS",
               "logicalPart": "2",
               "physChunking": "0x0021",
               "isRefMatch":"no" }
        self._client.createTable("Summer2012", dd)

        print "=====>> try create table (already exists)"
        try:
            self._client.createTable("Summer2012", dd)
        except QmsException as qe: pass
        else: self.fail("createTable: Exception not thrown")

        print "=====>> create table Exposure"
        s = "%s/tbSchema_Exposure.sql" % self._baseDir
        dd = { "tableName": "Exposure",
               "partitioning": "off",
               "schemaFile": s }
        self._client.createTable("Summer2012", dd)

        print "=====>> create table Source"
        # note, isRefMatch not set, default should be picked "no"
        s = "%s/tbSchema_Source.sql" % self._baseDir
        dd = { #"tableName": "Source", # get table name from schema file
               "partitioning": "on",
               "schemaFile": s,
               "clusteredIndex": "IDX_objectId",
               "overlap": "0",
               "phiColName": "ra",
               "thetaColName": "decl",
               "logicalPart": "1",
               "physChunking": "0x0011" }
        self._client.createTable("Summer2012", dd)

        print "=====>> create table RefObjMatch"
        s = "%s/tbSchema_RefObjMatch.sql" % self._baseDir
        dd = { #"tableName": "Source", # get table name from schema file
               "partitioning": "on",
               "schemaFile": s,
               "overlap": "0",
               "phiColName": "refRa",
               "thetaColName": "refDec",
               "logicalPart": "1",
               "physChunking": "0x0011",
               "isRefMatch": "yes" }
        try:
            self._client.createTable("Summer2012", dd)
        except QmsException as q:
            print q.getErrMsg()

        print "=====>> retrieve table info (Source) "
        print self._client.retrieveTableInfo("Summer2012", "Source")

        print "=====>> retrieve table info (Exposure)"
        print self._client.retrieveTableInfo("Summer2012", "Exposure")

        print "=====>> retrieve table info (RefObjMatch)"
        print self._client.retrieveTableInfo("Summer2012", "RefObjMatch")

        print "=====>> list all tables"
        print self._client.listTables("Summer2012")

        print "=====>> list partitioned tables"
        print self._client.listPartitionedTables("Summer2012")

        print "=====>> drop table Object"
        self._client.dropTable("Summer2012", "Object")

        print "=====>> drop db"
        self._client.dropDb("Summer2012")

    ###########################################################################
    #### test_badParams
    ###########################################################################
    def test_badParam(self):
        print "=====>> install meta"
        self._client.installMeta()

        print "=====>> try create db (missing parameters, 3 tries)"
        dd = {"partitioning": "on",
              "partitioningStrategy": "sphBox",
              #"nStripes": "10",
              "nSubStripes": "23",
              "defaultOverlap_fuzziness": "0.0001",
              "defaultOverlap_nearNeighbor": "0.25"}
        try:
            self._client.createDb("d1", dd)
        except QmsException as qe: pass
        else: self.fail("createDb(d1) Exception not thrown")

        dd = {"partitioning": "on",
              "partitioningStrategy": "sphBox",
              "nStripes": "10",
              #"nSubStripes": "23",
              "defaultOverlap_fuzziness": "0.0001",
              "defaultOverlap_nearNeighbor": "0.25"}
        try:
            self._client.createDb("d1", dd)
        except QmsException as qe: pass
        else: self.fail("createDb nSS: Exception not thrown")

        dd = {"partitioning": "on",
              "partitioningStrategy": "sphBox",
              "nStripes": "10",
              "nSubStripes": "23",
              #"defaultOverlap_fuzziness": "0.0001",
              "defaultOverlap_nearNeighbor": "0.25"}
        try:
            self._client.createDb("d1", dd)
        except QmsException as qe: pass
        else: self.fail("createDb dof: Exception not thrown")

        print "=====>> try create db (extra parameter)"
        dd = {"partitioning": "on",
              "partitioningStrategy": "sphBox",
              "nStripes": "10",
              "nSubStripes": "23",
              "defaultOverlap_fuzziness": "0.0001",
              "defaultOverlap_nearNeighbor": "0.25",
              "sthExtra": "abc"}
        try:
            self._client.createDb("d1", dd)
        except QmsException as qe: pass
        else: self.fail("createDb extraParam: Exception not thrown")

        print "=====>> try retrieveDb info (non-existing db)"
        try:
            self._client.retrieveDbInfo("drer")
        except QmsException as qe: pass
        else: self.fail("retrieveDb('bad'): Exception not thrown")

        print "=====>> try dropping table (does not exist)"
        try:
            self._client.dropTable("Summer2012", "Object")
        except QmsException as qe: pass
        else: self.fail("dropTable('bad'): Exception not thrown")

        print "=====>> try create table with invalid column"
        s = "%s/tbSchema_Object.sql" % self._baseDir
        dd = { "tableName": "Object",
               "partitioning": "on",
               "schemaFile": s,
               "clusteredIndex": "IDX_objectId",
               "overlap": "0.025",
               "phiColName": "dfadfd", # this is invalid
               "thetaColName": "decl_PS",
               "logicalPart": "2",
               "physChunking": "0x0021" }
        try:
            self._client.createTable("Summer2012", dd)
        except QmsException as qe: pass
        else: self.fail("createTable invParam: Exception not thrown")

        print "=====>> try create table in non-existing db"
        s = "%s/tbSchema_Object.sql" % self._baseDir
        dd = { "tableName": "Object",
               "partitioning": "on",
               "schemaFile": s,
               "clusteredIndex": "IDX_objectId",
               "overlap": "0.025",
               "phiColName": "ra_PS",
               "thetaColName": "decl_PS",
               "logicalPart": "2",
               "physChunking": "0x0021" }
        try:
            self._client.createTable("Sudfdfd2", dd)
        except QmsException as qe: pass
        else: self.fail("createTable invalidDb: Exception not thrown")

        print "=====>> create Summer2012 db"
        dd = {"partitioning": "on",
              "partitioningStrategy": "sphBox",
              "nStripes": "10",
              "nSubStripes": "23",
              "defaultOverlap_fuzziness": "0.0001",
              "defaultOverlap_nearNeighbor": "0.25"}
        self._client.createDb("Summer2012", dd)

        print "=====>> try create table with non-existing schema file"
        s = "%s/tbScdfad fadsf.sql" % self._baseDir # this is wrong
        dd = { "tableName": "Object",
               "partitioning": "on",
               "schemaFile": s,
               "clusteredIndex": "IDX_objectId",
               "overlap": "0.025",
               "phiColName": "ra_PS",
               "thetaColName": "decl_PS",
               "logicalPart": "2",
               "physChunking": "0x0021" }
        try:
            self._client.createTable("Summer2012", dd)
        except QmsException as qe: pass
        else: self.fail("createTable badSchemaF: Exception not thrown")

        print "=====>> try retrieve table info (db does not exist)"
        try:
            self._client.retrieveTableInfo("Sudfd2012", "Exposure")
        except QmsException as qe: pass
        else: self.fail("retrieveTInfo badDb: Exception not thrown")

        print "=====>> try retrieve table info (table does not exist)"
        try:
            self._client.retrieveTableInfo("Summer2012", "Edfdfd")
        except QmsException as qe: pass
        else: self.fail("retrieveTInfo badT: Exception not thrown")

        print "=====>> try drop invalid db"
        try:
            self._client.dropDb("Sudfdmmer2012")
        except QmsException as qe: pass
        else: self.fail("dropDb badDb: Exception not thrown")

        print "=====>> destroy meta"
        self._client.destroyMeta()

        print "=====>> try destroying non-existing meta"
        try:
            self._client.destroyMeta()
        except QmsException as qe: pass
        else: self.fail("destroy #2: Exception not thrown")

    def _getCachedConnInfo(self):
        fName = os.getenv("metaTestAuthFile")
        if fName is None:
            fName = os.path.expanduser("~/.qmsadm")

        config = ConfigParser.ConfigParser()
        config.read(fName)
        s = "qmsConn"
        if not config.has_section(s):
            raise QmsException(Status.ERR_INVALID_OPTION, 
                               "Can't find section '%s' in %s" % (s, fName))
        if not config.has_option(s, "host") or \
           not config.has_option(s, "port") or \
           not config.has_option(s, "user") or \
           not config.has_option(s, "pass"):
            raise QmsException(Status.ERR_INVALID_OPTION,
                               "Bad %s, can't find host, port, user or pass" \
                                   % fName)
        return (config.get(s, "host"), config.getint(s, "port"),
                config.get(s, "user"), config.get(s, "pass"))

###############################################################################
##### main
###############################################################################
if __name__ == '__main__':
    unittest.main()
