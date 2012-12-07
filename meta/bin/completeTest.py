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


import ConfigParser
import os
import sys

from lsst.qserv.meta.status import Status
from lsst.qserv.meta.client import Client

class Tester(object):
    def __init__(self, baseDir):
        (host, port, user, pwd) = self._getCachedConnInfo()
        self._client = Client(host, port, user, pwd)
        self._baseDir = baseDir

    def doIt(self):
        # initial cleanup
        try:
            self._client.destroyMeta()
        except Exception, e:
            pass

        print "=====>> print meta"
        try:
            print self._client.printMeta()
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("printMeta should fail, no meta")

        print "=====>> install meta"
        self._client.installMeta()

        print "=====>> list dbs"
        print self._client.listDbs()

        print "=====>> print meta"
        print self._client.printMeta()

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
        try:
            self._client.createDb("Summer2012", dd)
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("create db that already exists should fail")

        print "=====>> try create db (missing parameters, 3 tries)"
        try:
            dd = {"partitioning": "on",
                  "partitioningStrategy": "sphBox",
                  #"nStripes": "10",
                  "nSubStripes": "23",
                  "defaultOverlap_fuzziness": "0.0001",
                  "defaultOverlap_nearNeighbor": "0.25"}
            self._client.createDb("d1", dd)
        except Exception, e:
            print "Caught as expected:", str(e)
        try:
            dd = {"partitioning": "on",
                  "partitioningStrategy": "sphBox",
                  "nStripes": "10",
                  #"nSubStripes": "23",
                  "defaultOverlap_fuzziness": "0.0001",
                  "defaultOverlap_nearNeighbor": "0.25"}
            self._client.createDb("d1", dd)
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("createdb with missing param should fail")

        try:
            dd = {"partitioning": "on",
                  "partitioningStrategy": "sphBox",
                  "nStripes": "10",
                  "nSubStripes": "23",
                  #"defaultOverlap_fuzziness": "0.0001",
                  "defaultOverlap_nearNeighbor": "0.25"}
            self._client.createDb("d1", dd)
        except Exception, e:
            print "Caught as expected:", str(e)
        else: raise Exception("createdb with missing param2 should fail")

        print "=====>> try create db (extra parameter)"
        try:
            dd = {"partitioning": "on",
                  "partitioningStrategy": "sphBox",
                  "nStripes": "10",
                  "nSubStripes": "23",
                  "defaultOverlap_fuzziness": "0.0001",
                  "defaultOverlap_nearNeighbor": "0.25",
                  "sthExtra": "abc"}
            self._client.createDb("d1", dd)
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("createdb with extra param should fail")

        print "=====>> create non-partitioned db"
        dd = {"partitioning": "off"}
        self._client.createDb("NonPart", dd)

        print "=====>> list dbs"
        print self._client.listDbs()

        print "check if db exists"
        if not self._client.checkDbExists("Summer2012"):
            raise "Db 'Summer2012' should exist!!!"

        print "check if db exists"
        if self._client.checkDbExists("xerd"):
            raise "Db 'xer2d' should not exist!!!"

        print "=====>> retrieveDb info for partitioned db"
        print self._client.retrieveDbInfo("Summer2012")

        print "=====>> retrieveDb info for non-partitioned db"
        print self._client.retrieveDbInfo("NonPart")

        print "=====>> try retrieveDb info (non-existing db)"
        try:
            self._client.retrieveDbInfo("drer")
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("retrieveDb for non-existing db should fail")

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
               "physChunking": "0x0021" }
        self._client.createTable("Summer2012", dd)

        print "=====>> try create table (already exists)"
        try:
            self._client.createTable("Summer2012", dd)
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("createTable that already exists should fail")
            
        print "=====>> create table Exposure"
        s = "%s/tbSchema_Exposure.sql" % self._baseDir
        dd = { "tableName": "Exposure",
               "partitioning": "off",
               "schemaFile": s }
        self._client.createTable("Summer2012", dd)

        print "=====>> create table Source"
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

        print "=====>> print meta"
        print self._client.printMeta()

        print "=====>> drop table Object"
        self._client.dropTable("Summer2012", "Object")

        print "=====>> print meta"
        print self._client.printMeta()

        print "=====>> try dropping table (does not exist)"
        try:
            self._client.dropTable("Summer2012", "Object")
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("drop non-existing table should fail")

        print "=====>> try create table with invalid column"
        try:
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
            self._client.createTable("Summer2012", dd)
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("createT with non-existing column should fail")

        print "=====>> try create table in non-existing db"
        try:
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
            self._client.createTable("Sudfdfd2", dd)
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("create table for non-existing db should fail")

        print "=====>> try create table with non-existing schema file"
        try:
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
            self._client.createTable("Summer2012", dd)
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("create table with non-existing schema should fail")

        print "=====>> retrieve table info (Source) "
        self._client.retrieveTableInfo("Summer2012", "Source")

        print "=====>> retrieve table info (Exposure)"
        v = self._client.retrieveTableInfo("Summer2012", "Exposure")
        if v is None: print "NOOONE"
        print "got in compltest: ", v

        print "=====>> try retrieve table info (db does not exist)"
        try:
            self._client.retrieveTableInfo("Sudfd2012", "Exposure")
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("retrieveTInfo for non-existing db should fail")

        print "=====>> try retrieve table info (table does not exist)"
        try:
            self._client.retrieveTableInfo("Summer2012", "Edfdfd")
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("retrieveTInfo for non-existing tb should fail")

        print "=====>> drop db"
        self._client.dropDb("Summer2012")

        print "=====>> print meta"
        self._client.printMeta()

        print "=====>> try drop invalid db"
        try:
            self._client.dropDb("Sudfdmmer2012")
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("drop non-existing db should fail")

        print "=====>> destroy meta"
        self._client.destroyMeta()

        print "=====>> try destroying non-existing meta"
        try:
            self._client.destroyMeta()
        except Exception, e: print "Caught as expected:", str(e)
        else: raise Exception("destroy non-existing meta should fail")

        print "\n\n   Nice job! :)\n\n"


    def _getCachedConnInfo(self):
        fName = os.path.expanduser("~/.qmsadm")
        config = ConfigParser.ConfigParser()
        config.read(fName)
        s = "qmsConn"
        if not config.has_section(s):
            raise Exception("Can't find section '%s' in %s" % (s, fName))
        if not config.has_option(s, "host") or \
           not config.has_option(s, "port") or \
           not config.has_option(s, "user") or \
           not config.has_option(s, "pass"):
            raise Exception("Bad %s, can't find host, port, user or pass" \
                                % fName)
        return (config.get(s, "host"), config.getint(s, "port"),
                config.get(s, "user"), config.get(s, "pass"))

###############################################################################
##### main
###############################################################################
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Expecting one argument: base directory"
    else:
        baseDir = "%s/meta/examples" % sys.argv[1]
        try:
            t = Tester(baseDir)
            t.doIt()
        except Exception, e:
            print "\n\n*****TEST FAILED***** : ", str(e)
