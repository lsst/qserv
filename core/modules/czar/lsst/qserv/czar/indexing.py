# 
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

# indexing module for lsst.qserv.czar
#
# The indexing module handles non-spatial column indexing. Non-spatial
# column indexing allows qserv to convert non-spatial conditions in
# WHERE clauses into spatial restrictions.  
# 
# The motivation for this feature is for efficiency in handling
# queries for objects based on objectId. A narrow table is held on
# the qserv frontend that is indexed on the non-spatial column.  Qserv
# queries this table to find the chunkId for each objectId.
# Then qserv can generate and issue queries for only those chunks that
# host the desired objectIds.  This cuts down on worker load and
# overall execution time.  Given indexes on objectId on the workers,
# the dominant cost in execution is query dispatch and result
# downloading overhead, so reducing unnecessary
# dispatch/result-download is a big win for performance.
#

# Pkg imports.
import app
import config
import metadata
import logger
from db import Db

class Indexer:
    def __init__(self):
        self.pmap = app.makePmap()
        
    def setupIndexes(self):
        p = PartitionGroup()
        db = Db()
        db.activate()
        db.makeIfNotExist(db=metadata.getMetaDbName())
        #logger.inf(p.tables)
        for (t,d) in p.tables.items():
            if d.has_key("index"):
                self._makeIndex(t, p.partitionCols, d["index"])
        
    def _makeIndex(self, table, pCols, iCols):
        selCols = ",".join(iCols)
        q = "SELECT %s FROM %s;" % (selCols, table)
        indexName = metadata.getIndexNameForTable(table)
        # might need to drop this table.

        # configure merger to drop ENGINE=MEMORY from merged table.
        config.config.set("resultdb","dropMem", "yes")
        # configure qserv to buffer in a big place
        #config.config.set("frontend", "scratch_path","/tmp/qsIndex")

        db = Db()
        db.activate()
        db.applySql("DROP TABLE IF EXISTS %s;" %(indexName)) #make room first.
        a = app.HintedQueryAction(q, {"db" : metadata.getMetaDbName()}, 
                                  self.pmap, 
                                  lambda e: None, indexName)
        
        assert a.getIsValid()
        logger.inf("Gathering objectId/chunkId locality from workers")
        logger.inf(a.invoke())
        logger.inf(a.getResult())
        logger.inf("Retrieved result.")
        for i in iCols:
            if i in pCols: continue
            logger.inf("creating index for", i)
            iq = "ALTER TABLE %s ADD INDEX (%s);" % (indexName, i)
            logger.inf(iq)
            cids = db.applySql(iq)
            cids = map(lambda t: t[0], cids)
            del db
            logger.inf(cids)
    pass
    
def makeQservIndexes():
    i = Indexer()
    i.setupIndexes()

    

class PartitionGroup:
    # Hardcode for now.  Should merge with parts of the configuration
    # or split out into a more general qserv metadata system.
    def __init__(self): 
        self.partitionCols = ["x_chunkId","x_subChunkId"]
        self.tables = {"LSST.Object" : {"index" : ["objectId", 
                                                   "x_chunkId", 
                                                   "x_subChunkId"],
                                        "partition" : ["x_chunkId",
                                                       "x_subChunkId"]},
                       "LSST.Source" : {"partition" : ["x_chunkId"]}
                       }
        pass

