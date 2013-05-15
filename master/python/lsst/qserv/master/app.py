# 
# LSST Data Management System
# Copyright 2008-2013 LSST Corporation.
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

# app module for lsst.qserv.master
#
# The app module can be thought-of as the top-level code module for
# the qserv frontend's functionality.  It implements the "interesting"
# code that accepts an incoming query, parses it, generates
# subqueries, dispatches to workers, collects results, and returns to
# the caller, marshalling code from other modules as necessary.
# 
# This is the  "high-level application logic" and glue of the qserv
# master/frontend.  
#
# Warning: Older code for older/alternate parsing models and
# older/alternate dispatch/collection models remains here, in
# anticipation of supporting performance investigation.  The most
# current functionality can be understood by examining the
# HintedQueryAction class, QueryBabysitter class, and other related
# classes. 
# 

# Standard Python imports
import errno
import hashlib
from itertools import chain, imap
import os
import cProfile as profile
import pstats
import random
import re
from subprocess import Popen, PIPE
import sys
import threading
import time
from string import Template

# Package imports
import metadata
import lsst.qserv.master.config
from lsst.qserv.master import geometry
from lsst.qserv.master.geometry import SphericalBox
from lsst.qserv.master.geometry import SphericalConvexPolygon, convexHull
from lsst.qserv.master.db import TaskDb as Persistence
from lsst.qserv.master.db import Db
from lsst.qserv.master import protocol
from lsst.qserv.master.spatial import getSpatialConfig, getRegionFactory

from lsst.qserv.meta.status import Status, QmsException
from lsst.qserv.meta.client import Client

# SWIG'd functions

# xrdfile - raw xrootd access
from lsst.qserv.master import xrdOpen, xrdClose, xrdRead, xrdWrite
from lsst.qserv.master import xrdLseekSet, xrdReadStr
from lsst.qserv.master import xrdReadToLocalFile, xrdOpenWriteReadSaveClose

from lsst.qserv.master import charArray_frompointer, charArray

# transaction
from lsst.qserv.master import TransactionSpec

# Dispatcher 
from lsst.qserv.master import newSession, discardSession
from lsst.qserv.master import setupQuery, getSessionError
from lsst.qserv.master import getConstraints, addChunk, ChunkSpec
from lsst.qserv.master import getDominantDb
from lsst.qserv.master import configureSessionMerger3, submitQuery3


from lsst.qserv.master import submitQuery, submitQueryMsg
from lsst.qserv.master import initDispatcher
from lsst.qserv.master import tryJoinQuery, joinSession
from lsst.qserv.master import getQueryStateString, getErrorDesc
from lsst.qserv.master import SUCCESS as QueryState_SUCCESS
from lsst.qserv.master import pauseReadTrans, resumeReadTrans
# Parser
from lsst.qserv.master import ChunkMeta
# Merger
from lsst.qserv.master import TableMerger, TableMergerError, TableMergerConfig
from lsst.qserv.master import configureSessionMerger, getSessionResultName

# Metadata
from lsst.qserv.master import newMetadataSession, discardMetadataSession
from lsst.qserv.master import addDbInfoNonPartitioned
from lsst.qserv.master import addDbInfoPartitionedSphBox
from lsst.qserv.master import addTbInfoNonPartitioned
from lsst.qserv.master import addTbInfoPartitionedSphBox
from lsst.qserv.master import printMetadataCache

# Experimental interactive prompt (not currently working)
import code, traceback, signal


# Constant, long-term, this should be defined differently
dummyEmptyChunk = 1234567890


def debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d={'_frame':frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message  = "Signal recieved : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    i.interact(message)

def listen():
    signal.signal(signal.SIGUSR1, debug)  # Register handler
listen()


######################################################################
## Methods
######################################################################
## MySQL interface helpers
def computeShortCircuitQuery(query, conditions):
    if query == "select current_user()":
        return ("qserv@%","","")
    return # not a short circuit query.

## Helpers
def getResultTable(tableName):
    """A convenience function that uses the mysql client to get quick 
    results."""
    # Minimal sanitizing
    tableName = tableName.strip()
    assert not filter(lambda x: x in tableName, ["(",")",";"])
    sqlCmd = "SELECT * FROM %s;" % tableName

    # Get config
    config = lsst.qserv.master.config.config
    socket = config.get("resultdb", "unix_socket")
    db = config.get("resultdb", "db")
    mysql = config.get("mysql", "mysqlclient")
    if not mysql:
        mysql = "mysql"

    # Execute
    cmdList = [mysql, "--socket", socket, db]
    p = Popen(cmdList, bufsize=0, 
              stdin=PIPE, stdout=PIPE, close_fds=True)
    (outdata,errdummy) = p.communicate(sqlCmd)
    p.stdin.close()
    if p.wait() != 0:
        return "Error getting table %s." % tableName, outdata
    return outdata

def loadEmptyChunks(filename):
    def tolerantInt(i):
        try:
            return int(i)
        except:
            return None
    empty = set()
    try:
        if filename:
            s = open(filename).read()
            empty = set(map(tolerantInt, s.split("\n")))
    except:
        print filename, "not found while loading empty chunks file."
        return None
    return empty
    
    
######################################################################
## Classes
######################################################################

_defaultMetadataCacheSessionId = None

class MetadataCacheIface:
    """MetadataCacheIface encapsulates logic to prepare, metadata 
       information by fetching it from qserv metadata server into 
       C++ memory structure. It is stateless. Throws exceptions on
       failure."""

    def getDefaultSessionId(self):
        """Returns default sessionId. It will initialize it if needed.
           Note: initialization involves contacting qserv metadata
           server (qms). This is the only time qserv talks to qms. 
           This function throws QmsException if case of problems."""
        global _defaultMetadataCacheSessionId
        if _defaultMetadataCacheSessionId is None:
            _defaultMetadataCacheSessionId = self.newSession()
            self.printSession(_defaultMetadataCacheSessionId)
        return _defaultMetadataCacheSessionId

    def newSession(self):
        """Creates a new session: assigns sessionId, populates the C++
           cache and returns the sessionId."""
        sessionId = newMetadataSession()
        qmsClient = Client(
            lsst.qserv.master.config.config.get("metaServer", "host"),
            int(lsst.qserv.master.config.config.get("metaServer", "port")),
            lsst.qserv.master.config.config.get("metaServer", "user"),
            lsst.qserv.master.config.config.get("metaServer", "passwd"))
        self._fetchAllData(sessionId, qmsClient)
        return sessionId

    def printSession(self, sessionId):
        printMetadataCache(sessionId)

    def discardSession(self, sessionId):
        discardMetadataSession(sessionId)

    def _fetchAllData(self, sessionId, qmsClient):
        dbs = qmsClient.listDbs()
        for dbName in dbs:
            partStrategy = self._addDb(dbName, sessionId, qmsClient);
            tables = qmsClient.listTables(dbName)
            for tableName in tables:
                self._addTable(dbName, tableName, partStrategy, sessionId, qmsClient)

    def _addDb(self, dbName, sessionId, qmsClient):
        # retrieve info about each db
        x = qmsClient.retrieveDbInfo(dbName)
        # call the c++ function
        if x["partitioningStrategy"] == "sphBox":
            #print "add partitioned, ", db, x
            ret = addDbInfoPartitionedSphBox(
                sessionId, dbName,
                int(x["stripes"]), 
                int(x["subStripes"]), 
                float(x["defaultOverlap_fuzziness"]), 
                float(x["defaultOverlap_nearNeigh"]))
        elif x["partitioningStrategy"] == "None":
            #print "add non partitioned, ", db
            ret = addDbInfoNonPartitioned(sessionId, dbName)
        else:
            raise QmsException(Status.ERR_INVALID_PART)
        if ret != 0:
            if ret == -1: # the dbInfo does not exist
                raise QmsException(Status.ERR_DB_NOT_EXISTS)
            if ret == -2: # the table is already there
                raise QmsException(Status.ERR_TABLE_EXISTS)
            raise QmsException(Status.ERR_INTERNAL)
        return x["partitioningStrategy"]

    def _addTable(self, dbName, tableName, partStrategy, sessionId, qmsClient):
        # retrieve info about each db
        x = qmsClient.retrieveTableInfo(dbName, tableName)
        # call the c++ function
        if partStrategy == "sphBox": # db is partitioned
            if "overlap" in x:       # but this table does not have to be
                ret = addTbInfoPartitionedSphBox(
                    sessionId, 
                    dbName,
                    tableName, 
                    float(x["overlap"]),
                    x["phiCol"],
                    x["thetaCol"],
                    x["objIdCol"],
                    int(x["phiColNo"]),
                    int(x["thetaColNo"]),
                    int(x["objIdColNo"]),
                    int(x["logicalPart"]),
                    int(x["physChunking"]))
            else:                    # db is not partitioned
                ret = addTbInfoNonPartitioned(sessionId, dbName, tableName)
        elif partStrategy == "None":
            ret = addTbInfoNonPartitioned(sessionId, dbName, tableName)
        else:
            raise QmsException(Status.ERR_INVALID_PART)
        if ret != 0:
            if ret == -1: # the dbInfo does not exist
                raise QmsException(Status.ERR_DB_NOT_EXISTS)
            if ret == -2: # the table is already there
                raise QmsException(Status.ERR_TABLE_EXISTS)
            raise QmsException(Status.ERR_INTERNAL)

########################################################################

class TaskTracker:
    def __init__(self):
        self.tasks = {}
        self.pers = Persistence()
        pass

    def track(self, name, task, querytext):
        # create task in db with name
        taskId = self.pers.addTask((None, querytext))
        self.tasks[taskId] = {"task" : task}
        return taskId
    
    def task(self, taskId):
        return self.tasks[taskId]["task"]

########################################################################

class SleepingThread(threading.Thread):
    def __init__(self, howlong=1.0):
        self.howlong=howlong
        threading.Thread.__init__(self)
    def run(self):
        time.sleep(self.howlong)

########################################################################
class QueryHintError(Exception):
    """An error in handling query hints"""
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return repr(self.reason)

def setupResultScratch():
    # Make sure scratch directory exists.
    cm = lsst.qserv.master.config
    c = lsst.qserv.master.config.config
    
    scratchPath = c.get("frontend", "scratch_path")
    try: # Make sure the path is there
        os.makedirs(scratchPath)
    except OSError, exc: 
        if exc.errno == errno.EEXIST:
            pass
        else: 
            raise cm.ConfigError("Bad scratch_dir")
    # Make sure we can read/write the dir.
    if not os.access(scratchPath, os.R_OK | os.W_OK):
        raise cm.ConfigError("No access for scratch_path(%s)" % scratchPath)
    return scratchPath

########################################################################    
class InbandQueryAction:
    """InbandQueryAction is an action which represents a user-query
    that is executed using qserv in many pieces. It borrows a little
    from HintedQueryAction, but uses different abstractions
    underneath.
    """
    def __init__(self, query, hints, pmap, reportError, resultName=""):
        self.queryStr = query.strip()# Pull trailing whitespace
        # Force semicolon to facilitate worker-side splitting
        if self.queryStr[-1] != ";":  # Add terminal semicolon
            self.queryStr += ";" 

        # queryHash identifies the top-level query.
        self.queryHash = self._computeHash(self.queryStr)[:18]
        self.chunkLimit = 2**32 # something big
        self.isValid = False

        self.hints = hints
        self.pmap = pmap

        self._importQconfig()
        self._invokeLock = threading.Semaphore()
        self._invokeLock.acquire() # Prevent res-retrieval before invoke
        self._resultName = resultName
        self._reportError = reportError
        self.isValid = True
        self._metaCacheSession = MetadataCacheIface().getDefaultSessionId()
        pass

    def invoke(self):
        self._prepareForExec()
        self._execAndJoin()
        self._invokeLock.release()

    def getError(self):
        try:
            return self._error
        except:
            return "Unknown error InbandQueryAction"

    def getResult(self):
        """Wait for query to complete (as necessary) and then return 
        a handle to the result."""
        self._invokeLock.acquire()
        # Make sure it's joined.
        self._invokeLock.release()
        return self._resultName

    def getIsValid(self):
        return self.isValid

    def pauseReadback(self): 
        """pause readback of results for this session. 
        FIXME is this needed in the C++-managed dispatch scheme?"""
        pauseReadTrans(self.sessionId)
        pass

    def resumeReadback(self):
        resumeReadTrans(self.sessionId)
        pass

    def _computeHash(self, bytes):
        return hashlib.md5(bytes).hexdigest()

    def _prepareForExec(self):
        self.hints = self.hints.copy() # make a copy
        self._dbContext = self.hints.get("db", "")

        cfg = self._prepareCppConfig()
        self.sessionId = newSession(cfg)
        setupQuery(self.sessionId, self.queryStr, self._resultName)
        errorMsg = getSessionError(self.sessionId)
        # TODO: Handle error more gracefully.
        assert not getSessionError(self.sessionId)

        self._applyConstraints()
        self._prepareMerger()
        pass

    def _evaluateHints(self, hints, pmap):
        """Modify self.fullSky and self.partitionCoverage according to 
        spatial hints. This is copied from older parser model"""
        self._isFullSky = True
        self._intersectIter = pmap
        if hints:
            regions = self._computeRegions(hints)
            self._dbContext = hints.get("db", "")
            ids = hints.get("objectId", "")
            if regions != []:
                # Remove the region portion from the intersection tuple
                self._intersectIter = map(
                    lambda i: (i[0], map(lambda j:j[0], i[1])),
                    pmap.intersect(regions))
                self._isFullSky = False
            if ids:
                chunkIds = self._getChunkIdsFromObjs(ids)
                if regions != []:
                    self._intersectIter = chain(self._intersectIter, chunkIds)
                else:
                    self._intersectIter = map(lambda i: (i,[]), chunkIds)
                self._isFullSky = False
                if not self._intersectIter:
                    self._intersectIter = [(dummyEmptyChunk, [])]
        # _isFullSky indicates that no spatial hints were found.
        # However, if spatial tables are not found in the query, then
        # we should use the dummy chunk so the query can be dispatched
        # once to a node of the balancer's choosing.
                    
        # If hints only apply when partitioned tables are in play.
        # FIXME: we should check if partitionined tables are being accessed,
        # and then act to support the heaviest need (e.g., if a chunked table
        # is being used, then issue chunked queries).
        #print "Affected chunks: ", [x[0] for x in self._intersectIter]
        pass

    def _applyConstraints(self):
        # Retrieve constraints as (name, [param1,param2,param3,...])        
        self.constraints = getConstraints(self.sessionId)
        #print "Getting constraints", self.constraints, "size=",self.constraints.size()
        # Apply constraints
        def iterateConstraints(constraintVec):
            for i in range(constraintVec.size()):
                yield constraintVec.get(i)
        for constraint in iterateConstraints(self.constraints):
            print "constraint=", constraint
            params = [constraint.paramsGet(i) 
                      for i in range(constraint.paramsSize())]
            self.hints[constraint.name] = params
            pass 
        self._evaluateHints(self.hints, self.pmap)
        dominantDb = getDominantDb(self.sessionId)
        self._emptyChunks = metadata.getEmptyChunks(dominantDb)
        count = 0
        chunkLimit = self.chunkLimit
        for chunkId, subIter in self._intersectIter:
            if chunkId in self._emptyChunks:
                continue
            #prepare chunkspec
            c = ChunkSpec()
            c.chunkId = chunkId
            scount=0
            sList = [s for s in subIter]
            #for s in sList: # debugging
            #    c.addSubChunk(s)
            #    scount += 1
            #    if scount > 7: break ## FIXME: debug now.
            map(c.addSubChunk, sList)
            addChunk(self.sessionId, c)
            count += 1
            if count >= chunkLimit: break
        if count == 0:
            addChunk(dummyEmpty)
        pass

    def _execAndJoin(self):
        lastTime = time.time()
        submitQuery3(self.sessionId)
        elapsed = time.time() - lastTime
        print "Query dispatch (%s) took %f seconds" % (self.sessionId,
                                                       elapsed)
        lastTime = time.time()
        s = joinSession(self.sessionId)
        elapsed = time.time() - lastTime
        print "Query exec (%s) took %f seconds" % (self.sessionId,
                                                   elapsed)

        if s != QueryState_SUCCESS:
            self._reportError(getErrorDesc(self.sessionId))
        print "Final state of all queries", getQueryStateString(s)
        if not self.isValid:
            discardSession(self.sessionId)
            return

    def _importQconfig(self):
        """Import config file settings into self"""
        # Config preparation
        cModule = lsst.qserv.master.config

        # chunk limit: For debugging
        cfgLimit = int(cModule.config.get("debug", "chunkLimit"))
        if cfgLimit > 0:
            self.chunkLimit = cfgLimit
            print "Using debugging chunklimit:",cfgLimit

        # Memory engine(unimplemented): Buffer results/temporaries in
        # memory on the master. (no control over worker) 
        self._useMemory = cModule.config.get("tuning", "memoryEngine")
        return True
    
    def _prepareCppConfig(self):
        """Construct a C++ stringmap for passing settings and context
        to the C++ layer."""
        cfg = lsst.qserv.master.config.getStringMap()
        cfg["frontend.scratchPath"] = setupResultScratch()
        cfg["table.defaultdb"] = self._dbContext
        cfg["query.hints"] = ";".join(
            map(lambda (k,v): k + "," + v, self.hints.items()))
        cfg["table.result"] = self._resultName
        return cfg

    def _computeRegions(self, hints):
        r = getRegionFactory()
        regs = r.getRegionFromHint(hints)
        if regs != None:
            return regs
        else:
            if r.errorDesc:
                # How can we give a good error msg to the client?
                s = "Error parsing hint string %s"
                raise QueryHintError(s % r.errorDesc)
            return []
        pass

    def _prepareMerger(self):
        c = lsst.qserv.master.config.config
        dbSock = c.get("resultdb", "unix_socket")
        dbUser = c.get("resultdb", "user")
        dbName = c.get("resultdb", "db")        
        dropMem = c.get("resultdb","dropMem")

        mysqlBin = c.get("mysql", "mysqlclient")
        if not mysqlBin:
            mysqlBin = "mysql"
        configureSessionMerger3(self.sessionId)
        pass

    def _getChunkIdsFromObjs(self, ids):
        """ FIXME: objectID indexing not supported yet"""
        table = metadata.getIndexNameForTable("LSST.Object")
        objCol = "objectId"
        chunkCol = "x_chunkId"
        try:
            test = ",".join(map(str, map(int, ids.split(","))))
            chopped = filter(lambda c: not c.isspace(), ids)
            assert test == chopped
        except Exception, e:
            print "Error converting objectId spec. ", ids, "Ignoring.",e
            #print test,"---",chopped
            return []
        sql = "SELECT %s FROM %s WHERE %s IN (%s);" % (chunkCol, table,
                                                       objCol, ids)
        db = Db()
        db.activate()
        cids = db.applySql(sql)
        cids = map(lambda t: t[0], cids)
        del db
        return cids
        
    pass # class InbandQueryAction

class CheckAction:
    def __init__(self, tracker, handle):
        self.tracker = tracker
        self.texthandle = handle
        pass
    def invoke(self):
        self.results = None
        id = int(self.texthandle)
        t = self.tracker.task(id)
        if t: 
            self.results = 50 # placeholder. 50%

########################################################################
########################################################################
########################################################################

#see if it's better to not bother with an action object
def results(tracker, handle):
        id = int(handle)
        t = tracker.task(id)
        if t:
            return "Some host with some port with some db"
        return None

def clauses(col, cmin, cmax):
    return ["%s between %smin and %smax" % (cmin, col, col),
            "%s between %smin and %smax" % (cmax, col, col),
            "%smin between %s and %s" % (col, cmin, cmax)]

# Watch out for memory errors:
# Exception in thread Thread-2897:
# Traceback (most recent call last):
#   File "/home/wang55/scratch/s/Linux/external/python/2.5.2/lib/python2.5/threading.py", line 486, in __bootstrap_inner
#     self.run()
#   File "/home/wang55/5node/m121/lsst/qserv/master/app.py", line 192, in run
#     buf = "".center(bufSize) # Fill buffer
# MemoryError
