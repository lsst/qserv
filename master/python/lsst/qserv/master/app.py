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
from lsst.qserv.master.geometry import SphericalBox, SphericalBoxPartitionMap
from lsst.qserv.master.geometry import SphericalConvexPolygon, convexHull
from lsst.qserv.master.db import TaskDb as Persistence
from lsst.qserv.master.db import Db
from lsst.qserv.master import protocol
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
from lsst.qserv.master import submitQuery, submitQueryMsg
from lsst.qserv.master import initDispatcher
from lsst.qserv.master import tryJoinQuery, joinSession
from lsst.qserv.master import getQueryStateString, getErrorDesc
from lsst.qserv.master import SUCCESS as QueryState_SUCCESS
from lsst.qserv.master import pauseReadTrans, resumeReadTrans
# Parser
from lsst.qserv.master import ChunkMeta
from lsst.qserv.master import ChunkMapping, SqlSubstitution
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
def makePmap():
    c = lsst.qserv.master.config.config
    stripes = c.getint("partitioner", "stripes")
    substripes = c.getint("partitioner", "substripes")
    if (stripes < 1) or (substripes < 1):
        msg = "Partitioner's stripes and substripes must be natural numbers."
        raise lsst.qserv.master.config.ConfigError(msg)
    p = SphericalBoxPartitionMap(stripes, substripes) 
    print "Using %d stripes and %d substripes." % (stripes, substripes)
    return p

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
    
    
######################################################################
## Classes
######################################################################

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

class XrdOperation(threading.Thread):
        def __init__(self, chunk, query, outputFunc, outputArg, resultPath):
            threading.Thread.__init__(self)

            self._chunk = chunk
            self._query = query
            self._queryLen = len(query)
            self._outputFunc = outputFunc
            self._outputArg = outputArg
            self._url = ""
            self._handle = None
            self._resultBufferList = []
            self._usingCppTransaction = True
            self._usingXrdReadTo = True
            self._targetName = os.path.join(resultPath, self._outputArg)
            self._readSize = 8192000

            self.successful = None
            self.profileName = "/tmp/qserv_chunk%d_profile" % chunk # Public.

            self.setXrd()
            pass

        def setXrd(self):
            hostport = os.getenv("QSERV_XRD","lsst-dev01:1094")
            user = "qsmaster"
            self._url = "xroot://%s@%s//query/%d" % (user, hostport, self._chunk)

        def _doRead(self):
            xrdLseekSet(self._handle, 0L); ## Seek to beginning to read from beginning.
            while True:
                bufSize = self._readSize
                buf = "".center(bufSize) # Fill buffer
                rCount = xrdReadStr(self._handle, buf)
                tup = (self._chunk, len(self._resultBufferList), rCount)
                #print "chunk %d [packet %d] recv %d" % tup
                if rCount <= 0:
                    return False
                self._resultBufferList.append(buf[:rCount])
                del buf # Really get rid of the buffer
                if rCount < bufSize:
                    break
                pass
            return True

        def _doPostProc(self, stats, taskName):
            if self._resultBufferList:
                # print "Result buffer is", 
                # for s in resultBufferList:
                #     print "----",s,"----"
                stats[taskName+"postStart"] = time.time()
                self._outputFunc(self._outputArg, self._resultBufferList)
                stats[taskName+"postFinish"] = time.time()
            del self._resultBufferList # Really get rid of the result buffer
            pass

        def _readAndPostProc(self, stats, taskName):
            stats[taskName+"postStart"] = time.time()
            success = self._doRead()
            xrdClose(self._handle)
            if success:
                self._doPostProc(stats, taskName)
            stats[taskName+"postFinish"] = time.time()
            return success

        def _copyToLocal(self, stats, taskName):
            """_copyToLocal performs the necessary xrdRead and file writing 
            to copy xrd data to a local file, using C++ code for heavylifting.
            Warning!  This duplicates saveTableDump functionality.
            Use only one of these.  """
            xrdLseekSet(self._handle, 0L); ## Seek to beginning to read from beg
            stats[taskName+"postStart"] = time.time()
            writeRes, readRes = xrdReadToLocalFile(self._handle, 
                                                   self._readSize,
                                                   self._targetName)
            xrdClose(self._handle)
            stats[taskName+"postFinish"] = time.time()
            if readRes < 0:
                print "Couldn't read %d, error %d" %(self._chunk, -readRes)
                return False
            elif writeRes < 0:
                print "Couldn't write %s, error %d" %(self._targetName, 
                                                      -writeRes)
                return False
            return True

        def _doCppTransaction(self, stats, taskName):
            packedResult = xrdOpenWriteReadSaveClose(self._url,
                                                     self._query, 
                                                     self._readSize,
                                                     self._targetName);
            self._query = None # Reclaim memory
            if packedResult.open < 0:
                print "Error: open ", self._url
                return False
            if packedResult.queryWrite < 0:
                print "Error: dispatch/write %s (%d) e=%d" \
                    % (self._url, packedResult.open, packedResult.queryWrite)
                return False
            if packedResult.read < 0:
                print "Error: result readback %s (%d) e=%d" \
                    % (self._url, packedResult.open, packedResult.read)
                return False
            if packedResult.localWrite < 0:
                print "Error: result writeout %s (%d) file %s e=%d" \
                    % (self._url, packedResult.open, 
                       self._targetName, packedResult.localWrite)
                return False
            return True

        def _doNormalTransaction(self, stats, taskName):
            self._handle = xrdOpen(self._url, os.O_RDWR)
            wCount = xrdWrite(self._handle, charArray_frompointer(self._query), 
                              self._queryLen)
            self._query = None # Save memory!
            success = True
            if wCount == self._queryLen:
                #print self._url, "Wrote OK", wCount, "out of", self._queryLen
                if self._usingXrdReadTo:
                    success = self._copyToLocal(stats, taskName)
                else:
                    success = self._readAndPostProc(stats, taskName)
            else:
                print self._url, "Write failed! (%d of %d)" %(wCount, self._queryLen)
                success = False
            return success

        def run(self):
            #profile.runctx("self._doMyWork()", globals(), locals(), 
            #                self.profileName)
            self._doMyWork()

        def _doMyWork(self):
            #print "Issuing (%d)" % self._chunk, "via", self._url
            stats = time.qServQueryTimer[time.qServRunningName]
            taskName = "chunkQ_" + str(self._chunk)
            stats[taskName+"Start"] = time.time()
            self.successful = True
            if self._usingCppTransaction:
                self.successful = self._doCppTransaction(stats, taskName)
            else:
                self.successful = self._doNormalTransaction(stats, taskName)
            print "[", self._chunk, "complete]",
            stats[taskName+"Finish"] = time.time()
            return self.successful
        pass

########################################################################

class RegionFactory:
    def __init__(self):
        self._constraintNames = {
            "box" : self._handleBox,
            "circle" : self._handleCircle,
            "ellipse" : self._handleEllipse,
            "poly": self._handleConvexPolygon,
            "hull": self._handleConvexHull,
            # Handled elsewhere
            "db" : self._handleNop,
            "objectId" : self._handleNop
            }
        pass

    def _splitParams(self, name, tupleSize, param):
        hList = map(float,param.split(","))
        assert 0 == (len(hList) % tupleSize), "Wrong # for %s." % name
        # Split a long param list into tuples.
        return map(lambda x: hList[tupleSize*x : tupleSize*(x+1)],
                   range(len(hList)/tupleSize))
        
    def _handleBox(self, param):
        # ramin, declmin, ramax, declmax
        return map(lambda t: SphericalBox(t[:2], t[2:]),
                   self._splitParams("box", 4, param))

    def _handleCircle(self, param):
        # ra,decl, radius
        return map(lambda t: SphericalBox(t[:2], t[2:]),
                   self._splitParams("circle", 3, param))

    def _handleEllipse(self, param):
        # ra,decl, majorlen,minorlen,majorangle
        return map(lambda t: SphericalBox(t[:2], t[2], t[3], t[4]),
                   self._splitParams("ellipse", 5, param))

    def _handleConvexPolygon(self, param):
        # For now, list of vertices only, in counter-clockwise order
        # vertex count, ra1,decl1, ra2,decl2, ra3,decl3, ... raN,declN
        # Note that:
        # N = vertex_count, and 
        # N >= 3 in order to be considered a polygon.
        return self._handlePointSequence(SphericalConvexPolygon,
                                         "convex polygon", param)

    def _handleConvexHull(self, param):
        # ConvexHull is adds a processing step to create a polygon from
        # an unordered set of points.
        # Points are given as ra,dec pairs:
        # point count, ra1,decl1, ra2,decl2, ra3,decl3, ... raN,declN
        # Note that:
        # N = point_count, and 
        # N >= 3 in order to define a hull with nonzero area.
        return self._handlePointSequence(convexHull, "convex hull", param)

    def _handlePointSequence(self, constructor, name, param):
        h = param.split(",")
        polys = []
        while true:
            count = int(h[0]) # counts are integer
            next = 1 + (2*count)
            assert len(hList) >= next, \
                "Not enough values for %s (%d)" % (name, count)
            flatPoints = map(float, h[1 : next])
            # A list of 2-tuples should be okay as a list of vertices.
            polys.append(constructor(zip(flatPoints[::2],
                                        flatPoints[1::2])))
            h = h[next:]
        return polys
        
    def _handleNop(self, param):
        return []

    def getRegionFromHint(self, hintDict):
        """
        Convert a hint string list into a list of geometry regions that
        can be used with a partition mapper.
        
        @param hintDict a dictionary of hints
        @return None on error
        @return list of spherical regions if successful
        """
        regions = []
        try:
            for name,param in hintDict.items():
                if name in self._constraintNames: # spec?
                    regs = self._constraintNames[name](param)
                    regions.extend(regs)
                else: # No previous type, and no current match--> error
                    self.errorDesc = "Bad Hint name found:"+name
                    return None
        except Exception, e:
            self.errorDesc = str(e)
            return None

        return regions
                                          
########################################################################

class QueryCollater:
    def __init__(self, sessionId):
        self.sessionId = sessionId
        self.inFlight = {}
        self.scratchPath = setupResultScratch()
        self._mergeCount = 0
        self._setDbParams() # Sets up more db params
        pass
    
    def submit(self, chunk, table, q):
        saveName = self._saveName(chunk)
        handle = submitQuery(self.sessionId, chunk, q, saveName)
        self.inFlight[chunk] = (handle, table)
        #print "Chunk %d to %s    (%s)" % (chunk, saveName, table)
        #state = joinSession(self.sessionId)

    def finish(self):
        for (k,v) in self.inFlight.items():
            s = tryJoinQuery(self.sessionId, v[0])
            #print "State of", k, "is", getQueryStateString(s)

        s = joinSession(self.sessionId)
        print "Final state of all queries", getQueryStateString(s)
        
        # Merge all results.
        for (k,v) in self.inFlight.items():
            self._merger.merge(self._saveName(k), v[1])
            #self._mergeTable(self._saveName(k), v[1])

    def getResultTableName(self):
        ## Should do sanity checking to make sure that the name has been
        ## computed.
        return self._finalQname

    def _getTimeStampId(self):
        unixtime = time.time()
        # Use the lower digits as pseudo-unique (usec, sec % 10000)
        # FIXME: is there a better idea?
        return str(int(1000000*(unixtime % 10000)))

    def _setDbParams(self):
        c = lsst.qserv.master.config.config
        self._dbSock = c.get("resultdb", "unix_socket")
        self._dbUser = c.get("resultdb", "user")
        self._dbName = c.get("resultdb", "db")
        
        self._finalName = "result_%s" % self._getTimeStampId();
        self._finalQname = "%s.%s" % (self._dbName, self._finalName)

        self._mysqlBin = c.get("mysql", "mysqlclient")
        if not self._mysqlBin:
            self._mysqlBin = "mysql"
        # setup C++ merger
        # FIXME: This code is deprecated-- it doesn't properly 
        # configure the merger.
        mergeConfig = TableMergerConfig(self._dbName, self._finalQname, 
                                       self._dbUser, self._dbSock, 
                                       self._mysqlBin)
        self._merger = TableMerger(mergeConfig)

    def _mergeTable(self, dumpFile, tableName):
        dropSql = "DROP TABLE IF EXISTS %s;" % self._finalQname
        createSql = "CREATE TABLE %s SELECT * FROM %s;" % (self._finalQname,
                                                           tableName)
        insertSql = "INSERT INTO %s SELECT * FROM %s;" % (self._finalQname,
                                                          tableName)
        cleanupSql = "DROP TABLE %s;" % tableName
        
        cmdBase = "%s --socket=%s -u %s %s " % (self._mysqlBin, self._dbSock, 
                                                self._dbUser,
                                                self._dbName)
        loadCmd = cmdBase + " <" + dumpFile
        mergeSql = ""
        self._mergeCount += 1
        if self._mergeCount <= 1:
            mergeSql += dropSql + createSql + "\n"
        else:
            mergeSql += insertSql + "\n";
        mergeSql += cleanupSql
        rc = os.system(loadCmd)
        if rc != 0:
            print "Error loading result table :", dumpFile
        else:
            cmdList = cmdBase.strip().split(" ")
            p = Popen(cmdList, bufsize=0, 
                      stdin=PIPE, stdout=PIPE, stderr=PIPE,
                      close_fds=True)
            (outdata, errdata) = p.communicate(mergeSql)
            p.stdin.close()
            if p.wait() != 0:
                print "Error merging (%s)." % mergeSql, outdata, errdata
            pass
        pass
        
    def _saveName(self, chunk):
        dumpName = "%s_%s.dump" % (str(self.sessionId), str(chunk))
        return os.path.join(self.scratchPath, dumpName)

########################################################################

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

class QueryBabysitter:
    """Watches over queries in-flight.  An instrument of query care that 
    can be used by a client.  Unlike the collater, it doesn't do merging.
    """
    def __init__(self, sessionId, queryHash, fixup, 
                 reportError=lambda e:None, resultName=""):
        self._sessionId = sessionId
        self._inFlight = {}
        # Scratch mgmt (Consider putting somewhere else)
        self._scratchPath = setupResultScratch()

        self._setupMerger(fixup, resultName) 
        self._reportError = reportError
        pass

    def _setupMerger(self, fixup, resultName):
        c = lsst.qserv.master.config.config
        dbSock = c.get("resultdb", "unix_socket")
        dbUser = c.get("resultdb", "user")
        dbName = c.get("resultdb", "db")        
        dropMem = c.get("resultdb","dropMem")

        mysqlBin = c.get("mysql", "mysqlclient")
        if not mysqlBin:
            mysqlBin = "mysql"

        mergeConfig = TableMergerConfig(dbName, resultName, 
                                        fixup,
                                        dbUser, dbSock, 
                                        mysqlBin, dropMem)
        configureSessionMerger(self._sessionId, mergeConfig)

    def pauseReadback(self):
        pauseReadTrans(self._sessionId)
        pass

    def resumeReadback(self):
        resumeReadTrans(self._sessionId)
        pass

    def submit(self, chunk, table, q):
        saveName = self._saveName(chunk)
        handle = submitQuery(self._sessionId, chunk, q, saveName, table)
        #print "Chunk %d to %s    (%s)" % (chunk, saveName, table)

    def submitMsg(self, db, chunk, msg, table):
        saveName = self._saveName(chunk)
        handle = submitQueryMsg(self._sessionId, db, chunk, msg, 
                                saveName, table)
        self._inFlight[chunk] = (handle, table)

    def finish(self):
        for (k,v) in self._inFlight.items():
            s = tryJoinQuery(self._sessionId, v[0])
            #print "State of", k, "is", getQueryStateString(s)

        s = joinSession(self._sessionId)
        if s != QueryState_SUCCESS:
            self._reportError(getErrorDesc(self._sessionId))
        print "Final state of all queries", getQueryStateString(s)
        
    def getResultTableName(self):
        ## Should do sanity checking to make sure that the name has been
        ## computed.
        return getSessionResultName(self._sessionId)

    def _saveName(self, chunk):
        ## Want to delegate this to the merger.
        dumpName = "%s_%s.dump" % (str(self._sessionId), str(chunk))
        return os.path.join(self._scratchPath, dumpName)

########################################################################

class PartitioningConfig: 
    """ An object that stores information about the partitioning setup.
    """
    def __init__(self):
        self.clear() # reset fields

    def clear(self):
        ## public
        self.chunked = set([])
        self.subchunked = set([])
        self.allowedDbs = set([])
        self.chunkMapping = ChunkMapping()
        self.chunkMeta = ChunkMeta()
        pass

    def applyConfig(self):
        c = lsst.qserv.master.config.config
        try:
            chk = c.get("table", "chunked")
            subchk = c.get("table", "subchunked")
            adb = c.get("table", "alloweddbs")
            self.chunked.update(chk.split(","))
            self.subchunked.update(subchk.split(","))    
            self.allowedDbs.update(adb.split(","))
        except:
            print "Error: Bad or missing chunked/subchunked spec."
        self._updateMap()
        self._updateMeta()
        pass

    def getMapRef(self, chunk, subchunk):
        """@return a map reference suitable for sql parsing and substitution.
        For convenience.
        """
        return self.chunkMapping.getMapReference(chunk, subchunk)

    def _updateMeta(self):
        for db in self.allowedDbs:
            map(lambda t: self.chunkMeta.add(db, t, 1), self.chunked)
            map(lambda t: self.chunkMeta.add(db, t, 2), self.subchunked)
        pass

    def _updateMap(self):
        map(self.chunkMapping.addChunkKey, self.chunked)
        map(self.chunkMapping.addSubChunkKey, self.subchunked)
        pass

########################################################################
class QueryHintError(Exception):
    """An error in query hinting (Bad/missing values)."""
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return repr(self.reason)

########################################################################
class HintedQueryAction:
    """A HintedQueryAction encapsulates logic to prepare, execute, and 
    retrieve results of a query that has a hint string."""
    def __init__(self, query, hints, pmap, reportError, resultName=""):
        self.queryStr = query.strip()# Pull trailing whitespace
        # Force semicolon to facilitate worker-side splitting
        if self.queryStr[-1] != ";":  # Add terminal semicolon
            self.queryStr += ";" 

        # queryHash identifies the top-level query.
        self.queryHash = self._computeHash(self.queryStr)[:18]
        self.chunkLimit = 2**32 # something big
        if not self._importQconfig(pmap, hints):
            return

        if not self._parseAndPrep(query, hints):
            return

        if not self._isValid:
            discardSession(self._sessionId)
            return
        self._prepForExec(self._useMemory, reportError, resultName)

    def _importQconfig(self, pmap, hints):
        self._dbContext = "LSST" # Later adjusted by hints.
        # Hint evaluation
        self._pmap = pmap            
        self._isFullSky = False # Does query involves whole sky
        try:
            self._evaluateHints(hints, pmap) # Also gets new dbContext
        except QueryHintError, e:
            self._isValid = False
            self._error = e.reason
            return 

        # Config preparation
        qConfig = self._prepareCppConfig(self._dbContext, hints)
        self._sessionId = newSession(qConfig)
        cModule = lsst.qserv.master.config
        cf = cModule.config.get("partitioner", "emptyChunkListFile")
        self._emptyChunks = self._loadEmptyChunks(cf)
        cfgLimit = int(cModule.config.get("debug", "chunkLimit"))
        if cfgLimit > 0:
            self.chunkLimit = cfgLimit
            print "Using debugging chunklimit:",cfgLimit
        self._useMemory = cModule.config.get("tuning", "memoryEngine")
        return True

    def _parseAndPrep(self, query, hints):
        # Table mapping 
        try:
            self._pConfig = PartitioningConfig() # Should be shared.
            self._pConfig.applyConfig()
            cfg = self._prepareCppConfig(self._dbContext, hints)
            self._substitution = SqlSubstitution(query, 
                                                 self._pConfig.chunkMeta,
                                                 cfg)

            if self._substitution.getError():
                self._error = self._substitution.getError()
                self._isValid = False
            else:
                self._isValid = True
        except Exception, e:
            print "Exception!",e
            self._isValid = False
        return True

    def _prepForExec(self, useMemory, reportError, resultName):
        self._postFixChunkScope(self._substitution.getChunkLevel())

        # Query babysitter.
        self._babysitter = QueryBabysitter(self._sessionId, self.queryHash,
                                           self._substitution.getMergeFixup(),
                                           reportError, resultName)
        self._reportError = reportError
        ## For generating subqueries
        if useMemory == "yes":
            print "Memory spec:", useMemory
            engineSpec = "ENGINE=MEMORY "
        else: engineSpec = ""
        self._createTableTmpl = "CREATE TABLE IF NOT EXISTS %s " + engineSpec
        self._insertTableTmpl = "INSERT INTO %s " ;
        self._resultTableTmpl = "r_%s_%s" % (self._sessionId,
                                             self.queryHash) + "_%s"
        # Should use db from query, not necessarily context.
        self._factory = protocol.TaskMsgFactory(self._sessionId, 
                                                self._dbContext)

        # We want result table names longer than result-merging table names.
        self._isValid = True
        self._invokeLock = threading.Semaphore()
        self._invokeLock.acquire() # Prevent result retrieval before invoke
        pass


    # In transition to new protocol: only 1 result table allowed.
    def _headerFunc(self, tableNames, subc=[]):
        return ['-- SUBCHUNKS:' + ", ".join(imap(str,subc)),
                '-- RESULTTABLES:' + ",".join(tableNames)]

    def _prepareCppConfig(self, dbContext, hints):
        hintCopy = hints.copy()        
        hintCopy.pop("db") # Remove db hint--only pass spatial hints now.
        cfg = lsst.qserv.master.config.getStringMap()
        cfg["table.defaultdb"] = dbContext
        cfg["query.hints"] = ";".join(
            map(lambda (k,v): k + "," + v, hintCopy.items()))
        return cfg

    def _parseRegions(self, hints):
        r = RegionFactory()
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

    def _loadEmptyChunks(self, filename):
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
            print "ERROR: partitioner.emptyChunkListFile specified bad or missing chunk file"
        return empty

    def _postFixChunkScope(self, chunkLevel):
        if chunkLevel == 0:
            # In this case, non-chunked, so dummy chunk is good enough
            # for dispatch.
            self._intersectIter = [(dummyEmptyChunk, [])]
            return
        return

    def _evaluateHints(self, hints, pmap):
        """Modify self.fullSky and self.partitionCoverage according to 
        spatial hints"""
        self._isFullSky = True
        self._intersectIter = pmap

        if hints:
            regions = self._parseRegions(hints)
            self._dbContext = hints.get("db", "")
            ids = hints.get("objectId", "")
            if regions != []:
                self._intersectIter = pmap.intersect(regions)
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

    def _getChunkIdsFromObjs(self, ids):
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

    def _prepareMsg(self, chunkId, subIter):
        table = self._resultTableTmpl % str(chunkId)
        self._factory.newChunk(table, chunkId);
        x =  self._substitution.getChunkLevel()
        if x > 1:
            sclist =  self._getSubChunkList(subIter)
            for subChunkId in sclist:
                q = self._substitution.transform(chunkId, subChunkId)
                self._factory.fillFragment(q, [subChunkId])
        else:
            query = self._substitution.transform(chunkId, 0)
            self._factory.fillFragment(query, None)
        return self._factory.getBytes()

    def dispatchChunk(self, chunkId, subIter, lastTime):
        print "Dispatch iter: ", time.time() - lastTime
        msg = self._prepareMsg(chunkId, subIter)
        prepTime = time.time()
        print "DISPATCH: ", chunkId, self.queryStr # Limit printout spew
        self._babysitter.submitMsg(self._factory.msg.db,
                                   chunkId, msg, 
                                   self._factory.resulttable)
        print "Chunk %d dispatch took %f seconds (prep: %f )" % (
            chunkId, time.time() - lastTime, prepTime - lastTime)

    def invokeProtocol2(self):
        count = 0
        self._babysitter.pauseReadback();
        lastTime = time.time()
        chunkLimit = self.chunkLimit
        for chunkId, subIter in self._intersectIter:
            if chunkId in self._emptyChunks:
                continue
            self.dispatchChunk(chunkId, subIter, lastTime)
            lastTime = time.time()
            count += 1
            if count >= chunkLimit: break
            ##print >>sys.stderr, q, "submitted"
        if count == 0:
            self.dispatchChunk(dummyEmptyChunk, [], lastTime)

        self._babysitter.resumeReadback()
        self._invokeLock.release()
        return

    def invoke(self):
        self.invokeProtocol2()

    def invokeOldProtocol(self):
        count=0
        self._babysitter.pauseReadback();
        lastTime = time.time()
        chunkLimit = self.chunkLimit
        for chunkId, subIter in self._intersectIter:
            if chunkId in self._emptyChunks:
                continue # FIXME: What if all chunks are empty?
            print "Dispatch iter: ", time.time() - lastTime
            table = self._resultTableTmpl % str(chunkId)
            q = None
            x =  self._substitution.getChunkLevel()
            if x > 1:
                q = self._makeSubChunkQuery(chunkId, subIter, table)
            else:
                q = self._makeChunkQuery(chunkId, table)
            prepTime = time.time()
            print "DISPATCH: ", q[:500] # Limit printout spew
            self._babysitter.submit(chunkId, table, q)
            print "Chunk %d dispatch took %f seconds (prep: %f )" % (
                chunkId, time.time() - lastTime, prepTime - lastTime)
            lastTime = time.time()
            count += 1
            if count >= chunkLimit: break
            ##print >>sys.stderr, q, "submitted"
        self._babysitter.resumeReadback()
        self._invokeLock.release()
        return

    def getError(self):
        try:
            return self._error
        except:
            return ""

    def getResult(self):
        """Wait for query to complete (as necessary) and then return 
        a handle to the result."""
        self._invokeLock.acquire()
        self._babysitter.finish()
        self._invokeLock.release()
        table = self._babysitter.getResultTableName()
        #self._collater.finish()
        #table = self._collater.getResultTableName()
        return table

    def getIsValid(self):
        return self._isValid

    def _makeNonPartQuery(self, table):
        # Should be able to do less work than chunk query.
        query = "\n".join(self._headerFunc([table])) +"\n"
        query += self._createTableTmpl % table
        query += self._substitution.transform(0,0)
        return query

    def _makeChunkQuery(self, chunkId, table):
        # Prefix with empty subchunk spec.
        query = "\n".join(self._headerFunc([table])) +"\n"
        query += self._createTableTmpl % table
        query += self._substitution.transform(chunkId, 0)
        return query

    def _getSubChunkList(self, subIter):
        # Extract list first.
        if self._isFullSky:
            scList = [x for x in subIter]
        else:
            scList = [sub for (sub, regions) in subIter]
        return scList

    def _makeSubChunkQuery(self, chunkId, subIter, table):
        qList = [None] # Include placeholder for header
        scList = self._getSubChunkList(subIter)

        pfx = None
        qList = self._headerFunc([table], scList)
        for subChunkId in scList:
            q = self._substitution.transform(chunkId, subChunkId)
            if pfx:
                qList.append(pfx + q)
            else:
                qList.append((self._createTableTmpl % table) + q)
                pfx = self._insertTableTmpl % table
        return "\n".join(qList)

    def _computeHash(self, bytes):
        return hashlib.md5(bytes).hexdigest()

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
class MetadataCacheInterface:
    """MetadataCacheInterface encapsulates logic to prepare, metadata 
       information by fetching it from qserv metadata server into 
       C++ memory structure. It is stateless. Throws exceptions on
       failure."""

    def newSession(self):
        """Creates new session: assigns sessionId, populates the C++
           cache and returns the sessionId."""
        sessionId = newMetadataSession()
        qmsClient = Client(
            lsst.qserv.master.config.config.get("metaServer", "host"),
            int(lsst.qserv.master.config.config.get("metaServer", "port")),
            lsst.qserv.master.config.config.get("metaServer", "user"),
            lsst.qserv.master.config.config.get("metaServer", "pass"))
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
        if partStrategy == "sphBox":
            ret = addTbInfoPartitionedSphBox(
                sessionId, 
                dbName,
                tableName, 
                float(x["overlap"]),
                x["phiCol"],
                x["thetaCol"],
                int(x["phiColNo"]),
                int(x["thetaColNo"]),
                int(x["logicalPart"]),
                int(x["physChunking"]))
        elif partStrategy == "None":
            ret = addTbInfoNonPartitioned(sessionId, dbName, tableName)
        else:
            raise Exception("Not supported partitioning strategy: %s" % \
                                x["partitioningStrategy"])
        if ret != 0:
            if ret == -1: # the dbInfo does not exist
                raise QmsException(Status.ERR_DB_NOT_EXISTS)
            if ret == -2: # the table is already there
                raise QmsException(Status.ERR_TABLE_EXISTS)
            raise QmsException(Status.ERR_INTERNAL)

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

tokens_where = [ ['where', 
                  [ ['RA', 'between', '2', 'and', '5'] ], 
                  'and', 
                  [ ['DECL', 'between', '1', 'and', '10'] ]
                  ] 
                 ]

whereList = [ ( [
            ( ['RA', 'between', '2', 'and', '5'], 
              {'column': [('RA', 0)] }
              )
            ], {}),
              ( [ 
            ( ['DECL', 'between', '1', 'and', '10'], 
              {'column': [('DECL', 0)]}
              )
            ], {})
              ]
#      partmin                   partmax 
#  rmin           rmax
# 

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
