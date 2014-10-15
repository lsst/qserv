#
# LSST Data Management System
# Copyright 2008-2014 AURA/LSST.
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

# app module for lsst.qserv.czar
#
# The app module can be thought-of as the top-level code module for
# the qserv frontend's functionality.  It implements the "interesting"
# code that accepts an incoming query and sends it to be
# executed. Most of its logic should eventually be pushed to C++ code
# for greater efficiency and maintainability. The dispatch and query
# management has been migrated over to the C++ layer for efficiency, so
# it makes sense to move closely-related code there to reduce the pain
# of Python-C++ language boundary crossings.
#
# This is the  "high-level application logic" and glue of the qserv
# czar/frontend.
#
# InBandQueryAction is the biggest actor in this module. Leftover code
# from older parsing/manipulation/dispatch models may exist and should
# be removed (please open a ticket).
#
# The biggest ugliness here is due to the use of a Python geometry
# module for computing the chunk numbers, given a RA/decl area
# specification. The C++ layer extracts these specifications from the
# query, the code here must pull them out, pass them to the geometry
# module, and then push the resulting specifications down to C++ again
# so that the chunk queries can be dispatched without language
# crossings for each chunk query.
#
# Questions? Contact Daniel L. Wang, SLAC (danielw)
#
# Standard Python imports
import errno
import hashlib
from itertools import chain, imap, ifilter
import os
import cProfile as profile
import pstats
import random
import re
from subprocess import Popen, PIPE
import sys
import threading
import time
import traceback
from string import Template

# Package imports
import logger
import metadata
import spatial
import css
import lsst.qserv.czar.config
from lsst.geom.geometry import SphericalBox
from lsst.geom.geometry import SphericalConvexPolygon, convexHull
from lsst.qserv.czar.db import TaskDb as Persistence
from lsst.qserv.czar.db import Db

# SWIG'd functions

from lsst.qserv.czar import CHUNK_COLUMN, SUB_CHUNK_COLUMN, DUMMY_CHUNK

# qdisp
from lsst.qserv.czar import ChunkSpec
# ccontrol
from lsst.qserv.czar import getQueryStateString
from lsst.qserv.czar import SUCCESS as QueryState_SUCCESS

# UserQuery
from lsst.qserv.czar import UserQueryFactory
from lsst.qserv.czar import UserQuery_containsDb
from lsst.qserv.czar import UserQuery_getConstraints
from lsst.qserv.czar import UserQuery_getDominantDb
from lsst.qserv.czar import UserQuery_getDbStriping
from lsst.qserv.czar import UserQuery_getExecDesc
from lsst.qserv.czar import UserQuery_getError
from lsst.qserv.czar import UserQuery_addChunk
from lsst.qserv.czar import UserQuery_submit
from lsst.qserv.czar import UserQuery_join
from lsst.qserv.czar import UserQuery_kill
from lsst.qserv.czar import UserQuery_discard

# Parser
from lsst.qserv.czar import ChunkMeta

# queryMsg
from lsst.qserv.czar import msgCode
from lsst.qserv.czar import queryMsgAddMsg

# Experimental interactive prompt (not currently working)
import code, traceback, signal


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
    """Register debug() as a signal handler to SIGUSR1"""
    signal.signal(signal.SIGUSR1, debug)  # Register handler
listen()

invokedActions = set()
def stopAll():
    """Try to stop all InbandQueryActions in flight"""
    deathQueue = [i for i in invokedActions]
    for action in deathQueue:
        action.abort()
        invokedActions.discard(action)
    pass



######################################################################
## Methods
######################################################################
## MySQL interface helpers
def computeShortCircuitQuery(query, conditions):
    """Return the result for a short-circuit query, or None if query is
    not a known short-circuit query."""
    if query == "select current_user()":
        return ("qserv@%","","")
    return # not a short circuit query.

## Helpers
def getResultTable(tableName):
    """Spawn a subprocess to invoke mysql and retrieve the rows of
    table tableName."""
    # Minimal sanitizing
    tableName = tableName.strip()
    assert not filter(lambda x: x in tableName, ["(",")",";"])
    sqlCmd = "SELECT * FROM %s;" % tableName

    # Get config
    config = lsst.qserv.czar.config.config
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
class CzarError(Exception):
    """Generic error in the czar"""
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return repr(self.reason)

class AccessError(CzarError):
    """Missing/unauthorized access"""
    __init__ = CzarError.__init__

class QueryHintError(CzarError):
    """An error in handling query hints"""
    __init__ = CzarError.__init__

class ConfigError(CzarError):
    """An error in the configuration"""
    __init__ = CzarError.__init__

class ParseError(CzarError):
    """An error in parsing the query"""
    __init__ = CzarError.__init__

class DataError(CzarError):
    """An error with Qserv data. The data underlying qserv is not consistent."""
    __init__ = CzarError.__init__

########################################################################
def setupResultScratch():
    """Prepare the configured scratch directory for use, creating if
    necessary and checking for r/w access. """
    # Make sure scratch directory exists.
    cm = lsst.qserv.czar.config
    c = lsst.qserv.czar.config.config

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

class IndexLookup:
    """Value class for SecondaryIndex queries"""
    def __init__(self, db, table, keyColumn, keyVals):
        self.db = db
        self.table = table
        self.keyColumn = keyColumn
        self.keyVals = keyVals
        pass
class SecondaryIndex:
## FIXME: subchunk index creation
##    "SELECT DISTINCT %s, %s FROM %s" % (CHUNK_COL, SUBCHUNK_COL, table)

    def lookup(self, indexLookups):
        sqls = []
        for lookup in indexLookups:
            table = metadata.getIndexNameForTable("%s.%s" % (lookup.db,
                                                             lookup.table))
            keys = ",".join(lookup.keyVals)
            condition = "%s IN (%s)" % (lookup.keyColumn, keys)
            sql = "SELECT %s FROM %s WHERE %s" % (CHUNK_COLUMN, table, condition)
            sqls.append(sql)
        if not sqls:
            return
        sql = " UNION ".join(sqls)
        db = Db()
        db.activate()
        cids = db.applySql(sql)
        try:
            logger.inf("cids are ", cids)
            cids = map(lambda t: t[0], cids)
        except:
            raise QueryHintError("mysqld error during index lookup q=" + sql)
        del db
        return cids

class Context:
    """Context for InbandQueryAction construction.
    Hides management of UserQueryFactory object so that it can be
    shared among InbandQueryActions."""

    _uqFactory = None # Shared UserQueryFactory object

    def __init__(self, conditions):
        """Construct a context to pass bulk user conditions to InbandQueryAction.
        Constructs a UserQueryFactory if one is not available.

        @param conditions dict containing query hints and context
        """
        if not Context._uqFactory:
            Context._initFactory()

        self.uqFactory = Context._uqFactory
        self.conditions = conditions


    @classmethod
    def destroyShared(cls):
        """Destroy shared state, e.g., the UserQueryFactory object. Calling
        this is not generally necessary unless the configuration changes."""
        cls._uqFactory = None

    @classmethod
    def _initFactory(cls):
        """Initialize the UserQueryFactory instance from our configuration"""
        cfg = lsst.qserv.czar.config.getStringMap()
        cssItems = lsst.qserv.czar.config.config.items("css")
        cf = css.CssCacheFactory(config=cssItems)
        cls._uqFactory = UserQueryFactory(cfg, cf.createKvMem())
        cls._cssCacheFactory = cf


########################################################################
class InbandQueryAction:
    """InbandQueryAction is an action which represents a user-query
    that is executed using qserv in many pieces. It borrows a little
    from HintedQueryAction, but uses different abstractions
    underneath.
    """
    def __init__(self, query, context, setSessionId, resultName=""):
        """Construct an InbandQueryAction
        @param query SQL query text (SELECT...)
        @param context a user context object containing conditions and a user query factory
        @param setSessionId - unary function. a callback so this object can provide
                          a handle (sessionId) for the caller to access query
                          messages.
        @param resultName name of result table for query results."""

        # Set logging severity threshold.
        logger.threshold_inf()

        ## Fields with leading underscores are internal-only
        ## Those without leading underscores may be read by clients
        self.queryStr = query.strip()# Pull trailing whitespace
        # Force semicolon to facilitate worker-side splitting
        if self.queryStr[-1] != ";":  # Add terminal semicolon
            self.queryStr += ";"

        # queryHash identifies the top-level query.
        self.queryHash = self._computeHash(self.queryStr)[:18]
        self.chunkLimit = 2**32 # something big
        self.isValid = False

        self.context = context
        self.hintList = [] # C++ parser-extracted hints only.

        self._importQconfig()
        self._invokeLock = threading.Semaphore()
        self._invokeLock.acquire() # Prevent res-retrieval before invoke
        self._resultName = resultName
        try:
            self._prepareForExec()
            self.isValid = True
        except QueryHintError, e:
            self._error = str(e)
        except ParseError, e:
            self._error = str(e)
        except ConfigError, e:
            self._error = str(e)
        except:
            self._error = "Unexpected error: " + str(sys.exc_info())
            logger.err(self._error, traceback.format_exc())
            self._reportError(-1,
                              msgCode.MSG_QUERY_INIT,
                              "Initialize Query: " + self.queryStr);
        finally:
            # Pass up the sessionId for query messages access.
            # more serious errors won't even have a sessionId
            if hasattr(self, "sessionId"):
                setSessionId(self.sessionId)
        pass

    def _reportError(self, chunkId, code, message):
        logger.dbg("reporting", chunkId, code, message)
        queryMsgAddMsg(self.sessionId, chunkId, code, message)

    def invoke(self):
        """Begin execution of the query"""
        invokedActions.add(self) # Put self on the list to allow aborting.
        try:
            self._execAndJoin()
        finally:
            invokedActions.discard(self)
        self._invokeLock.release()

    def getError(self):
        """@return description of last error encountered. """
        try:
            return self._error
        except:
            return "Unknown error InbandQueryAction"

    def getResult(self):
        """Wait for query to complete (as necessary) and then return
        a handle to the result.
        @return name of result table"""
        self._invokeLock.acquire()
        # Make sure it's joined.
        self._invokeLock.release()
        return self._resultName

    def getIsValid(self):
        """@return true if instance is valid and can be executed"""
        return self.isValid

    def abort(self):
        """Abort execution. Not fully implemented"""
        UserQuery_kill(self.sessionId)

    def _computeHash(self, bytes):
        """Compute and return a hash that identifies the contained query"""
        return hashlib.md5(bytes).hexdigest()

    def _prepareForExec(self):
        """Prepare data structures and objects for query execution"""
        self.hints = self.context.conditions.copy() # make a copy
        dbContext = self.hints.get("db", "")

        logger.dbg("Setting sessionId")
        self.sessionId = self.context.uqFactory.newUserQuery(self.queryStr,
                                                             dbContext,
                                                             self._resultName)
        errorMsg = UserQuery_getError(self.sessionId)
        if errorMsg: raise ParseError(errorMsg)
        self.dominantDb = UserQuery_getDominantDb(self.sessionId)
        if not UserQuery_containsDb(self.sessionId, self.dominantDb):
            raise ParseError("Illegal db")
        self.dbStriping = UserQuery_getDbStriping(self.sessionId)
        self._addChunks()
        pass

    def _evaluateHints(self, dominantDb, hintList, pmap):
        """Modify self.fullSky and self.partitionCoverage according to
        spatial hints. This is copied from older parser model."""
        self._isFullSky = True
        self._intersectIter = pmap
        if hintList:
            regions = self._computeSpatialRegions(hintList)
            indexRegions = self._computeIndexRegions(hintList)

            if regions != []:
                # Remove the region portion from the intersection tuple
                self._intersectIter = map(
                    lambda i: (i[0], map(lambda j:j[0], i[1])),
                    pmap.intersect(regions))
                self._isFullSky = False
            if indexRegions:
                if regions != []:
                    self._intersectIter = chain(self._intersectIter, indexRegions)
                else:
                    self._intersectIter = map(lambda i: (i,[]), indexRegions)
                self._isFullSky = False
                if not self._intersectIter:
                    self._intersectIter = [(DUMMY_CHUNK, [])]
        # _isFullSky indicates that no spatial hints were found.
        # However, if spatial tables are not found in the query, then
        # we should use the dummy chunk so the query can be dispatched
        # once to a node of the balancer's choosing.

        # If hints only apply when partitioned tables are in play.
        # FIXME: we should check if partitionined tables are being accessed,
        # and then act to support the heaviest need (e.g., if a chunked table
        # is being used, then issue chunked queries).
        logger.dbg("Affected chunks: ", [x[0] for x in self._intersectIter])
        pass

    def _makePmap(self, dominantDb, dbStriping):
        if (dbStriping.stripes < 1) or (dbStriping.subStripes < 1):
            msg = "Partitioner's stripes and substripes must be natural numbers."
            raise lsst.qserv.czar.config.ConfigError(msg)
        return spatial.makePmap(dominantDb,
                                dbStriping.stripes,
                                dbStriping.subStripes)

    def _importConstraints(self, constraints):
        def iterateConstraints(constraintVec):
            for i in range(constraintVec.size()):
                yield constraintVec.get(i)
        for constraint in iterateConstraints(constraints):
            logger.inf("constraint=", constraint)
            params = [constraint.paramsGet(i)
                      for i in range(constraint.paramsSize())]
            self.hints[constraint.name] = params
            self.hintList.append((constraint.name, params))
        pass

    def _generateChunkSpec(self, chunkIter):
        count = 0
        chunkLimit = self.chunkLimit
        for chunkId, subIter in chunkIter:
            if chunkId in self._emptyChunks:
                logger.dbg("Rejecting empty chunk:", chunkId)
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
            yield c
            count += 1
            if count >= chunkLimit: break
        if count == 0:
            c = ChunkSpec()
            c.chunkId = DUMMY_CHUNK
            scount=0
            yield c
        pass

    def _computeConstraintsAsHints(self):
        """Retrieve discovered constraints from the query and
        evaluate chunk coverage against them."""
        constraints = UserQuery_getConstraints(self.sessionId)
        logger.dbg("Getting constraints", constraints, "size=",
                   constraints.size())
        self._importConstraints(constraints)
        self.pmap = self._makePmap(self.dominantDb, self.dbStriping)
        self._evaluateHints(self.dominantDb, self.hintList, self.pmap)

    def _addChunks(self):
        """Push the covered chunks down to the C++ layer in preparation for
        execution."""
        self._computeConstraintsAsHints()
        self._emptyChunks = metadata.getEmptyChunks(self.dominantDb)
        if not self._emptyChunks:
            raise DataError("No empty chunks for db")

        ## UserQuery rejects non-dummy chunks when chunking is not needed
        ## on partitioned tables.
        debugLimit=2
        current=[]
        for chunkSpec in self._generateChunkSpec(self._intersectIter):
            UserQuery_addChunk(self.sessionId, chunkSpec)
            current.append(chunkSpec)
            #if len(current) >= debugLimit: break ### DEBUGDEBUG REMOVE


    def _execAndJoin(self):
        """Signal dispatch to C++ layer and block until execution completes"""
        logger.threshold_dbg()

        lastTime = time.time()
        self._reportError(-1, msgCode.MSG_CHUNK_DISPATCH, "Dispatch Query.")
        UserQuery_submit(self.sessionId)
        elapsed = time.time() - lastTime
        logger.inf("Query dispatch (%s) took %f seconds" % (self.sessionId, elapsed))
        lastTime = time.time()
        s = UserQuery_join(self.sessionId)
        elapsed = time.time() - lastTime
        logger.inf("Query exec (%s) took %f seconds" % (self.sessionId, elapsed))

        if s != QueryState_SUCCESS:
            self._reportError(-1, -1,
                               UserQuery_getExecDesc(self.sessionId))
        logger.inf("Final state of all queries", getQueryStateString(s))
        # session should really be discarded here unconditionally,
        # but in the current design it is used in proxy.py, so it is
        # (temporarily) discarded there.
        if (not self.isValid) and self.sessionId:
            UserQuery_discard(self.sessionId)

    def _importQconfig(self):
        """Import config file settings into self"""
        # Config preparation
        cModule = lsst.qserv.czar.config

        # chunk limit: For debugging
        cfgLimit = int(cModule.config.get("debug", "chunkLimit"))
        if cfgLimit > 0:
            self.chunkLimit = cfgLimit
            logger.inf("Using debugging chunklimit:", cfgLimit)

        # Memory engine(unimplemented): Buffer results/temporaries in
        # memory on the czar. (no control over worker)
        self._useMemory = cModule.config.get("tuning", "memoryEngine")
        return True

    def _computeIndexRegions(self, hintList):
        """Compute spatial region coverage based on hints.
        @return list of regions"""
        logger.inf("Looking for indexhints in ", hintList)
        secIndexSpecs = ifilter(lambda t: t[0] == "sIndex", hintList)
        lookups = []
        for s in secIndexSpecs:
            params = s[1]
            lookup = IndexLookup(params[0], params[1], params[2], params[3:])
            lookups.append(lookup)
            pass
        index = SecondaryIndex()
        chunkIds = index.lookup(lookups)
        logger.inf("lookup got chunks:", chunkIds)
        return chunkIds

    def _computeSpatialRegions(self, hintList):
        """Compute spatial region coverage based on hints.
        @return list of regions"""
        r = spatial.getRegionFactory()
        regs = r.getRegionFromHint(hintList)
        if regs != None:
            return regs
        else:
            if r.errorDesc:
                # How can we give a good error msg to the client?
                s = "Error parsing hint string %s"
                raise QueryHintError(s % r.errorDesc)
            return []
        pass

    pass # class InbandQueryAction

### Other Action classes for invocation from appInterface and others
### (unimplemented)

class KillQueryAction:
    def __init__(self, query):
        self.query = query
        pass
    def invoke(self):
        logger.err("invoking kill query", self.query)
        return "Unimplemented"

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
