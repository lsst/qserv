# app package for lsst.qserv.master

# Standard Python imports
import errno
import hashlib
from itertools import imap
import os
import cProfile as profile
import pstats
import random
from subprocess import Popen, PIPE
import sys
import threading
import time
from string import Template

# Mysql
import MySQLdb as sql

# Package imports
import sqlparser
import lsst.qserv.master.config
from lsst.qserv.master import geometry
from lsst.qserv.master.geometry import SphericalBox, SphericalBoxPartitionMap


# SWIG'd functions

# xrdfile - raw xrootd access
from lsst.qserv.master import xrdOpen, xrdClose, xrdRead, xrdWrite
from lsst.qserv.master import xrdLseekSet, xrdReadStr
from lsst.qserv.master import xrdReadToLocalFile, xrdOpenWriteReadSaveClose

from lsst.qserv.master import charArray_frompointer, charArray

# transaction
from lsst.qserv.master import TransactionSpec

# Dispatcher 
from lsst.qserv.master import newSession, submitQuery, initDispatcher
from lsst.qserv.master import tryJoinQuery, joinSession, getQueryStateString

# Parser
from lsst.qserv.master import ChunkMapping, SqlSubstitution


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
    signal.signal(signal.SIGUSR1, debug)  # Register handler
listen()



######################################################################
## Methods
######################################################################
def makePmap():
    c = lsst.qserv.master.config.config
    stripes = c.getint("partitioner", "stripes")
    substripes = c.getint("partitioner", "substripes")
    if (stripes < 1) or (substripes < 1):
        msg = "Partitioner's stripes and substripes must be natural numbers."
        raise lsst.qserv.master.config.ConfigError(msg)
    p = SphericalBoxPartitionMap(stripes, substripes) 
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
class Persistence:
    def __init__(self):
        self._conn = None
        pass

    def check(self):
        if not self._conn:
            self.activate()
        if not self._conn:
           return False
        return True

    def activate(self):
        config = lsst.qserv.master.config.config
         #os.getenv("QSM_DBSOCK", "/data/lsst/run/mysql.sock")
        socket = config.get("resultdb", "unix_socket")
        user = config.get("resultdb", "user")
        passwd = config.get("resultdb", "passwd")
        host = config.get("resultdb", "host")
        port = config.getint("resultdb", "port")
        db = config.get("resultdb", "db")
        try: # Socket file first
            self._conn = sql.connect(user=user,
                                     passwd=passwd,
                                     unix_socket=socket,
                                     db=db)
        except Exception, e:
            try:
                self._conn = sql.connect(user=user,
                                         passwd=passwd,
                                         host=host,
                                         port=port,
                                         db=db)
            except Exception, e2:
                print >>sys.stderr, "Couldn't connect using file", socket, e
                msg = "Couldn't connect using %s:%s" %(host,port)
                print >>sys.stderr, msg, e2
                self._conn = None
                return
        c = self._conn.cursor()
        # should check if db exists here.
        # Database gets populated with fake data automatically, but
        # the db "test" must exist.
        pass

    def _dropSilent(self, cursor, tables):
        for t in tables:
            try:
                cursor.execute('DROP TABLE %s;' %t)
            finally:
                pass
        pass

    def makeTables(self):
        c = self._conn.cursor()
        self._dropSilent(c, ['tasks']) # don't drop the partmap anymore
        c.execute("CREATE TABLE tasks (id int, queryText text);")
        # We aren't in charge of the partition map anymore.
        # c.execute("CREATE TABLE partmap (%s);" % (", ".join([
        #                 "chunkId int", "subchunkId int", 
        #                 "ramin float", "ramax float", 
        #                 "declmin float", "declmax float"])))
        # self._populatePartFake()
        c.close()

        pass

    def _populatePartFake(self):
        c = self._conn.cursor()
        #
        # fake chunk layout (all subchunk 0 right now)
        # +----+-----+
        # | 1  |  2  |
        # +----+-----+    center at 0,0
        # | 3  |  4  |
        # +----+-----+
        # 
        # ^
        # |
        # |ra+
        #
        # decl+
        # -------->
        
        # chunkId, subchunkId, ramin, ramax, declmin, declmax
        fakeInfin = 100.0
        fakeRows = [(1, 0, 0.0, fakeInfin, -fakeInfin, 0.0),
                    (2, 0, 0.0, fakeInfin, 0.0, fakeInfin),
                    (3, 0, -fakeInfin, 0.0, -fakeInfin, 0.0),
                    (4, 0, -fakeInfin, 0.0, 0.0, fakeInfin),
                    ]
        for cTuple in fakeRows:
            sqlstr = 'INSERT INTO partmap VALUES %s;' % str(cTuple) 
            c.execute(sqlstr)
        c.close()

    
    def nextId(self):
        if not self._conn:
            self.activate()
        c = self._conn.cursor()
        c.execute('SELECT MAX(id) FROM tasks;') # non-atomic.
        maxId = c.fetchall()[0][0]
        if not maxId:
            return 1
        else:
            return 1 + maxId
        
        
    def addTask(self, taskparam):
        """taskparam should be a tuple of (id, query)
        You can pass None for id, and let the system assign a safe id.
        """
        if not self._conn:
            self.activate()
        if taskparam[0] == None:
            a = list(taskparam)
            a[0] = int(self.nextId())
            assert type(a[0]) is int
            taskparam = tuple(a)
        taskstr = str(taskparam)
        sqlstr = 'INSERT INTO tasks VALUES %s' % taskstr
        print "---",sqlstr
        self._conn.cursor().execute(sqlstr)
        return a[0]

    def issueQuery(self, query):
        c = self._conn.cursor()
        c.execute(query)
        return c.fetchall()    
    pass

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

class QueryPreparer:
    def __init__(self, queryStr):
        self.queryMunger = sqlparser.QueryMunger(queryStr)
        self.queryHash = hashlib.md5(queryStr).hexdigest()
        self.resultPath = self.setupDumpSaver(self.queryHash)
        self.createTemplate = "CREATE TABLE IF NOT EXISTS %s ENGINE=MEMORY ";
        self.insertTemplate = "INSERT INTO %s ";
        self.tableTemplate = "result_%s";
        self.db = Persistence()
        pass



    def computePartSet(self):
        """compute the set of chunks,subchunks needed for the query.
        Results are saved internally for later usage."""
        query = self.queryMunger.computePartMapQuery()
        print "partmapquery is", query
        self.db.activate()
        stats["partMapPrepStart"] = time.time()
        chunktuples = self.db.issueQuery(query)
        stats["partMapCollectStart"] = time.time()
        self.collectedSet = self.queryMunger.collectSubChunkTuples(chunktuples)
        del chunktuples # Free chunktuples memory
        stats["partMapCollectFinish"] = time.time()
        #collected = dict(collected.items()[:5]) ## DEBUG: Force only 3 chunks
        self.chunkNums = self.collectedSet.keys()
        random.shuffle(self.chunkNums) # Try to balance among workers.
        #chunkNums = chunkNums[:200]
        stats["partMapPrepFinish"] = time.time()

    def clearDb(self):
        self.db.activate()
        # Drop result table to make room.
        r = self.db.issueQuery("USE test; DROP TABLE IF EXISTS result;")
        pass

    def getQueryIterable(self):
        return imap(lambda c: [c] + self.computeQuery(c), self.chunkNums)

    def computeQuery(self, chunk):
        tableName = self.tableTemplate % str(chunk)
        createPrep = self.createTemplate % tableName
        insertPrep = self.insertTemplate % tableName

        subc = self.collected[chunk][:2000] # DEBUG: force less subchunks
        # MySQL will probably run out of memory with >2k subchunks.
        header = '-- SUBCHUNKS:' + ", ".join(imap(str,subc))                
        cq = self.queryMunger.expandSubQueries(chunk, subc)
        qlist = []
        if cq:
            qlist.append(createPrep + cq[0])
            remain = cq[1:]
            if remain:
                qlist.extend(imap(lambda s: insertPrep + s, remain))
                del remain
        del createPrep
        q = "\n".join([header] + qlist + ["\0\0\0\0"])  
        del qlist
        #del insertPrep #Python thinks insertPrep is still bound.
        
        return [tableName,q]

########################################################################

class RegionFactory:
    def __init__(self):
        self._constraintNames = {
            "areaSpec_box" : self._handleBox,
            "areaSpec_circle" : self._handleCircle,
            "areaSpec_ellipse" : self._handleEllipse,
            "areaSpec_poly": self._handleConvexPolygon
            }
        pass

    def _handleBox(self, hList):
        # ramin, declmin, ramax, declmax
        assert len(hList) >= 4, "Not enough values for box"
        bounds = map(float, hList[:4]) 
        return (hList[4:], SphericalBox(bounds[:2], bounds[2:]))
    def _handleCircle(self, hList):
        # ra,decl, radius
        assert len(hList) >= 3, "Not enough values for circle"
        params = map(float, hList[:3]) 
        return (hList[3:], SphericalCircle(bounds[:2], bounds[2:]))
    def _handleEllipse(self, hList):
        # ra,decl, majorlen,minorlen,majorangle
        assert len(hList) >= 5, "Not enough values for ellipse"
        params = map(float, hList[:5])
        return (hList[5:], SphericalEllipse(params[:2], params[2],
                                            params[3], params[4]))
    def _handleConvexPolygon(self, hList):
        # For now, list of vertices only, in counter-clockwise order
        # vertex count, ra1,decl1, ra2,decl2, ra3,decl3, ... raN,declN
        # Note that:
        # N = vertex_count, and 
        # N >= 3 in order to be considered a polygon.
        count = int(h[0]) # counts are integer
        next = 1 + (2*count)
        assert len(hList) >= next, "Not enough values for polygon(%d)" % count
        
        params = map(float, hList[1 : next])
        # Not sure if a list of 2-tuples is okay as a list of vertices.
        return (hList[next:], SphericalConvexPolygon(zip(params[::2],
                                                         params[1::2])))
    def getRegionFromHint(self, hList):
        """
        Convert a hint string list into a list of geometry regions that
        can be used with a partition mapper.
        
        @param hList a list of strings
        @return None on error
        @return list of spherical regions if successful
        """
        current = hList
        regions = []
        cType = None
        try:
            while current:
                name = current[0]
                if name in self._constraintNames: # spec?
                    (current, reg) = self._constraintNames[name](current[1:])
                    cType = name
                elif cType: # No spec?  Retry with previous type
                    (current, reg) = self._constraintNames[cType](current)
                else: # No previous type, and no current match--> error
                    self.errorDesc = "No hint name found."
                    return None
                regions.append(reg)
        except Exception, e:
            self.errorDesc = e.message
            return None

        return regions
                                          
########################################################################

class QueryCollater:
    def __init__(self, sessionId):
        self.sessionId = sessionId
        self.inFlight = {}
        self.scratchPath = None
        self._setupScratch()
        self._mergeCount = 0
        self._setDbParams() # Sets up more db params
        pass
    
    def submit(self, chunk, table, q):
        saveName = self._saveName(chunk)
        handle = submitQuery(self.sessionId, chunk, q, saveName)
        self.inFlight[chunk] = (handle, table)
        print "Chunk %d to %s    (%s)" % (chunk, saveName, table)
        #state = joinSession(self.sessionId)

    def finish(self):
        for (k,v) in self.inFlight.items():
            s = tryJoinQuery(self.sessionId, v[0])
            print "State of", k, "is", getQueryStateString(s)

        s = joinSession(self.sessionId)
        print "Final state of all queries", getQueryStateString(s)
        
        # Merge all results.
        for (k,v) in self.inFlight.items():
            self._mergeTable(self._saveName(k), v[1])


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

    def _setupScratch(self):
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
            raise cm.ConfigError("No access for scratch_path")
        self.scratchPath = scratchPath
        pass
    
########################################################################

class HintedQueryAction:
    def __init__(self, query, hints, pmap):
        self.queryStr = query.strip()# Pull trailing whitespace
        # Force semicolon to facilitate worker-side splitting
        if self.queryStr[-1] != ";":  # Add terminal semicolon
            self.queryStr += ";" 
        self.queryHash = hashlib.md5(self.queryStr).hexdigest()[:16] 

        self._pmap = pmap            
        self._isFullSky = False # Does query involves whole sky
        self._chunkList = None # List of chunks,subchunks needed
         ## {chunknum : subchunks, ...}
        self._evaluateHints(self._parseRegions(hints), pmap)

        # Ids for queries in this session
        self._queriesInFlight = {}

        self._mapping = ChunkMapping()         # Table remapping
        self._mapping.addChunkKey("Source")
        self._mapping.addSubChunkKey("Object")
        dummyMap = self._mapping.getMapReference(2,3)
        self._substitution = SqlSubstitution(query, dummyMap)
        self._sessionId = newSession()
        self._collater = QueryCollater(self._sessionId)


        self._resultTableTmpl = "r_%s_%s" % (self._sessionId,
                                             self.queryHash) + "_%s"
        self._createTableTmpl = "CREATE TABLE IF NOT EXISTS %s ENGINE=MEMORY " ;
        self._insertTableTmpl = "INSERT INTO TABLE %s " ;
        pass

    def _headerFunc(self, subc=[]):
        return '-- SUBCHUNKS:' + ", ".join(imap(str,subc))

    def _parseRegions(self, hints):
        r = RegionFactory()
        regs = r.getRegionFromHint(hints)
        if regs != None:
            return regs
        else:
            s = "Error parsing hint string %s, using hardcoded [0,0]-[1,1]"
            print s % r.errorDesc 
            return [SphericalBox([0,0],[1,1])] # Hardcode right now.
        pass

    def _evaluateHints(self, regions, pmap):
        """Modify self.fullSky and self.partitionCoverage according to 
        spatial hints"""
        self._isFullSky = False
        self._needSubChunk = False
        self._chunkList = None # FIXME

        if regions == []:
            self._isFullSky = True
            self._intersectIter = pmap
        else:
            self._intersectIter = pmap.intersect(regions)

         # for subChunkId, xRegions in subIter:
         #     assert isinstance(xRegions, set)
         #     if len(xRegions) == 0:
         #         # the sub-chunk is completely contained in at least
         #         # one constraint
         #         ...
         #     else:
         #         # xRegions is a set of SphericalRegion objects
         #         # partially overlapping the sub-chunk
         #         ...
        pass

    def _expandQuery(self, chunk, subChunks):
        """Expands the query for a given chunk and list of subchunks.
        A single SQL query becomes, per chunk:
        -- one whole-chunk subquery when no subchunking is needed
        -- n subqueries, one per subchunk.
        """
        return query ## FIXME Should expand-as-we-go

    def invoke(self):
        inFlight = self._queriesInFlight # copy to local ref.
        for chunkId, subIter in self._intersectIter:
            table = self._resultTableTmpl % str(chunkId)
            q = None
            if self._substitution.getChunkLevel() > 1:
                q = self._makeSubChunkQuery(chunkId, subIter, table)
            else:
                q = self._makeChunkQuery(chunkId, table)
            self._collater.submit(chunkId, table, q)
            print >>sys.stderr, q, "submitted"
        return

    def getResult(self):
        """Wait for query to complete (as necessary) and then return 
        a handle to the result."""
        self._collater.finish()
        table = self._collater.getResultTableName()
        return table

    def _makeChunkQuery(self, chunkId, table):
        # Prefix with empty subchunk spec.
        query = self._headerFunc() +"\n"
        ref = self._mapping.getMapReference(chunkId,0)
        query += self._createTableTmpl % table
        query += self._substitution.transform(ref)
        print query
        return query

    def _makeSubChunkQuery(self, chunkId, subIter, table):
        qList = [None] # Include placeholder for header
        scList = None
        # Extract list first.
        if self._isFullSky:
            scList = [x for x in subIter]
        else:
            scList = [sub for (sub, regions) in subIter]

        pfx = None
        for subChunkId in scList:
            ref = self._mapping.getMapReference(chunkId, subChunkId)
            q = self._substitution.transform(ref)
            if pfx:
                qList.append(pfx + q)
            else:
                qList.append((self._createTableTmpl % table) + q)
                pfx = self._insertTableTmpl % table
        qList[0] = self._headerFunc(scList)
        return "\n".join(qList)
        


class QueryAction:
    def __init__(self, query):
        self.queryStr = query.strip()# Pull trailing whitespace
        # Force semicolon to facilitate worker-side splitting
        if self.queryStr[-1] != ";":  # Add terminal semicolon
            self.queryStr += ";" 
            
        self.queryMunger = None ## sqlparser.QueryMunger()
        self.db = Persistence()
        self.running = {}
        self.resultLock = threading.Lock()
        self.finished = {}
        self.threadHighWater = 130 # Python can't make more than 159??
        self._slowDispatchTime = 0.5
        self._brokenChunks = []
        self._coolDownTime = 10
        self._preparer = QueryPreparer(query)
        pass

    def _joinAny(self, jostle=False, printProfile=False):
        # Should delete jostling code
        poppable = []
        while (not poppable) and (self.running):  # keep waiting 
            for (chunk, thread) in self.running.items():
                if not thread.isAlive(): 
                    poppable.append(chunk)
            if not poppable: 
                if jostle:
                    sys.stderr.write("poke! " + str(len(self.running)))
                    j = SleepingThread(0.4)
                    j.start()
                    j.join()
                else:
                    time.sleep(0.5) # Don't spin-check too fast.
                                        
        for p in poppable: # Move the threads out.
            t = self.running.pop(p)
            t.join()
            self.finished[p] = t.successful
            if not t.successful:
                print "Unsuccessful with %s on chunk %d" % (self.queryStr, p)
                self._brokenChunks.append(p)
            if printProfile:
                p = pstats.Stats(t.profileName)
                p.strip_dirs().sort_stats(-1).print_stats()

                
            # Don't save existing thread object.
        pass

    def _joinAll(self):
        for (c,xo) in self.running.items():
            xo.join()
            t = self.running.pop(c)
            self.finished[c] = t.successful
            if not xo.successful:
                print "Unsuccessful with %s on chunk %d" % (self.queryStr, c)
                self._brokenChunks.append(c)
            # discard thread object.

    def _progressiveJoinAll(self, **kwargs):
        self._joinAny()
        while len(self.running) > 4:
            time.sleep(1)
            joinTime = time.time()
            self._joinAny(**kwargs)
            joinTime -= time.time()
            sys.stderr.write("(%d) " % len(self.running))
            
        print "Almost done..."
        self._joinAll()
        
    def invoke2(self):
        self._preparer.computePartSet()
        self._preparer.clearDb()
        for chunk,table,q in self._preparer.getQueryIterable():
            xo = XrdOperation(chunk, q, lambda x:x, table, 
                              self.setupDumpSaver(self._preparer.queryHash))
        return

    def invoke3(self):
        self._preparer.computePartSet()
        self._preparer.clearDb()
        sessionId = newSession()
        for chunk,table,q in self._preparer.getQueryIterable():
            i = submitQuery(sessionId, chunk, q, saveName)
            inFlight[chunk] = i
        state = joinSession(sessionId)
        return
    
    def invoke(self):
        print "Query invoking..."
        stats = time.qServQueryTimer[time.qServRunningName]
        stats["queryActionStart"] = time.time()
        self.queryMunger = sqlparser.QueryMunger(self.queryStr)
        # 64bit hash is enough for now(testing).
        self.queryHash = hashlib.md5(self.queryStr).hexdigest()[:16] 
        self.resultPath = self.setupDumpSaver(self.queryHash)
        
        query = self.queryMunger.computePartMapQuery()
        print "partmapquery is", query
        p = Persistence()
        p.activate()
        stats["partMapPrepStart"] = time.time()
        chunktuples = p.issueQuery(query)
        stats["partMapCollectStart"] = time.time()
        collected = self.queryMunger.collectSubChunkTuples(chunktuples)
        del chunktuples # Free chunktuples memory
        stats["partMapCollectFinish"] = time.time()
        #collected = dict(collected.items()[:5]) ## DEBUG: Force only 3 chunks
        chunkNums = collected.keys()
        random.shuffle(chunkNums) # Try to balance among workers.
        #chunkNums = chunkNums[:200]
        stats["partMapPrepFinish"] = time.time()
        self.initQueryPreparer()
        q = ""
        self.db.activate()
        # Drop result table to make room.
        self.applySql("test", "DROP TABLE IF EXISTS result;")
        triedAgain = False
        for chunk in chunkNums:
                dispatchStart = time.time()
                (tableName, q) = self.prepareChunkQuery(collected, chunk)
                print "dispatch chunk=", chunk, 
                print "run=%d fin=%d tot=%d" %(len(self.running), 
                                               len(self.finished), 
                                               len(chunkNums))
                #print "header=", header[:70],"..."
                #print "create=", qlist[0]
                #xrdSubmitTransaction(chunk, q, 
                #                     os.path.join(self.resultPath, tableName))
                xo = XrdOperation(chunk, q, self.saveTableDump, tableName, self.resultPath) 
                del q
                xo.start()
                self.running[chunk] = xo
                dispatchTime = time.time() - dispatchStart
                if dispatchTime > self._slowDispatchTime: 
                    if triedAgain:
                        print "Slow dispatch detected. Draining all queries,"
                        # Drain *everyone*
                        self._progressiveJoinAll()
                        print "Cooling down for %d seconds." % self._coolDownTime 
                        time.sleep(self._coolDownTime)
                        print "Back to work!"
                        triedAgain = False
                    triedAgain = True
                elif len(self.running) > self.threadHighWater:
                    print "Reaping"
                    self._joinAny()
                    print "Reap done"
                else:
                    triedAgain = False

        # Try reaping until there's not much left
        remaining = self.running.keys()
        remaining.sort()
        print "All dispatched, periodic ", remaining
        self._progressiveJoinAll()
        
        print "results available at fs path,", self.resultPath
        stats["queryActionFinish"] = time.time()
        #print self.queryStr, "resulted in", query
        ## sqlparser.test(self.queryStr)
        
        # Want parser to:
        # a) help me munge the query for the partmap DB
        # b) help me re-form the original query for the chunk/subchunk pairs.
        
        #  tokens = sqlparser.getTokens(self.queryStr)
        
        # Then, for each chunk/subchunk, dispatch the cmd via xrd.
        # Later, we will fork for parallelism.  Test serially first.
         
        pass

    def initQueryPreparer(self):
        self.createTemplate = "CREATE TABLE IF NOT EXISTS %s ENGINE=MEMORY ";
        self.insertTemplate = "INSERT INTO %s ";
        self.tableTemplate = "result_%s";

        pass
    def prepareChunkQuery(self, subchunks, chunk ):
        subc = subchunks[chunk][:2000] # DEBUG: force less subchunks
        # MySQL will probably run out of memory with >2k subchunks.
        header = '-- SUBCHUNKS:' + ", ".join(imap(str,subc))

        cq = self.queryMunger.expandSubQueries(chunk, subc)
        tableName = self.tableTemplate % str(chunk)
        createPrep = self.createTemplate % tableName
        insertPrep = self.insertTemplate % tableName
        qlist = []
        if cq:
            qlist.append(createPrep + cq[0])
            remain = cq[1:]
            if remain:
                qlist.extend(imap(lambda s: insertPrep + s, remain))
                del remain
        q = "\n".join([header] + qlist + ["\0\0\0\0"])  
        # \0\0\0\0 is the magic query terminator for the 
        # worker to detect.
        return (tableName, q)
    
    def setupDumpSaver(self, hashkey):
        path = "/tmp/qserv_"+hashkey
        # Make room, recursive delete.
        try:
            os.rmdir(path)
        except OSError, e: 
            if e.errno == 39:
                # If not empty, make it empty
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                    # Do not recurse.  qserv-managed dir will not require it.
                os.rmdir(path)
            pass
        # Now make a nice, new directory
        os.mkdir(path)
        return path
    
    def saveTableDump(self, tableName, dumpPieces):
        """Write dumpPieces into a file, named after the table name.
        This shouldn't get called when xrdReadToLocalFile is in use."""
        name = os.path.join(self.resultPath, tableName)
        dumpfile = open(name, "wb")
        bytes = 0
        for f in dumpPieces:
            dumpfile.write(f)
            bytes += len(f)
        dumpfile.close()
        print "Wrote to %s, %d bytes" % (name, bytes)
        pass

        
    def mergeTableDump(self, tableName, dumpPieces):
        db = "test"
        targetTable = "result"

        self.resultLock.acquire()

        # Apply dump.  This ingests the dump into a table named tableName
        # The dump should be newline-separated.
        if False:
            self.applySqlSep(db, dumpPieces, ";\n")
        else:
            from subprocess import Popen, PIPE
            p = Popen(["/home/wang55/bin/mysql","test"], bufsize=0, 
                      stdin=PIPE, close_fds=True)
            if False:
                p.communicate("".join(dumpPieces))
            else:
                for f in dumpPieces:
                    p.stdin.write(f)
                p.communicate()


        ## Might need to specially handle null results.
        # Merge table results into the real results table.
        destSrc = (targetTable, tableName)
        mergeSql = [
            "CREATE TABLE IF NOT EXISTS %s like %s;" % destSrc,
            "INSERT INTO %s SELECT * FROM %s;" % destSrc]
            
        try:
            for s in mergeSql:
                self.applySql(db, s) 
        except:
            print "Problem merging after ", dump[:100]
            pass
        # Get rid of the table we ingested, since we've used it.
        self.applySql(db, "DROP TABLE %s;" % tableName)
        self.resultLock.release()
        pass

    def applySql(self, dbName, qtext):
        r = self.db.issueQuery(("USE %s;" % dbName) + qtext)
        pass

    def applySqlSep(self, dbName, qtext, sep):
        if sep in qtext:
            for l in qtext.split(sep):
                self.applySql(dbName, l + sep)
        else:
            self.applySql(dbName, qtext)
        pass
        

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

