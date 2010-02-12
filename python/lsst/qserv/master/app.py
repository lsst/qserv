# app package for lsst.qserv.master

# Standard Python imports
import hashlib
from itertools import imap
import os
import cProfile as profile
import pstats
import random
import sys
import threading
import time
import MySQLdb as sql
from string import Template

# Package imports
import sqlparser
from lsst.qserv.master import xrdOpen, xrdClose, xrdRead, xrdWrite
from lsst.qserv.master import xrdLseekSet, xrdReadStr
from lsst.qserv.master import xrdReadToLocalFile, xrdOpenWriteReadSaveClose
from lsst.qserv.master import charArray_frompointer, charArray
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

class Persistence:
    def __init__(self):
        self._conn = None
        pass

    def activate(self):
        socket = os.getenv("QSM_DBSOCK", "/data/lsst/run/mysql.sock")
        self._conn = sql.connect(host = "localhost",
                                 user = "test",
                                 passwd = "",
                                 unix_socket = socket,
                                 db = "test")
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

class SleepingThread(threading.Thread):
    def __init__(self, howlong=1.0):
        self.howlong=howlong
        threading.Thread.__init__(self)
    def run(self):
        time.sleep(self.howlong)

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
        del insertPrep
        q = "\n".join([header] + qlist + ["\0\0\0\0"])  
        return [tableName,q]


    
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
        createTemplate = "CREATE TABLE IF NOT EXISTS %s ENGINE=MEMORY ";
        insertTemplate = "INSERT INTO %s ";
        tableTemplate = "result_%s";
        q = ""
        self.db.activate()
        # Drop result table to make room.
        self.applySql("test", "DROP TABLE IF EXISTS result;")
        triedAgain = False
        for chunk in chunkNums:
                dispatchStart = time.time()
                subc = collected[chunk][:2000] # DEBUG: force less subchunks
                # MySQL will probably run out of memory with >2k subchunks.
                header = '-- SUBCHUNKS:' + ", ".join(imap(str,subc))
                
                cq = self.queryMunger.expandSubQueries(chunk, subc)
                tableName = tableTemplate % str(chunk)
                createPrep = createTemplate % tableName
                insertPrep = insertTemplate % tableName
                qlist = []
                if cq:
                    qlist.append(createPrep + cq[0])
                    remain = cq[1:]
                    if remain:
                        qlist.extend(imap(lambda s: insertPrep + s, remain))
                        del remain
                createPrep = ""
                insertPrep = ""
                q = "\n".join([header] + qlist + ["\0\0\0\0"])  
                # \0\0\0\0 is the magic query terminator for the 
                # worker to detect.
                #print "-----------------"
                print "dispatch chunk=", chunk, 
                print "run=%d fin=%d tot=%d" %(len(self.running), 
                                               len(self.finished), 
                                               len(chunkNums))
                #print "header=", header[:70],"..."
                #print "create=", qlist[0]
                del header
                del qlist
                xo = XrdOperation(chunk, q, self.saveTableDump, tableName, self.resultPath) 
                ##id = submitQuery(session, chunk, q, self.resultPath);
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

