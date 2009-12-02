# app package for lsst.qserv.master

# Standard Python imports
from itertools import imap
import os
import sys
import threading
import time
import MySQLdb as sql
from string import Template

# package import
import sqlparser
from lsst.qserv.master import xrdOpen, xrdClose, xrdRead, xrdWrite
from lsst.qserv.master import xrdLseekSet, xrdReadStr
from lsst.qserv.master import charArray_frompointer, charArray


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

class XrdOperation(threading.Thread):
        def __init__(self, chunk, query, outputFunc, outputArg):
            threading.Thread.__init__(self)

            self.chunk = chunk
            self.query = query
            self.outputFunc = outputFunc
            self.outputArg = outputArg
            self.url = ""
            self.setXrd()
            self.successful = None
            pass

        def setXrd(self):
            hostport = os.getenv("QSERV_XRD","lsst-dev01:1094")
            user = "qsmaster"
            self.url = "xroot://%s@%s//query/%d" % (user, hostport, self.chunk)

        def run(self):
            print "Issuing (%d)" % self.chunk, "via", self.url
            self.successful = True
            handle = xrdOpen(self.url, os.O_RDWR)
            q = self.query
            wCount = xrdWrite(handle, charArray_frompointer(q), len(q))
            #print "wrote ", wCount, "out of", len(q)
            resultBufferList = []
            if wCount == len(q):
                print self.url, "Wrote OK"
                xrdLseekSet(handle, 0L); ## Seek to beginning to read from beginning.
                while True:
                    bufSize = 8192000
                    buf = "".center(bufSize) # Fill buffer
                    rCount = xrdReadStr(handle, buf)
                    tup = (self.chunk, len(resultBufferList), rCount)
                    print "chunk %d [packet %d] recv %d" % tup
                    if rCount <= 0:
                        successful = False
                        break ## 
                    resultBufferList.append(buf[:rCount])
                    if rCount < bufSize:
                        break
                    pass
                self.successful = True
            else:
                print self.url, "Write failed!"
                self.successful = False
            xrdClose(handle)
            if resultBufferList:
                # print "Result buffer is", 
                # for s in resultBufferList:
                #     print "----",s,"----"
                self.outputFunc(self.outputArg, "".join(resultBufferList))
            print "[", self.chunk, "complete]",
            return self.successful
        pass

class QueryAction:
    def __init__(self, query):
        self.queryStr = query
        self.queryMunger = None ## sqlparser.QueryMunger()
        self.db = Persistence()
        self.running = {}
        self.resultLock = threading.Lock()
        self.finished = {}
        self.threadHighWater = 50
        pass

    def _joinAny(self):
        poppable = []
        while (not poppable) and (self.running):  # keep waiting 
            for (chunk, thread) in self.running.items():
                if not thread.isAlive(): 
                    poppable.append(chunk)
            if not poppable: 
                time.sleep(0.5) # Don't spin-check too fast.
        for p in poppable: # Move the threads out.
            t = self.running.pop(p)
            self.finished[p] = t
            if not t.successful:
                print "Unsuccessful with %s on chunk %d" % (self.queryStr, p)
        pass
        
    def invoke(self):
        self.queryMunger = sqlparser.QueryMunger(self.queryStr)
        
        query = self.queryMunger.computePartMapQuery()
        print "partmapquery is", query
        p = Persistence()
        p.activate()
        chunktuples = p.issueQuery(query)
        collected = self.queryMunger.collectSubChunkTuples(chunktuples)
        #collected = dict(collected.items()[:3]) ## DEBUG: Force only 3 chunks
        createTemplate = "CREATE TABLE IF NOT EXISTS %s ";
        insertTemplate = "INSERT INTO %s ";
        tableTemplate = "result_%s";
        q = ""
        self.db.activate()
        # Drop result table to make room.
        self.applySql("test", "DROP TABLE IF EXISTS result;")

        for chunk in collected:
                subc = collected[chunk][:2] # DEBUG: force only 2 subchunks
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
                q = "\n".join([header] + qlist)
                #print "-----------------",q
                if len(self.running) > self.threadHighWater:
                    print "Reaping"
                    self._joinAny()
                xo = XrdOperation(chunk, q, self.mergeTableDump, tableName) 
                xo.start()
                self.running[chunk] = xo

        for (c,xo) in self.running.items():
            xo.join()
            if not xo.successful:
                print "Unsuccessful with %s on chunk %d" % (self.queryStr, c)
            
        print "results available in db test, as table 'result'"
        #print self.queryStr, "resulted in", query
        ## sqlparser.test(self.queryStr)
        
        # Want parser to:
        # a) help me munge the query for the partmap DB
        # b) help me re-form the original query for the chunk/subchunk pairs.
        
        #  tokens = sqlparser.getTokens(self.queryStr)
        
        # Then, for each chunk/subchunk, dispatch the cmd via xrd.
        # Later, we will fork for parallelism.  Test serially first.
         
        pass
        

    def mergeTableDump(self, tableName, dump):
        db = "test"
        targetTable = "result"

        self.resultLock.acquire()

        # Apply dump.  This ingests the dump into a table named tableName
        # The dump should be newline-separated.
        self.applySqlSep(db, dump, ";\n")

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

        
        
class XrdAction:
    def __init__(self):
        self.handle = None
        pass

    def invoke(self):
        self.handle = xrdfile.xrdOpen(self.xrdPath)
        writtenbytes = xrdfile.xrdWrite(self.handle)
        readbytes = xrdfile.xrdRead(self.handle)
        xrdfile.xrdClose(self.handle)
        pass
    

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

