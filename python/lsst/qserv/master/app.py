# app package for lsst.qserv.master

# Standard Python imports
from itertools import imap
import os
import MySQLdb as sql

# package import
import sqlparser
from lsst.qserv.master import xrdOpen, xrdClose, xrdRead, xrdWrite
from lsst.qserv.master import xrdLseekSet
from lsst.qserv.master import charArray_frompointer, charArray


class Persistence:
    def __init__(self):
        self._conn = None
        pass

    def activate(self):
        self._conn = sql.connect(host = "localhost",
                                 user = "test",
                                 passwd = "",
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
        self._dropSilent(c, ['tasks', 'partmap'])
        c.execute("CREATE TABLE tasks (id int, queryText text);")
        c.execute("CREATE TABLE partmap (%s);" % (", ".join([
                        "chunkId int", "subchunkId int", 
                        "ramin float", "ramax float", 
                        "declmin float", "declmax float"])))
        c.close()
        self._populatePartFake()
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


class QueryAction:
    def __init__(self, query):
        self.queryStr = query
        self.queryMunger = None ## sqlparser.QueryMunger()
        self.db = Persistence()
        pass
    def invoke(self):
        self.queryMunger = sqlparser.QueryMunger(self.queryStr)
        
        query = self.queryMunger.computePartMapQuery()
        print "partmapquery is", query
        p = Persistence()
        p.activate()
        chunktuples = p.issueQuery(query)
        collected = self.queryMunger.collectSubChunkTuples(chunktuples)
        createTemplate = "CREATE TABLE IF NOT EXISTS %s ";
        tableTemplate = "result_%s";
        q = ""
        for chunk in collected:
            if False: ## Deprecated
                q = self.queryMunger.computeChunkQuery(chunk, 
                                                       collected[chunk])
            else:
                header = '-- SUBCHUNKS:' + ", ".join(imap(str,collected[chunk]))
                #header = '-- SUBCHUNKS:0,1'
                cq = self.queryMunger.expandSubQueries(chunk,
                                                       collected[chunk])
                tableName = tableTemplate % str(chunk)
                resultPrepend = createTemplate % tableName
                q = "\n".join([header] + map(lambda s: resultPrepend+s, 
                                             cq))
            success = self.issueXrd(chunk, q, 
                                    lambda d: self.mergeTableDump(tableName, d))

            
            if not success: 
                print "Unsuccessful with %s on chunk %d" % (self.queryStr, chunk)
                break
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

    def issueXrd(self, chunk, query, outputFunc):

        print "Asked to issue '%s' to xrd." % query
        # debug:
        #query = "--SUBCHUNKS: 0\nselect * from qserv.dummy_%1% ;"

        ## Will need to add specificity for particular tables
        host = "lsst-dev01"
        port = 1094
        user = "qsmaster"
        urlTempl = "xroot://%s@%s:%d//query/%d" % (user, host, port, chunk)
        print "Issuing (%d)" % chunk, "via", urlTempl
        successful = True
        ## return
        handle = xrdOpen(urlTempl, os.O_RDWR)
        q = query
        wCount = xrdWrite(handle, charArray_frompointer(q), len(q))
        #print "wrote ", wCount, "out of", len(q)
        resultBufferList = []
        if wCount == len(q):
            xrdLseekSet(handle, 0L); ## Seek to beginning to read from beginning.
            while True:
                bufSize = 65536 # lower level may ignore, so may want to set big.
                buf = charArray(bufSize)
                rCount = xrdRead(handle, buf, bufSize)
                print "recv", rCount
                if rCount <= 0:
                    successful = False
                    break ## 
                s = "".join(map(lambda x: buf[x], range(rCount)))
                resultBufferList.append(s)
                ##print "(", s, ")"
                # always quit right now. 

                if rCount < bufSize:
                    break
                ## break
        else:
            successful = False
        xrdClose(handle)

        if resultBufferList:
            # print "Result buffer is", 
            # for s in resultBufferList:
            #     print "----",s,"----"
            outputFunc("".join(resultBufferList))
        return successful

    def mergeTableDump(self, tableName, dump):
        db = "test"
        # Apply dump.  This ingests the dump into a table named tableName
        self.applySql(db, dump)

        ## Might need to specially handle null results.
        # Merge table results into the real results table.
        mergeSql = "CREATE TABLE IF NOT EXISTS result SELECT * FROM %s;"
        self.applySql(db, mergeSql % tableName)

        # Get rid of the table we ingested, since we've used it.
        self.applySql(db, "DROP TABLE %s;" % tableName)
        pass

    def applySql(self, dbName, qtext):
        self.db.activate()
        r = self.db.issueQuery(("USE %s;" % dbName) + qtext)
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

