import MySQLdb as sql

import sqlparser


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
        self.querystring = query
        pass
    def invoke(self):

        sqlparser.test(self.querystring)
        tokens = sqlparser.getTokens(self.querystring)
         
        print "null query invoke"
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
    

