#
# lsst.qserv.master.db - Package for direct-db interaction.
#

# Mysql
import MySQLdb as sql

# Package
import lsst.qserv.master.config

class Db:
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

    def getCursor(self):
        return self._conn.cursor()

    def applySql(self, sql):
        c = self._conn.cursor()
        c.execute(sql)
        return c.fetchall()    


class TaskDb:
    def __init__(self):
        self._db = Db()
        pass
    def check(self):
        return self._db.check()
    def activate(self):
        return self._db.activate()

    def _dropSilent(self, cursor, tables):
        for t in tables:
            try:
                cursor.execute('DROP TABLE %s;' %t)
            finally:
                pass
        pass

    def makeTables(self):
        c = self._db.getCursor()
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
        c = self._db.getCursor()
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
        assert self._db.check()
        c = self._db.getCursor()
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
        assert self._db.check()
        if taskparam[0] == None:
            a = list(taskparam)
            a[0] = int(self.nextId())
            assert type(a[0]) is int
            taskparam = tuple(a)
        taskstr = str(taskparam)
        sqlstr = 'INSERT INTO tasks VALUES %s' % taskstr
        print "---",sqlstr
        self._db.getCursor().execute(sqlstr)
        return a[0]

    def issueQuery(self, query):
        return self._db.applySql(self, query)
    pass
