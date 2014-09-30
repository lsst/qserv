#
# LSST Data Management System
# Copyright 2009-2014 LSST Corporation.
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

# lsst.qserv.czar.db - Package for direct-db interaction.
#
# This package is meant to abstract database interaction so that
# efforts to port qserv to other database implementations can be
# localized here.
#
# Non-functioning task-tracking db operations reside here as well.

import sys
# Mysql
import MySQLdb as sql
import _mysql_exceptions

# Package
import lsst.qserv.czar.config
from lsst.qserv.czar import logger

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
        config = lsst.qserv.czar.config.config
         #os.getenv("QSM_DBSOCK", "/data/lsst/run/mysql.sock")
        socket = config.get("resultdb", "unix_socket")
        user = config.get("resultdb", "user")
        passwd = config.get("resultdb", "passwd")
        host = config.get("resultdb", "host")
        port = config.getint("resultdb", "port")
        db = config.get("resultdb", "db")
        #logger.inf("DB CONNECT: ", user, " p=", passwd, " sock=", socket, " db=", db)
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
                logger.err("Couldn't connect using file", socket, e)
                msg = "Couldn't connect using %s:%s" %(host,port)
                logger.err(msg, e2)
                self._conn = None
                return
        c = self._conn.cursor()
        # should check if db exists here.
        # Database gets populated with fake data automatically, but
        # the db "test" must exist.
        pass

    def getCursor(self):
        return self._conn.cursor()

    def applySql(self, sql, params=None):
        """
        Execute SQL statement, retry in case of some errors.
        @param sql:       query string
        @param params:    optional parameters for query
        @return  all rows of the query result
        """
        failures = 0
        while True:
            c = self._conn.cursor()
            try:
                c.execute(sql, params)
                break # Success: break out of the loop
            except _mysql_exceptions.OperationalError, e:
                failures += 1
                if failures > 5: # MAGIC 5
                    logger.wrn("Too many SQL failures, not retrying.")
                    self._conn = None
                    return None
                logger.err("operational error, retrying", e)
            pass # Try again
        return c.fetchall()

    def makeIfNotExist(self, db=None, table=None):
        """
        Create a database and/or a table
        @param db:    name of db to create
        @param table: (tableName, columDefStr)
        """
        dbTmpl = "CREATE DATABASE IF NOT EXISTS %s;"
        tblTmpl = "CREATE TABLE IF NOT EXISTS %s %s;"
        res = []
        res.append(self.applySql(dbTmpl % db))
        if table:
            res.append(self.applySql(tblTmpl % (table[0], table[1])))
        return res

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
        sqlstr = 'INSERT INTO partmap VALUES (%s, %s, %s, %s, %s, %s);'
        for cTuple in fakeRows:
            c.execute(sqlstr, cTuple)
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
        """
        taskparam should be a tuple of (id, query)
        You can pass None for id, and let the system assign a safe id.
        """
        assert self._db.check()
        if taskparam[0] == None:
            a = list(taskparam)
            a[0] = int(self.nextId())
            assert type(a[0]) is int
            taskparam = tuple(a)
        sqlstr = 'INSERT INTO tasks VALUES (%s, %s)'
        logger.inf("---",sqlstr)
        self._db.getCursor().execute(sqlstr, taskparam)
        return a[0]

    def issueQuery(self, query):
        return self._db.applySql(self, query)
