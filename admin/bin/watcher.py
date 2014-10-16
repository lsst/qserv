#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2014 LSST Corporation.
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

"""
Database Watcher - runs on each Qserv node and maintains Qserv databases
(creates databases, deletes databases, creates tables, drops tables etc).

This script will be a long-running daemon which watches the state of
metadata database (currently works with zookeeper only) and makes sure
that state of the local mysql server correctly reflects the state of
metadata by creating/deleting databases and tables.

The implementation is based on kazoo watchers. There are several watchers
involved:
- session watcher which re-starts everyhting when zookeeper disconnects
  and then re-connects again (disconnects loose regular watchers)
- "root exists" watcher which is called when root node (/DBS) is created
  or destroyed. It is needed to restart other watchers in case root node
  disappears for some reason (which causes loss of children watchers)
- "root children" watcher which is called when root node children are
  added or removed, it starts database watcher with each new child
- database watchers observe state of the database nodes (/DBS<DBNAME>)
  by reading data associated with the node and create/delete
  corresponding database instance in mysql.

@author  Jacek Becla, SLAC

Known issues and todos:
 * need to go through kvInterface interface, now bypassing it
"""

import logging
from optparse import OptionParser
import os
import Queue

from kazoo.client import KazooState
from kazoo.protocol.states import EventType, WatchedEvent
from kazoo.exceptions import NoNodeError, KazooException
from lsst.qserv.css.kvInterface import KvInterface
from lsst.db.db import Db, DbException


####################################################################################

class CreateDatabase(object):
    """
    Action class which is responsible for creating databases.

    Instances of this class will be added to the action queue.
    """
    def __init__(self, dbName):
        self._dbName = dbName

    def __call__(self, db):
        """
        Execute an action. Method takes database instance as an argument.
        """
        logging.info('Creating database %s', self._dbName)
        db.createDb(self._dbName, mayExist=True)

####################################################################################

class DropDatabase(object):
    """
    Action class which is responsible for deleting databases.

    Instances of this class will be added to the action queue.
    """
    def __init__(self, dbName):
        self._dbName = dbName

    def __call__(self, db):
        """
        Execute an action. Method takes database instance as an argument.
        """
        logging.info('Dropping database %s', self._dbName)
        db.dropDb(self._dbName)

####################################################################################

class SessionListener(object):
    """ Class defining callable watcher for kazoo session callbacks """

    def __init__(self, zk, rootExists, node):
        self._zk = zk
        self._rootExists = rootExists
        self._node = node
        self._logger = logging.getLogger("WATCHER.SessionListener")

    def __call__(self, state):
        if state == KazooState.LOST:
            self._logger.warn("zookeeper session is lost")
        elif state == KazooState.SUSPENDED:
            self._logger.warn("zookeeper session is suspended")
        else:
            self._logger.info("zookeeper session is connected")

            # we need to restart whole shebang again, for that fake
            # the CREATED event
            event = WatchedEvent(EventType.CREATED, state, self._node)
            self._rootExists(event)

####################################################################################

class RootExistsListener(object):
    """ Class defining callable watcher for root node (/DBS) """

    def __init__(self, zk, rootChildren):
        self._zk = zk
        self._rootChildren = rootChildren
        self._logger = logging.getLogger("WATCHER.RootExistsListener")

    def __call__(self, event):

        self._logger.info("event: %s", event)
        if event.type == EventType.CREATED:

            # the node has been created, need to start watching it
            # for new children, use fake event for that
            self._rootChildren(WatchedEvent(EventType.CHILD, event.state, event.path))
            wexists = True

        elif event.type == EventType.DELETED:
            # root node being deleted is something out of ordinary,
            # should not normally happen
            wexists = False

        elif event.type == EventType.CHANGED:
            # root node being deleted is something out of ordinary,
            # should not normally happen
            wexists = True

        # get current state and re-register callback, note that node state
        # could have changed between callback being triggered and this point
        exists = self._zk.retry(self._zk.exists, event.path, self)
        if exists and not wexists:
            # if current state is different from triggered state then do children
            self._rootChildren(WatchedEvent(EventType.CHILD, event.state, event.path))

####################################################################################

class RootChildrenListener(object):
    """ Class defining callable watcher for root node (/DBS) """

    def __init__(self, zk, queue):
        self._zk = zk
        self._queue = queue
        self._logger = logging.getLogger("WATCHER.RootChildrenListener")
        self._children = {}

    def __call__(self, event):
        self._logger.info("event: %s", event)

        if event.type == EventType.DELETED:
            # This is not supposed to happen in normal life but have
            # to handle it anyways
            children = []
        else:
            # get current set of children and re-register callback.
            # could it fail if the node is deleted simultaneously?
            try:
                children = self._zk.get_children(event.path, self)
            except NoNodeError:
                self._logger.info("node disappeared")
                children = []
            except KazooException:
                # unexpected exception, this is problematic as watcher is likely
                # not registered if exception happened. We could retry but this could
                # cause delays which are potential problems in callbacks. For now just
                # return like nothing had happened
                self._logger.warning("exception in get_children(), call, callback is likely lost",
                                     exc_info=True)
                return

        # filter out bad names
        children = set(RootChildrenListener.filterNodes(children))

        # look at the new list, if there is anything new there make a watcher for it
        for node in children:
            if node not in self._children:

                path = event.path + '/' + node
                self._logger.info("create ChildrenDataListener watcher for %s", path)
                watcher = ChildrenDataListener(self._zk, self._queue)
                self._children[node] = watcher

                # make it start its own watching by faking a CREATED event
                watcher(WatchedEvent(EventType.CREATED, event.state, path))

        # remove any watchers that disappeared
        old = set(self._children.keys())
        for node in old-children:
            del self._children[node]

    @staticmethod
    def filterNodes(nodes):
        """Remove special names from the node list"""
        for node in nodes:
            if node.endswith(".LOCK"):
                continue
            yield node

####################################################################################

class ChildrenDataListener(object):
    """ Class defining callable watcher for database nodes (/DBS/<DBNAME>) """

    def __init__(self, zk, queue):
        self._zk = zk
        self._queue = queue
        self._logger = logging.getLogger("WATCHER.ChildrenDataListener")
        self._dbExists = None      # one of (True, False, None)

    def __call__(self, event):
        self._logger.info("event: %s", event)

        # logic is screwed up here, thanks to kazoo one-time watchers
        if event.type == EventType.DELETED:
            # ternary logic here (True, False, None)
            if self._dbExists != False:
                self._logger.info("Path %s deleted", event.path)
                dbName = event.path.split('/')[-1]
                self._logger.info("Add action to drop database %s", dbName)
                self._queue.put(DropDatabase(dbName))
                self._dbExists = False

        # check node state (its data), create database if needed,
        # note that node can disappear at any moment
        try:
            data, dummy_stat = self._zk.get(event.path, self)

            # ternary logic for _dbExists here (True, False, None)
            if data == 'READY' and self._dbExists == True: return

            dbName = event.path.split('/')[-1]
            self._logger.info("Add action to create database %s", dbName)
            self._queue.put(CreateDatabase(dbName))
            self._dbExists = True

        except NoNodeError:

            # ternary logic here (True, False, None)
            if self._dbExists == False: return

            dbName = event.path.split('/')[-1]
            self._logger.info("Path %s deleted", event.path)
            self._logger.info("Add action to drop database %s", dbName)
            self._queue.put(DropDatabase(dbName))
            self._dbExists = False

        except KazooException:

            # unexpected exception, this is problematic as watcher is likely
            # not registered if exception happened. We could retry but this could
            # cause delays which are potential problems in callbacks. For now just
            # return like nothing had happened
            self._logger.warning("exception in get(), call, callback is likely lost",
                                 exc_info=True)
            return

####################################################################################
class SimpleOptionParser(object):
    """
    Parse command line options.
    """

    def __init__(self):
        self._verbosity = 40 # default is ERROR
        self._logFN = None
        self._connI = '127.0.0.1:2181' # default for kazoo (single node, local)
        self._credF = '~/.my.cnf'

        self._usage = \
"""

NAME
        watcher - Watches CSS and acts upon changes to keep node up to date.

SYNOPSIS
        watcher [OPTIONS]

OPTIONS
   -v, --verbose=#
        Verbosity threshold. Logging messages which are less severe than
        provided will be ignored. Expected value range: 0=50: (CRITICAL=50,
        ERROR=40, WARNING=30, INFO=20, DEBUG=10). Default value is ERROR.
   -f, --logFile
        Name of the output log file. If not specified, the output goes to stderr.
   -c, --connection=name
        Connection information for the metadata server.
   -a, --credFile=name
        Credential file containing MySQL connection information. Default location:
        ~/.my.cnf
"""

    @property
    def verbosity(self):
        return self._verbosity

    @property
    def logFileName(self):
        return self._logFN

    @property
    def connInfo(self):
        return self._connI

    @property
    def credFile(self):
        return self._credF

    def parse(self):
        """
        Parse options.
        """
        parser = OptionParser(usage=self._usage)
        parser.add_option("-v", "--verbose",    dest="verbT")
        parser.add_option("-f", "--logFile",    dest="logFN")
        parser.add_option("-c", "--connection", dest="connI")
        parser.add_option("-a", "--credFile",   dest="credF")

        (options, dummy_args) = parser.parse_args()
        if options.verbT:
            self._verbosity = int(options.verbT)
            if   self._verbosity > 50: self._verbosity = 50
            elif self._verbosity < 0: self._verbosity = 0
        if options.logFN: self._logFN = options.logFN
        if options.connI: self._connI = options.connI
        if options.credF: self._credF = options.credF

####################################################################################
def main():
    # parse arguments
    p = SimpleOptionParser()
    p.parse()

    # configure logging
    if p.logFileName:
        logging.basicConfig(
            filename=p.logFileName,
            format='%(asctime)s %(name)s %(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=p.verbosity)
    else:
        logging.basicConfig(
            format='%(asctime)s %(name)s %(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=p.verbosity)

    # disable kazoo logging if requested
    kL = os.getenv('KAZOO_LOGGING')
    if kL: logging.getLogger("kazoo.client").setLevel(int(kL))

    # initialize database connection, and connect (to catch issues early)
    db = Db(read_default_file=p.credFile)
    try:
        db.connect()
    except DbException as e:
        logging.error("%s", e.getErrMgs())
        return

    # initialize CSS
    kvI = KvInterface(p.connInfo)

    # we only need Kazoo client instance
    zk = kvI._zk

    # queue used by callbacks to push actions into it
    queue = Queue.Queue()

    # make few initial ZK listeners/watchers
    root = '/DBS'
    rootChildrenListener = RootChildrenListener(zk, queue)
    rootExistsListener = RootExistsListener(zk, rootChildrenListener)

    # add session watcher to handle ZK disappearing/reappearing
    session_listener = SessionListener(zk, rootExistsListener, root)
    zk.add_listener(session_listener)

    # start whole business, for that we fake connected event
    session_listener(KazooState.CONNECTED)

    while True:
        # get next action from a queue
        try:
            # timeout is needed to be able to terminate it with Ctrl-C
            action = queue.get(False, 1)
        except Queue.Empty:
            continue
        # execute action
        try:
            action(db)
        except Exception:
            # say something but do not stop
            logging.error('exception while executing database action', exc_info=True)
        queue.task_done()

if __name__ == "__main__":
    main()
