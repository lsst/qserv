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
Database Watcher - runs on each Qserv node and maintains Qserv databases (creates databases, deletes databases, creates tables, drops tables etc).

@author  Jacek Becla, SLAC


Known issues and todos:
 * need to go through kvInterface interface, now bypassing it in two places:
    - @self._interface._zk.DataWatch
    - @self._interface._zk.ChildrenWatch
 * considering implementing garbage collection for threads corresponding to
   deleted databases.
 * If all metadata is deleted when the watcher is running, the watcher will die
   with: ERROR:kazoo.handlers.threading:Exception in worker queue thread.
   To reproduce, just run (when watcher is running): 
       echo "drop everything;" | ./qserv_admin.py
"""

import logging
from optparse import OptionParser
import os
import time
import threading

from lsst.qserv.css.kvInterface import KvInterface
from lsst.db.db import Db, DbException


class OneDbWatcher(threading.Thread):
    """
    This class implements a database watcher. Each instance is responsible for
    creating / dropping one database. It is relying on Zookeeper's DataWatch.
    It runs in a dedicated thread.
    """

    def __init__(self, kvI, db, pathToWatch):
        """
        Initialize shared state.
        """
        self._kvI = kvI
        self._db = db
        self._path = pathToWatch
        self._dbName = pathToWatch[11:] # skip '/DATABASES/'
        self._data = None
        self._logger = logging.getLogger("WATCHER.DB_%s" % self._dbName)
        threading.Thread.__init__(self)

    def run(self):
        """
        Watch for changes, and act upon them: create/drop databases.
        """
        @self._kvI._zk.DataWatch(self._path, allow_missing_node=True)
        def my_watcher_func(newData, stat):
            if newData == self._data: return
            if newData is None and stat is None:
                self._logger.info(
                    "Path %s deleted. (was %s)" % (self._path, self._data))
                self._logger.info("Dropping my database")
                self._db.dropDb(self._dbName)
            elif newData == 'PENDING':
                    self._logger.info("Meta not initialized yet for my database.")
            elif newData == 'READY':
                self._logger.info("Creating my database")
                self._db.createDb(self._dbName, mayExist=True)
            else:
                self._logger.error("Unsupported status '%s' for my db." % newData)
            self._data = newData

####################################################################################
class AllDbsWatcher(threading.Thread):
    """
    This class implements watcher that watches for new znodes that represent
    databases. A new OnedbWatcher is setup for each new znode that is creeated. 
    If ensures only one watcher runs, even if a database is created/deleted/created
    again. It currently does NOT do any garbage collection of threads for deleted
    databases. It is relying on Zookeeper's ChildrenWatch. It runs in a dedicated
    thread.
    """

    def __init__(self, kvI, db):
        """
        Initialize shared data.
        """
        self._kvI = kvI
        self._db = db
        self._path =  "/DATABASES"
        self._children = []
        self._watchedDbs = [] # registry of all watched databases
        # make sure the path exists
        if not kvI.exists(self._path): kvI.create(self._path)
        self._logger = logging.getLogger("WATCHER.ALLDBS")
        threading.Thread.__init__(self)

    def run(self):
        """
        Watch for new/deleted nodes, and act upon them: setup new watcher for each
        new database.
        """
        @self._kvI._zk.ChildrenWatch(self._path)
        def my_watcher_func(children):
            # look for new entries
            for val in children:
                if not val in self._children:
                    self._logger.info("node '%s' was added" % val)
                    # set data watcher for this node (unless it is already up)
                    p2 = "%s/%s" % (self._path, val)
                    if p2 not in self._watchedDbs:
                        self._logger.info("Setting a new watcher for '%s'" % p2)
                        w = OneDbWatcher(self._kvI, self._db, p2)
                        w.start()
                        self._watchedDbs.append(p2)
                    else:
                        self._logger.debug("Already have a watcher for '%s'" % p2)
                    self._children.append(val)
            # look for entries that were removed
            for val in self._children:
                if not val in children:
                    self._logger.info("node '%s' was removed" % val)
                    self._children.remove(val)

####################################################################################
class SimpleOptionParser:
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

        (options, args) = parser.parse_args()
        if options.verbT: 
            self._verbosity = int(options.verbT)
            if   self._verbosity > 50: self._verbosity = 50
            elif self._verbosity <  0: self._verbosity = 0
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

    # initialize CSS
    kvI = KvInterface(p.connInfo)

    # initialize database connection, and connect (to catch issues early)
    db = Db(read_default_file=p.credFile)
    try:
        db.connect()
    except DbException as e:
        print e.getErrMgs()
        return

    # setup the thread watching
    try:
        w1 = AllDbsWatcher(kvI, db)
        w1.start()
        while True: time.sleep(60)
    except(KeyboardInterrupt, SystemExit):
        print ""

if __name__ == "__main__":
    main()
