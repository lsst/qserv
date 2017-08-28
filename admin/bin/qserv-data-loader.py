#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014-2015 AURA/LSST.
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
User-friendly data loading script for Qserv partitioned data.

This is a driver script for (currently single-node) data loading process.
Script is loading data for one table at a time and must be called with
these input arguments:
  - database/table name
  - table schema file (SQL statement CREATE TABLE ...)
  - one or more input data files (input for partitioner)
  - partitioner configuration

Script performs these tasks:
  - optionally call partitioner to produce per-chunk data files
  - create database if needed and all chunk tables in it
  - load data into every chunk table
  - update CSS data for database and table

Script can be run in two modes:
  1. Normal partitioning, where partitioned tables have their data split
     into chunks and each chunk is loaded into separate table
  2. Non-partitioned, all table data stored in one table, it is used to load
     non-partitioned tables into Qserv and also for testing and doing
     comparison of qserv to direct mysql queries

Currently not supported is duplication mode when data from one sky segment
is replicated multiple times to other segments to cover whole sky. This
option may be added later.

@author  Andy Salnikov, SLAC

"""

from __future__ import absolute_import, division, print_function

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import sys
import argparse
import logging

# ----------------------------
# Imports for other modules --
# ----------------------------
import lsst.log
from lsst.qserv import css
from lsst.qserv.css import cssConfig
from lsst.qserv.admin.dataLoader import DataLoader
from lsst.qserv.admin.nodeAdmin import NodeAdmin
import lsst.qserv.admin.logger
from lsst.qserv.wmgr.client import WmgrClient

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------


class _LogFilter(object):
    """Filter for logging which passes all records from specified logger only"""

    def __init__(self, loggerName):
        self.loggerName = loggerName
        self.loggerNameDot = loggerName + '.'

    def filter(self, record):
        if record.levelno > logging.INFO:
            return 1
        if record.name == self.loggerName:
            return 1
        if record.name.startswith(self.loggerNameDot):
            return 1
        return 0

# -----------------------
# Exported definitions --
# -----------------------


class Loader(object):
    """
    Application class for loader application
    """

    def __init__(self):
        """
        Constructor parse all arguments and prepares for execution.
        """

        # define all command-line arguments
        parser = argparse.ArgumentParser(description='Single-node data loading script for Qserv.')

        parser.add_argument('-v', '--verbose', dest='verbose', default=[], action='append_const',
                            const=None, help='More verbose output, can use several times.')
        parser.add_argument('--verbose-all', dest='verboseAll', default=False, action='store_true',
                            help='Apply verbosity to all loggers, by default only loader level is set.')
        parser = lsst.qserv.admin.logger.add_logfile_opt(parser)
        group = parser.add_argument_group('Partitioning options',
                                          'Options defining how partitioning is performed')
        group.add_argument('-f', '--config', dest='configFiles', default=[], action='append',
                           required=True, metavar='PATH',
                           help='Partitioner configuration file, required, more than one acceptable.')
        group.add_argument('-d', '--chunks-dir', dest='chunksDir', metavar='PATH',
                           default="./loader_chunks", help='Directory where to store chunk data, must '
                           'have enough space to keep all data. If option --skip-partition is specified '
                           '(without --one-table) then directory must exist and have existing data in it. '
                           'Otherwise directory must be empty or do not exist. def: %(default)s.')
        group.add_argument('-t', '--tmp-dir', dest='tmpDir', metavar='PATH',
                           default=None, help='Directory for non-chunk temporary files, e.g. uncompressed '
                           'data files. By default temporary directory with random name inside chunks-dir '
                           'is created to hold temporary data.')
        group.add_argument('-k', '--keep-chunks', dest='keepChunks', action='store_true', default=False,
                           help='If specified then chunks will not be deleted after loading.')
        group.add_argument('-s', '--skip-partition', dest='skipPart', action='store_true', default=False,
                           help='If specified then skip partitioning, chunks must exist already '
                           'if option --one-table is not specified (from previous run with -k option).')
        group.add_argument('-1', '--one-table', dest='oneTable', action='store_true', default=False,
                           help='If specified then load whole dataset into one table. This is useful for '
                           'testing quries against mysql directly. If --skip-partition is specified '
                           'then original non-partitioned data will be loaded, otherwise data will be '
                           'partitioned but still loaded into a single table.')

        group = parser.add_argument_group('CSS options', 'Options controlling CSS metadata')
        parser.set_defaults(cssConn='mysql://qsmaster@127.0.0.1:13306/qservCssData')
        group.add_argument('-c', '--css-conn', dest='cssConn',
                           help='Connection string for CSS, def: %(default)s.')
        group.add_argument('-r', '--css-remove', dest='cssClear', default=False, action='store_true',
                           help='Remove CSS table info if it already exists.')
        group.add_argument('-C', '--no-css', dest='cssConn', action='store_const', const=None,
                           help='Disable CSS updates.')

        group = parser.add_argument_group('Database options', 'Options for database connection')
        group.add_argument('-H', '--host', dest='czarHost', default='localhost', metavar='HOST',
                           help='Host name for czar wmgr service, def: %(default)s.')
        group.add_argument('-P', '--port', dest='czarPort', default=5012, metavar='PORT_NUMBER', type=int,
                           help='Port number to use for czar wmgr connection, def: %(default)s.')
        group.add_argument('-W', '--worker', dest='workerNodes', default=[], action='append',
                           metavar='STRING', help='Node name for worker server, may be specified '
                           'more than once. If missing then czar server is used to store worker '
                           'data. If more than one node is given then chunks are distributed '
                           'randomly across all hosts. If CSS is used then nodes must already be '
                           'defined in CSS (using qserv-admin command "CREATE NODE ..."). If CSS '
                           'is disabled (with --no-css) then node name will be treated as a host '
                           'name.')
        group.add_argument('-x', '--secret', dest='secret', default=None,
                           help='Path name for the file with wmgr secret.')

        group = parser.add_argument_group('Control options', 'Options for controlling other operations')
        group.add_argument('-E', '--empty-chunks', dest='emptyChunks', default=None, metavar='PATH',
                           help='Path name for "empty chunks" file, if not specified then this file is '
                           'not produced.')
        group.add_argument('-i', '--index-db', dest='indexDb', default='qservMeta', metavar='DB_NAME',
                           help='Name of the database which keeps czar-side object index, def: '
                           '%(default)s. Index is generated only for director table which is specified '
                           'with dirTable option in configuration file. Set to empty string to avoid '
                           'building index. If name is not empty then database must already exist.')
        group.add_argument('-e', '--delete-tables', dest='deleteTables', default=False, action='store_true',
                           help='If specified then existing tables in database will be deleted if '
                           'they exist, this includes both data and metadata.')

        parser.add_argument('database',
                            help='Database name, Expected to exist and have correct permissions.')
        parser.add_argument('table', help='Table name, must not exist.')
        parser.add_argument('schema',
                            help='Table schema file (should contain CREATE [TABLE|VIEW] ... statement).')
        parser.add_argument('data', nargs='*',
                            help='Input data files (CSV or anything that partitioner accepts). '
                            'Input can be empty, e.g. in case of defining SQL view instead of '
                            'regular table.')

        # parse all arguments
        self.args = parser.parse_args()

        # configure logging
        loggerName = None
        if lsst.qserv.admin.logger.setup_logging(self.args.log_conf):
            logger = logging.getLogger()
        else:
            # if global configuration file for logging isn't available
            # then use custom procedure (could be removed to simplify code?)
            loggerName = "Loader"
            verbosity = len(self.args.verbose)
            levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
            if not self.args.verboseAll:
                # suppress INFO/DEBUG regular messages from other loggers
                handler.addFilter(_LogFilter(loggerName))
            logger = logging.getLogger()
            logger.setLevel(level=levels.get(verbosity, logging.DEBUG))
            logger.handlers = []
            logger.addHandler(handler)

        # configure log4cxx logging based on the logging level of Python logger
        levels = {logging.ERROR: lsst.log.ERROR,
                  logging.WARNING: lsst.log.WARN,
                  logging.INFO: lsst.log.INFO,
                  logging.DEBUG: lsst.log.DEBUG}
        lsst.log.setLevel('', levels.get(logger.level, lsst.log.DEBUG))

        # connect to czar server
        czarWmgr = WmgrClient(self.args.czarHost, self.args.czarPort, secretFile=self.args.secret)

        # instantiate CSS interface
        css_inst = None
        if self.args.cssConn:
            logger.debug('connecting to css: %s', self.args.cssConn)
            config = cssConfig.configFromUrl(self.args.cssConn)
            css_inst = css.CssAccess.createFromConfig(config, "")

        # connect to all worker servers
        workerWmgrMap = {}
        for worker in self.args.workerNodes:
            workerWmgrMap[worker] = self._wmgrConnect(worker, css_inst)

        # instantiate loader
        self.loader = DataLoader(self.args.configFiles,
                                 czarWmgr,
                                 workerWmgrMap=workerWmgrMap,
                                 chunksDir=self.args.chunksDir,
                                 keepChunks=self.args.keepChunks,
                                 tmpDir=self.args.tmpDir,
                                 skipPart=self.args.skipPart,
                                 oneTable=self.args.oneTable,
                                 css=css_inst,
                                 cssClear=self.args.cssClear,
                                 indexDb=self.args.indexDb,
                                 emptyChunks=self.args.emptyChunks,
                                 deleteTables=self.args.deleteTables,
                                 loggerName=loggerName)

    def run(self):
        """
        Do actual loading based on parameters defined in constructor.
        This will throw exception if anything goes wrong.
        """
        self.loader.load(self.args.database, self.args.table, self.args.schema, self.args.data)
        if self.loader.chunks:
            logging.getLogger('Loader').info('loaded chunks: %s',
                                             ' '.join([str(c) for c in self.loader.chunks]))
        return 0

    def _wmgrConnect(self, nodeName, css):
        """
        Connect to wmgr server, returns NodeAdmin instance or throws exception.
        """
        if css:
            # will get node info from CSS
            nodeAdmin = NodeAdmin(name=nodeName, css=css, wmgrSecretFile=self.args.secret)
        else:
            # no CSS, use node name as host name
            nodeAdmin = NodeAdmin(host=nodeName, port=self.args.czarPort, wmgrSecretFile=self.args.secret)
        return nodeAdmin.wmgrClient()


if __name__ == "__main__":
    try:
        loader = Loader()
        sys.exit(loader.run())
    except Exception as exc:
        logging.critical('Exception occured: %s', exc, exc_info=True)
        sys.exit(1)
