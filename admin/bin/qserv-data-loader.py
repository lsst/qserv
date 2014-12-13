#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014 AURA/LSST.
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
  2. Non-partitioned, all table data stored in one table, this is useful
     for testing and doing comparison of qserv to direct mysql queries

Currently not supported is duplication mode when data from one sky segment
is replicated multiple times to other segments to cover whole sky. This
option will be added later.

@author  Andy Salnikov, SLAC

"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import sys
import argparse
import logging
import MySQLdb as mysql
import warnings

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.qserv.admin.dataLoader import DataLoader

#----------------------------------
# Local non-exported definitions --
#----------------------------------

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

#------------------------
# Exported definitions --
#------------------------
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
        group.add_argument('-k', '--keep-chunks', dest='keepChunks', action='store_true', default=False,
                           help='If specified then chunks will not be deleted after loading.')
        group.add_argument('-K', '--keep-input-data', dest='keepInputData', action='store_true', default=False,
                           help='If specified then input data will not be deleted after loading.')
        group.add_argument('-s', '--skip-partition', dest='skipPart', action='store_true', default=False,
                           help='If specified then skip partitioning, chunks must exist already '
                           'if option --one-table is not specified (from previous run with -k option).')
        group.add_argument('-1', '--one-table', dest='oneTable', action='store_true', default=False,
                           help='If specified then load whole dataset into one table. This is useful for '
                           'testing quries against mysql directly. If --skip-partition is specified '
                           'then original non-partitioned data will be loaded, otherwise data will be '
                           'partitioned but still loaded into a single table.')

        group = parser.add_argument_group('CSS options', 'Options controlling CSS metadata')
        parser.set_defaults(cssConn='localhost:12181')
        group.add_argument('-c', '--css-conn', dest='cssConn',
                           help='Connection string for zookeeper, def: %(default)s.')
        group.add_argument('-r', '--css-remove', dest='cssClear', default=False, action='store_true',
                           help='Remove CSS table info if it already exists.')
        group.add_argument('-C', '--no-css', dest='cssConn', action='store_const', const=None,
                           help='Disable CSS updates.')

        group = parser.add_argument_group('Database options', 'Options for database connection')
        group.add_argument('-H', '--host', dest='mysqlHost', default='localhost', metavar='HOST',
                           help='Host name for mysql server, def: %(default)s.')
        group.add_argument('-u', '--user', dest='user', default=None,
                           help='User name to use when connecting to server.')
        group.add_argument('-p', '--password', dest='password', default=None,
                           help='Password to use when connecting to server.')
        group.add_argument('-P', '--port', dest='mysqlPort', default=3306, metavar='PORT_NUMBER',
                           help='Port number to use for connection, def: %(default)s.')
        group.add_argument('-S', '--socket', dest='mysqlSocket', default=None, metavar='PATH',
                           help='The socket file to use for connection.')

        group = parser.add_argument_group('Control options', 'Options for controlling other operations')
        group.add_argument('-E', '--empty-chunks', dest='emptyChunks', default=None, metavar='PATH',
                           help='Path name for "empty chunks" file, if not specified then this file is '
                           'not produced.')
        group.add_argument('-i', '--index-db', dest='indexDb', default='qservMeta', metavar='DB_NAME',
                           help='Name of the database which keeps czar-side object index, '
                           'def: %(default)s. Set to empty string to avoid building index. '
                           'If name is not empty then database must already exist.')
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

        # suppress some warnings from mysql
        warnings.filterwarnings('ignore', category=mysql.Warning)

        # parse all arguments
        self.args = parser.parse_args()

        # configure logging
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
        logger.addHandler(handler)

        # connect to mysql server
        mysqlConn = self._dbConnect()

        # instantiate loader
        self.loader = DataLoader(self.args.configFiles,
                                 mysqlConn,
                                 chunksDir=self.args.chunksDir,
                                 keepChunks=self.args.keepChunks,
                                 keepInputData=self.args.keepInputData,
                                 skipPart=self.args.skipPart,
                                 oneTable=self.args.oneTable,
                                 cssConn=self.args.cssConn,
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
            logging.getLogger('Loader').info('loaded chunks: %s', ' '.join(map(str, self.loader.chunks)))
        return 0


    def _dbConnect(self):
        """
        Connect to mysql server, returns connection or throws exception.
        """
        kws = dict(local_infile=1)
        if self.args.mysqlHost:
            kws['host'] = self.args.mysqlHost
        if self.args.user:
            kws['user'] = self.args.user
        if self.args.password:
            kws['passwd'] = self.args.password
        if self.args.mysqlPort:
            kws['port'] = self.args.mysqlPort
        if self.args.mysqlSocket:
            kws['unix_socket'] = self.args.mysqlSocket

        try:
            return mysql.Connection(**kws)
        except Exception as exc:
            logging.critical('Failed to connect to mysql database: %s', exc)
            raise


if __name__ == "__main__":
    try:
        loader = Loader()
        sys.exit(loader.run())
    except Exception as exc:
        logging.critical('Exception occured: %s', exc, exc_info=True)
        sys.exit(1)
