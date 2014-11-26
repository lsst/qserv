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
import os
import re
import argparse
import itertools
import logging
import shutil
import subprocess
import tempfile
import MySQLdb as mysql
import warnings
import UserDict

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.qserv.admin.qservAdmin import QservAdmin
from lsst.qserv.admin.configParser import ConfigParser

#----------------------------------
# Local non-exported definitions --
#----------------------------------

class PartOptions(UserDict.UserDict):
    """
    Class which holds all table partition options.
    Implemented as a dictionary with some extra methods.
    """

    # keys that must be defined in partitioner config files
    requiredConfigKeys = ['part.num-stripes', 'part.num-sub-stripes', 'part.overlap',
                          ]

    def __init__(self, files):
        """
        Process all config files, throws on error
        """

        UserDict.UserDict.__init__(self)

        def _options(group):
            '''massage options list, takes list of (key, opt) pairs'''
            options = list(opt for k, opt in group)
            if len(options) == 1:
                options = options[0]
            return options

        for config in files:

            # parse whole thing
            try:
                cfgParser = ConfigParser(open(config))
                options = cfgParser.parse()
            except Exception as ex:
                logging.error('Failed to parse configuration file: %s', ex)
                raise

            # options are returned as a list of (key, value) pairs, there will be
            # more than one key appearance for some options, merge this together and
            # make a dict out of it
            options.sort()
            options = dict((key, _options(group)) for key, group
                           in itertools.groupby(options, lambda pair: pair[0]))

            # in partitioner config files loaded earlier have higer priority
            # (options are not overwritten by later configs), do the same here
            options.update(self.data)
            self.data = options

        # check that we have a set of required options defined
        for key in self.requiredConfigKeys:
            if key not in self.data:
                logging.error('Required option is missing from configuration files: %s', key)
                raise KeyError('missing required option')

    @property
    def partitioned(self):
        """Returns True if table is partitioned"""
        return 'part.pos' in self.data or 'part.pos1' in self.data

    def cssDbOptions(self):
        """
        Returns dictionary of CSS options for database.
        """
        options = {'nStripes': self['part.num-stripes'],
                   'nSubStripes': self['part.num-sub-stripes'],
                   'storageClass': self.get('storageClass', 'L2')
                   }
        return options

    def cssTableOptions(self):
        """
        Returns dictionary of CSS options for a table.
        """
        options = {'compression': '0',
                   'dirTable': self.get('dirTable', 'Object'),
                   'dirColName': self.get('dirColName', 'objectId')
                   }

        # refmatch table has part.pos1 instead of part.pos, CSS expects a string, not a number
        isRefMatch = 'part.pos1' in self and 'part.pos2' in self
        options['match'] = '1' if isRefMatch else '0'

        if 'part.pos' in self:
            pos = self['part.pos'].split(',')
            raCol, declCol = pos[0].strip(), pos[1].strip()
            options['latColName'] = declCol
            options['lonColName'] = raCol
            options['overlap'] = self['part.overlap']

        return options

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

        group = parser.add_argument_group('Partitioning options',
                                          'Options defining how partitioning is performed')
        group.add_argument('-f', '--config', dest='part_config', default=[], action='append',
                           required=True, metavar='PATH',
                           help='Partitioner configuration file, required, more than one acceptable.')
        group.add_argument('-d', '--chunks-dir', dest='chunks_dir', metavar='PATH',
                           default="./loader_chunks", help='Directory where to store chunk data, must '
                           'have enough space to keep all data. If option --skip-partition is specified, '
                           'then directory must exist and have existing data in it. Otherwise directory '
                           'must be empty or do not exist. def: %(default)s.')
        group.add_argument('-k', '--keep-chunks', dest='keep_chunks', action='store_true', default=False,
                           help='If specified then chunks will not be deleted after loading.')
        group.add_argument('-s', '--skip-partition', dest='skip_part', action='store_true', default=False,
                           help='If specified then skip partitioning, chunks must exist already '
                           '(from previous run with -k option).')
        group.add_argument('-1', '--one-table', dest='one_table', action='store_true', default=False,
                           help='If specified then load whole dataset into one table, even if it is '
                           'partitioned. This is useful for testing quries against mysql directly.')

        group = parser.add_argument_group('CSS options', 'Options controlling CSS metadata')
        group.add_argument('-c', '--css-conn', dest='css_conn', default='localhost:12181',
                           help='Connection string for zookeeper, def: %(default)s.')
        group.add_argument('-r', '--css-remove', dest='css_remove', default=False, action='store_true',
                           help='Remove CSS table info if it already exists.')

        group = parser.add_argument_group('Connection options', 'Options for database connection')
        group.add_argument('-H', '--host', dest='mysql_host', default='localhost', metavar='HOST',
                           help='Host name for mysql server, def: %(default)s.')
        group.add_argument('-u', '--user', dest='user', default=None,
                           help='User name to use when connecting to server.')
        group.add_argument('-p', '--password', dest='password', default=None,
                           help='Password to use when connecting to server.')
        group.add_argument('-P', '--port', dest='mysql_port', default=3306, metavar='PORT_NUMBER',
                           help='Port number to use for connection, def: %(default)s.')
        group.add_argument('-S', '--socket', dest='mysql_socket', default=None, metavar='PATH',
                           help='The socket file to use for connection.')

        parser.add_argument('database', help='Database name, will be created if it does not exist.')
        parser.add_argument('table', help='Table name, must not exist.')
        parser.add_argument('schema', help='Table schema file (CREATE [TABLE|VIEW] ... statement).')
        parser.add_argument('data', nargs='*',
                            help='Input data files (CSV or anything that partitioner accepts).')

        # suppress some warnings from mysql
        warnings.filterwarnings('ignore', category=mysql.Warning)

        # parse all arguments
        self.args = parser.parse_args()

        verbosity = len(self.args.verbose)
        levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
        logging.basicConfig(level=levels.get(verbosity, logging.DEBUG))

        self.chunkPrefix = 'chunk'
        self.chunkRe = re.compile('^' + self.chunkPrefix + '_(?P<id>[0-9]+)(?P<ov>_overlap)?[.]txt$')
        self.css = None
        self.partOptions = None
        self.cleanupChunksDir = False
        self.unzipDir = None   # directory used for uncompressed data
        self.files = None      # list of data files after uncompressing
        self.schema = None     # "CREATE TABLE" statement


    def run(self):
        """
        Do actual loading based on parameters defined in constructor.
        This will throw exception if anyhting goes wrong.
        """

        try:
            return self._run()
        finally:
            self._cleanup()


    def _run(self):
        """
        Do loading only, cleanup is done in _cleanup()
        """

        # parse all config files
        self.partOptions = PartOptions(self.args.part_config)

        # mysql connection
        self.mysql = self._dbConnect()

        # css instance
        self.css = QservAdmin(self.args.css_conn)

        # see if database is already defined in CSS and get its partitioning info
        self._checkCss()

        # make chunks directory or check that there are usable data there already
        self._makeOrCheckChunksDir()

        # uncompress data files that are compressed, this is only needed if we
        # table is not partitioned or if we are not reusing existing chunks
        if not self.partOptions.partitioned or not self.args.skip_part:
            self.files = self._gunzip()

        # run partitioner if necessary
        if self.partOptions.partitioned and not self.args.skip_part:
            self._runPartitioner()

        # create table
        self._createTable()

        # load data
        self._loadData()

        # update CSS with info for this table
        self._updateCss()


    def _cleanup(self):
        """
        Do cleanup, remove all temporary files, this should not throw.
        """

        # remove dir with unzipped files
        if self.unzipDir is not None:
            try:
                shutil.rmtree(self.unzipDir)
            except Exception as exc:
                logging.error('Failed to remove unzipped files: %s', exc)

        # remove chunks directory, only if we created it
        if not self.args.keep_chunks and self.cleanupChunksDir:
            try:
                shutil.rmtree(self.args.chunks_dir)
            except Exception as exc:
                logging.error('Failed to remove chunks directory: %s', exc)


    def _checkCss(self):
        """
        Check CSS for existing configuration and see if it matches ours.
        Throws exception f any irregulatrities are observed.
        """

        # get database config
        dbConfig = self.css.getDbInfo(self.args.database)
        logging.debug('CSS database info: %s', dbConfig)
        if dbConfig is None:
            return

        # get partitioning ID
        partId = dbConfig.get('partitioningId')
        if partId is None:
            raise RuntimeError("CSS error: partitioningId is not defined for database " \
                               + self.args.database)

        # get partitioning config
        partConfig = self.css.getPartInfo(partId)
        logging.debug('CSS partitioning info: %s', partConfig)

        # check parameters
        self._checkPartParam(self.partOptions, 'part.num-stripes', partConfig, 'nStripes', int)
        self._checkPartParam(self.partOptions, 'part.num-sub-stripes', partConfig, 'nSubStripes', int)
#         self._checkPartParam(self.partOptions, 'part.overlap', partConfig, 'overlap', float)

        # also check that table does not exist in CSS, or optionally remove it
        cssTableExists = self.css.tableExists(self.args.database, self.args.table)
        if cssTableExists:
            if self.args.css_remove:
                # try to remove it
                self.css.dropTable(self.args.database, self.args.table)
            else:
                logging.error('Table is already defined in CSS')
                raise RuntimeError('table exists in CSS')

    @staticmethod
    def _checkPartParam(partOptions, partKey, cssOptions, cssKey, optType=str):
        """
        Check that partitioning parameters are compatible. Throws exception
        if there is a mismatch.
        """
        optValue = optType(partOptions[partKey])
        cssValue = optType(cssOptions[cssKey])
        if optValue != cssValue:
            raise ValueError('Option "%s" does not match CSS "%s": %s != %s' % \
                             (partKey, cssKey, optValue, cssValue))

    def _makeOrCheckChunksDir(self):
        '''create directory for chunk data or check that it exists, throws in case of errors'''

        chunks_dir = self.args.chunks_dir

        # if it exists it must be directory
        exists = False
        if os.path.exists(chunks_dir):
            exists = True
            if not os.path.isdir(chunks_dir):
                logging.error('Path for chunks exists but is not a directory: %s',
                              chunks_dir)
                raise RuntimeError('chunk path is not directory')

        if self.args.skip_part:
            # directory must exist and have some files (chunk_index.bin at least)
            if not exists:
                logging.error('Chunks directory does not exist: %s', chunks_dir)
                raise RuntimeError('chunk directory is missing')
            path = os.path.join(chunks_dir, 'chunk_index.bin')
            if not os.path.exists(path):
                logging.error('Could not find required file (chunk_index.bin) in chunks directory')
                raise RuntimeError('chunk_index.bin is missing')
        else:
            if exists:
                # must be empty, we do not want any extraneous stuff there
                if os.listdir(chunks_dir):
                    logging.error('Chunks directory is not empty: %s', chunks_dir)
                    raise RuntimeError('chunks directory is not empty')
            else:
                try:
                    os.makedirs(chunks_dir)
                except Exception as exc:
                    logging.error('Failed to create chunks directory: %s', exc)
                    raise
            self.cleanupChunksDir = True


    def _runPartitioner(self):
        '''run partitioner to fill chunks directory with data, returns 0 on success'''

        # build arguments list
        args = ['sph-partition', '--out.dir', self.args.chunks_dir, '--part.prefix', self.chunkPrefix]
        for config in self.args.part_config:
            args += ['--config-file', config]
        for data in self.files:
            args += ['--in', data]

        try:
            # run partitioner
            logging.debug('run partitioner: %s', ' '.join(args))
            output = subprocess.check_output(args=args)
        except Exception as exc:
            logging.error('Failed to run partitioner: %s', exc)
            raise


    def _gunzip(self):
        """
        Uncompress compressed input files to a temporary directory. 
        Returns list of input file names with compressed files replaced by
        uncompressed file location. Throws exception in case of errors.
        """

        result = []
        for infile in self.args.data:
            if infile.endswith('.gz'):

                if self.unzipDir is None:
                    # directory needs sufficient space, use output chunks directory for that
                    try:
                        self.unzipDir = tempfile.mkdtemp(dir=self.args.chunks_dir)
                    except Exception as exc:
                        logging.critical('Failed to create tempt directory for uncompressed files: %s', exc)
                        raise
                    logging.info('Created temporary directory %s', self.unzipDir)

                # construct output file name
                outfile = os.path.basename(infile)
                outfile = os.path.splitext(outfile)[0]
                outfile = os.path.join(self.unzipDir, outfile)

                logging.info('Uncompressing %s to %s', infile, outfile)
                try:
                    input = open(infile)
                    output = open(outfile, 'wb')
                    cmd = ['gzip', '-d', '-c']
                    subprocess.check_call(args=cmd, stdin=input, stdout=output)
                except Exception as exc:
                    logging.critical('Failed to uncompress data file: %s', exc)
                    raise

            result.append(outfile)

        return result


    def _dbConnect(self):
        """
        Connect to mysql server, returns connection or throws exception.
        """
        kws = dict(local_infile=1)
        if self.args.mysql_host:
            kws['host'] = self.args.mysql_host
        if self.args.user:
            kws['user'] = self.args.user
        if self.args.password:
            kws['passwd'] = self.args.password
        if self.args.mysql_port:
            kws['port'] = self.args.mysql_port
        if self.args.mysql_socket:
            kws['unix_socket'] = self.args.mysql_socket

        try:
            return mysql.Connection(**kws)
        except Exception as exc:
            logging.critical('Failed to connect to mysql database: %s', exc)
            raise


    def _createTable(self):
        """
        Create database and table if needed.
        """

        cursor = self.mysql.cursor()

        # read table schema
        try:
            self.schema = open(self.args.schema).read()
        except Exception as exc:
            logging.critical('Failed to read table schema file: %s', exc)
            raise

        # create table
        try:
            cursor.execute("USE %s" % self.args.database)
            logging.debug('Creating table')
            cursor.execute(self.schema)
        except Exception as exc:
            logging.critical('Failed to create mysql table: %s', exc)
            raise

        # finish with this session, otherwise ATER TABLE will fail
        del cursor

        # add/remove chunk columns for partitioned tables only
        if self.partOptions.partitioned:
            self._alterTable()


    def _alterTable(self):
        """
        Change table schema, drop _chunkId, _subChunkId, add chinkId, subChunkId
        """

        cursor = self.mysql.cursor()

        try:
            # get current column set
            q = "SHOW COLUMNS FROM %s" % self.args.table
            cursor.execute(q)
            rows = cursor.fetchall()
            columns = set(row[0] for row in rows)

            # delete rows
            toDelete = set(["_chunkId", "_subChunkId"]) & columns
            mods = ['DROP COLUMN %s' % col for col in toDelete]

            # create rows, want them in that order
            toAdd = ["chunkId", "subChunkId"]
            mods += ['ADD COLUMN %s INT(11) NOT NULL' % col for col in toAdd if col not in columns]

            q = 'ALTER TABLE %s ' % self.args.table
            q += ', '.join(mods)

            logging.debug('Alter table: %s', q)
            cursor.execute(q)
        except Exception as exc:
            logging.critical('Failed to alter mysql table: %s', exc)
            raise

    def _loadData(self):
        """
        Load data into existing table.
        """
        if self.partOptions.partitioned:
            self._loadChunkedData()
        else:
            self._loadNonChunkedData()


    def _chunkFiles(self):
        """
        Generator methof which returns list of all chunk files. For each chunk returns
        a triplet (path:string, chunkId:int, overlap:bool).
        """
        for dirpath, _, filenames in os.walk(self.args.chunks_dir, followlinks=True):
            for fileName in filenames:
                match = self.chunkRe.match(fileName)
                if match is not None:
                    path = os.path.join(dirpath, fileName)
                    chunkId = int(match.group('id'))
                    overlap = match.group('ov') is not None
                    yield (path, chunkId, overlap)


    def _loadChunkedData(self):
        """
        Load chunked data into mysql table, if one-table option is specified then all chunks
        are loaded into a single table with original name, otherwise we create one table per chunk.
        """

        # As we read from partitioner output files we use "out.csv" option for that.
        csvPrefix = "out.csv"

        for file, chunkId, overlap in self._chunkFiles():
            if self.args.one_table:
                # just load everything into existing table
                self._loadOneFile(self.args.table, file, csvPrefix)
            else:

                # make table if needed
                table = self._makeChunkTable(chunkId, overlap)

                # load data into chunk table
                self._loadOneFile(table, file, csvPrefix)


    def _makeChunkTable(self, chunkId, overlap):
        """ Create table foa chunk if it does not exist yet. Returns table name. """

        # build a table name
        table = self.args.table
        table += ['_', 'FullOverlap_'][overlap]
        table += str(chunkId)

        cursor = self.mysql.cursor()

        # make table using DDL from non-chunked one
        q = "CREATE TABLE IF NOT EXISTS {0} LIKE {1}".format(table, self.args.table)

        logging.debug('Make chunk table: %s', table)
        cursor.execute(q)

        return table


    def _loadNonChunkedData(self):
        """
        Load non-chunked data into mysql table. We use (unzipped) files that
        we got for input.
        """

        # As we read from input files (which are also input files for partitioner)
        # we use "in.csv" option for that.
        csvPrefix = "in.csv"

        for file in self.files:
            self._loadOneFile(self.args.table, file, csvPrefix)


    def _loadOneFile(self, table, file, csvPrefix):
        """Load data from a single file into existing table"""
        cursor = self.mysql.cursor()

        # need to know field separator, default is the same as in partitioner.
        separator = self.partOptions.get(csvPrefix + '.delimiter', '\t')

        logging.debug('load data: table=%s file=%s', table, file)
        q = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s'" % \
            (file, table, separator)
        try:
            cursor.execute(q)
        except Exception as exc:
            logging.critical('Failed to load data into non-partitioned table: %s', exc)
            raise


    def _updateCss(self):
        """
        Update CSS with information about loaded table and database
        """

        # create database in CSS if not there yet
        if not self.css.dbExists(self.args.database):
            logging.debug('Creating database CSS info')
            options = self.partOptions.cssDbOptions()
            self.css.createDb(self.args.database, options)

        # define options for table
        options = self.partOptions.cssTableOptions()
        options['schema'] = self._schemaForCSS()

        logging.debug('Creating table CSS info')
        self.css.createTable(self.args.database, self.args.table, options)


    def _schemaForCSS(self):
        """
        Returns schema string for CSS, which is a create table only without
        create table, only column definitions
        """

        cursor = self.mysql.cursor()

        # make table using DDL from non-chunked one
        q = "SHOW CREATE TABLE {0}".format(self.args.table)
        cursor.execute(q)
        data = cursor.fetchone()[1]

        # strip CREATE TABLE and all table options
        i = data.find('(')
        j = data.rfind(')')
        return data[i:j + 1]


if __name__ == "__main__":
    loader = Loader()
    try:
        sys.exit(loader.run())
    except Exception as exc:
        logging.critical('Exception occured: %s', exc, exc_info=True)
        sys.exit(1)
