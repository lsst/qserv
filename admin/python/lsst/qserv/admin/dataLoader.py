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
Module defining DataLoader class and related methods.

DataLoader class is used to simplify data loading procedure.

@author  Andy Salnikov, SLAC
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import logging
import os
import re
import shutil
import subprocess
import tempfile

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.qserv.admin.partConfig import PartConfig
from lsst.qserv.admin.qservAdmin import QservAdmin

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

class DataLoader(object):
    """
    DataLoader class defines all loginc for loading data, including data
    partitioning, CSS updating, etc. It is driven by a set of configuration
    files which are passed to constructor.
    """

    def __init__(self, configFiles, mysqlConn, chunksDir="./loader_chunks",
                 chunkPrefix='chunk', keepChunks=False, skipPart=False, oneTable=False,
                 cssConn='localhost:12181', cssClear=False, loggerName="Loader"):
        """
        Constructor parse all arguments and prepares for execution.

        @param configFiles:  Sequence of the files defininig all partitioning options.
        @param mysqlConn:    Mysql connection object.
        @param chunksDir:    Temporary directory to store chunks files, will be created
                             if does not exist.
        @param chunkPrefix:  File name prefix for generated chunk files.
        @param keepChunks:   Chunks will not be deleted if this argument is set to True.
        @param skipPart:     If set to True then partitioning will not be performed
                             (chunks should exist already).
        @param oneTable:     If set to True then load all data into one table, do not
                             create chunk tables.
        @param cssConn:      Connection string for CSS service.
        @param cssClear:     If true then CSS info for a table will be deleted first.
        @param loggerName:   Logger name used for logging all messaged from loader.
        """

        self.configFiles = configFiles
        self.mysql = mysqlConn
        self.chunksDir = chunksDir
        self.chunkPrefix = chunkPrefix
        self.keepChunks = keepChunks
        self.skipPart = skipPart
        self.oneTable = oneTable
        self.cssConn = cssConn
        self.cssClear = cssClear

        self.chunkRe = re.compile('^' + self.chunkPrefix + '_(?P<id>[0-9]+)(?P<ov>_overlap)?[.]txt$')
        self.cleanupChunksDir = False
        self.unzipDir = None   # directory used for uncompressed data
        self.schema = None     # "CREATE TABLE" statement
        self.chunks = set()    # set of chunks that were loaded

        # parse all config files, this can raise an exception
        self.partOptions = PartConfig(configFiles)

        # connect to CSS
        self.css = QservAdmin(self.cssConn)

        self._log = logging.getLogger(loggerName)


    def load(self, database, table, schema, data):
        """
        Do actual loading based on parameters defined in constructor.
        This will throw exception if anyhting goes wrong.

        @param database:     Database name.
        @param table:        Table name.
        @param schema:       File name which contains SQL with CREATE TABLE/VIEW.
        @param data:         List of file names with data, may be empty (e.g. when
                             defining views instead of tables).
        """

        try:
            return self._run(database, table, schema, data)
        finally:
            self._cleanup()


    def _run(self, database, table, schema, data):
        """
        Do loading only, cleanup is done in _cleanup()
        """

        # see if database is already defined in CSS and get its partitioning info
        self._checkCss(database, table)

        # make chunks directory or check that there are usable data there already
        self._makeOrCheckChunksDir()

        # uncompress data files that are compressed, this is only needed if
        # table is not partitioned or if we are not reusing existing chunks
        files = data
        if not self.partOptions.partitioned or not self.skipPart:
            files = self._gunzip(data)

        # run partitioner if necessary
        if files and self.partOptions.partitioned and not self.skipPart:
            self._runPartitioner(files)

        # create table
        self._createTable(database, table, schema)

        # load data
        self._loadData(table, files)

        # create special dummy chunk
        self._createDummyChunk(database, table)

        # update CSS with info for this table
        self._updateCss(database, table)


    def _cleanup(self):
        """
        Do cleanup, remove all temporary files, this should not throw.
        """

        # remove dir with unzipped files
        if self.unzipDir is not None:
            try:
                shutil.rmtree(self.unzipDir)
            except Exception as exc:
                self._log.error('Failed to remove unzipped files: %s', exc)

        # remove chunks directory, only if we created it
        if not self.keepChunks and self.cleanupChunksDir:
            try:
                shutil.rmtree(self.chunksDir)
            except Exception as exc:
                self._log.error('Failed to remove chunks directory: %s', exc)


    def _checkCss(self, database, table):
        """
        Check CSS for existing configuration and see if it matches ours.
        Throws exception f any irregulatrities are observed.
        """

        # get database config
        dbConfig = self.css.getDbInfo(database)
        self._log.debug('CSS database info: %s', dbConfig)
        if dbConfig is None:
            return

        # get partitioning ID
        partId = dbConfig.get('partitioningId')
        if partId is None:
            raise RuntimeError("CSS error: partitioningId is not defined for database " \
                               + database)

        # get partitioning config
        partConfig = self.css.getPartInfo(partId)
        self._log.debug('CSS partitioning info: %s', partConfig)

        # check parameters
        self._checkPartParam(self.partOptions, 'part.num-stripes', partConfig, 'nStripes', int)
        self._checkPartParam(self.partOptions, 'part.num-sub-stripes', partConfig, 'nSubStripes', int)
#         self._checkPartParam(self.partOptions, 'part.overlap', partConfig, 'overlap', float)

        # also check that table does not exist in CSS, or optionally remove it
        cssTableExists = self.css.tableExists(database, table)
        if cssTableExists:
            if self.cssClear:
                # try to remove it
                self.css.dropTable(database, table)
            else:
                self._log.error('Table is already defined in CSS')
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

        chunks_dir = self.chunksDir

        # if it exists it must be directory
        exists = False
        if os.path.exists(chunks_dir):
            exists = True
            if not os.path.isdir(chunks_dir):
                self._log.error('Path for chunks exists but is not a directory: %s',
                              chunks_dir)
                raise RuntimeError('chunk path is not directory')

        if self.skipPart:
            # directory must exist and have some files (chunk_index.bin at least)
            if not exists:
                self._log.error('Chunks directory does not exist: %s', chunks_dir)
                raise RuntimeError('chunk directory is missing')
            path = os.path.join(chunks_dir, 'chunk_index.bin')
            if not os.path.exists(path):
                self._log.error('Could not find required file (chunk_index.bin) in chunks directory')
                raise RuntimeError('chunk_index.bin is missing')
        else:
            if exists:
                # must be empty, we do not want any extraneous stuff there
                if os.listdir(chunks_dir):
                    self._log.error('Chunks directory is not empty: %s', chunks_dir)
                    raise RuntimeError('chunks directory is not empty')
            else:
                try:
                    os.makedirs(chunks_dir)
                except Exception as exc:
                    self._log.error('Failed to create chunks directory: %s', exc)
                    raise
            self.cleanupChunksDir = True


    def _runPartitioner(self, files):
        '''run partitioner to fill chunks directory with data, returns 0 on success'''

        # build arguments list
        args = ['sph-partition', '--out.dir', self.chunksDir, '--part.prefix', self.chunkPrefix]
        for config in self.configFiles:
            args += ['--config-file', config]
        for data in files:
            args += ['--in', data]

        try:
            # run partitioner
            self._log.debug('run partitioner: %s', ' '.join(args))
            subprocess.check_output(args=args)
        except Exception as exc:
            self._log.error('Failed to run partitioner: %s', exc)
            raise


    def _gunzip(self, data):
        """
        Uncompress compressed input files to a temporary directory.
        Returns list of input file names with compressed files replaced by
        uncompressed file location. Throws exception in case of errors.
        """

        result = []
        for infile in data:
            if infile.endswith('.gz'):

                if self.unzipDir is None:
                    # directory needs sufficient space, use output chunks directory for that
                    try:
                        self.unzipDir = tempfile.mkdtemp(dir=self.chunksDir)
                    except Exception as exc:
                        self._log.critical('Failed to create tempt directory for uncompressed files: %s', exc)
                        raise
                    self._log.info('Created temporary directory %s', self.unzipDir)

                # construct output file name
                outfile = os.path.basename(infile)
                outfile = os.path.splitext(outfile)[0]
                outfile = os.path.join(self.unzipDir, outfile)

                self._log.info('Uncompressing %s to %s', infile, outfile)
                try:
                    input = open(infile)
                    output = open(outfile, 'wb')
                    cmd = ['gzip', '-d', '-c']
                    subprocess.check_call(args=cmd, stdin=input, stdout=output)
                except Exception as exc:
                    self._log.critical('Failed to uncompress data file: %s', exc)
                    raise

            result.append(outfile)

        return result


    def _createTable(self, database, table, schema):
        """
        Create database and table if needed.
        """

        cursor = self.mysql.cursor()

        # read table schema
        try:
            self.schema = open(schema).read()
        except Exception as exc:
            self._log.critical('Failed to read table schema file: %s', exc)
            raise

        # create table
        try:
            cursor.execute("USE %s" % database)
            self._log.debug('Creating table')
            cursor.execute(self.schema)
        except Exception as exc:
            self._log.critical('Failed to create mysql table: %s', exc)
            raise

        # finish with this session, otherwise ALTER TABLE will fail
        del cursor

        # add/remove chunk columns for partitioned tables only
        if self.partOptions.partitioned:
            self._alterTable(table)


    def _alterTable(self, table):
        """
        Change table schema, drop _chunkId, _subChunkId, add chunkId, subChunkId
        """

        cursor = self.mysql.cursor()

        try:
            # get current column set
            q = "SHOW COLUMNS FROM %s" % table
            cursor.execute(q)
            rows = cursor.fetchall()
            columns = set(row[0] for row in rows)

            # delete rows
            toDelete = set(["_chunkId", "_subChunkId"]) & columns
            mods = ['DROP COLUMN %s' % col for col in toDelete]

            # create rows, want them in that order
            toAdd = ["chunkId", "subChunkId"]
            mods += ['ADD COLUMN %s INT(11) NOT NULL' % col for col in toAdd if col not in columns]

            q = 'ALTER TABLE %s ' % table
            q += ', '.join(mods)

            self._log.debug('Alter table: %s', q)
            cursor.execute(q)
        except Exception as exc:
            self._log.critical('Failed to alter mysql table: %s', exc)
            raise

    def _loadData(self, table, files):
        """
        Load data into existing table.
        """
        if self.partOptions.partitioned:
            self._loadChunkedData(table)
        else:
            self._loadNonChunkedData(table, files)


    def _chunkFiles(self):
        """
        Generator method which returns list of all chunk files. For each chunk returns
        a triplet (path:string, chunkId:int, overlap:bool).
        """
        for dirpath, _, filenames in os.walk(self.chunksDir, followlinks=True):
            for fileName in filenames:
                match = self.chunkRe.match(fileName)
                if match is not None:
                    path = os.path.join(dirpath, fileName)
                    chunkId = int(match.group('id'))
                    overlap = match.group('ov') is not None
                    yield (path, chunkId, overlap)


    def _loadChunkedData(self, table):
        """
        Load chunked data into mysql table, if one-table option is specified then all chunks
        are loaded into a single table with original name, otherwise we create one table per chunk.
        """

        # As we read from partitioner output files we use "out.csv" option for that.
        csvPrefix = "out.csv"

        for path, chunkId, overlap in self._chunkFiles():

            # remember all chunks that we loaded
            if not overlap:
                self.chunks.add(chunkId)

            if self.oneTable:
                # just load everything into existing table
                self._loadOneFile(table, path, csvPrefix)
            else:

                # make table if needed
                ctable = self._makeChunkTable(table, chunkId, overlap)

                # load data into chunk table
                self._loadOneFile(ctable, path, csvPrefix)


    def _createDummyChunk(self, database, table):
        """
        Make special dummy chunk in case of partitioned data
        """

        if not self.partOptions.partitioned or self.oneTable:
            # only do it for true partitioned stuff
            return

        if not self.partOptions.isView:
            # just make regular chunk with special ID, do not load any data
            self._makeChunkTable(table, 1234567890, False)
        else:
            # TODO: table is a actually a view, need somethig special. Old loader was
            # creating new view just by renaming Table to Table_1234567890, I'm not sure
            # this is a correct procedure. In any case here is the code that does it

            cursor = self.mysql.cursor()
            q = "RENAME TABLE {0}.{1} to {0}.{1}_1234567890".format(database, table)

            self._log.debug('Rename view: %s', q)
            cursor.execute(q)


    def _makeChunkTable(self, table, chunkId, overlap):
        """ Create table for a chunk if it does not exist yet. Returns table name. """

        # build a table name
        ctable = table
        ctable += ['_', 'FullOverlap_'][overlap]
        ctable += str(chunkId)

        cursor = self.mysql.cursor()

        # make table using DDL from non-chunked one
        q = "CREATE TABLE IF NOT EXISTS {0} LIKE {1}".format(ctable, table)

        self._log.debug('Make chunk table: %s', ctable)
        cursor.execute(q)

        return ctable


    def _loadNonChunkedData(self, table, files):
        """
        Load non-chunked data into mysql table. We use (unzipped) files that
        we got for input.
        """

        # As we read from input files (which are also input files for partitioner)
        # we use "in.csv" option for that.
        csvPrefix = "in.csv"

        for file in files:
            self._loadOneFile(table, file, csvPrefix)


    def _loadOneFile(self, table, file, csvPrefix):
        """Load data from a single file into existing table"""
        cursor = self.mysql.cursor()

        # need to know field separator, default is the same as in partitioner.
        separator = self.partOptions.get(csvPrefix + '.delimiter', '\t')

        self._log.debug('load data: table=%s file=%s', table, file)
        q = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s'" % \
            (file, table, separator)
        try:
            cursor.execute(q)
        except Exception as exc:
            self._log.critical('Failed to load data into non-partitioned table: %s', exc)
            raise


    def _updateCss(self, database, table):
        """
        Update CSS with information about loaded table and database
        """

        # create database in CSS if not there yet
        if not self.css.dbExists(database):
            self._log.debug('Creating database CSS info')
            options = self.partOptions.cssDbOptions()
            self.css.createDb(database, options)

        # define options for table
        options = self.partOptions.cssTableOptions()
        options['schema'] = self._schemaForCSS(table)

        self._log.debug('Creating table CSS info')
        self.css.createTable(database, table, options)


    def _schemaForCSS(self, table):
        """
        Returns schema string for CSS, which is a create table only without
        create table, only column definitions
        """

        cursor = self.mysql.cursor()

        # make table using DDL from non-chunked one
        q = "SHOW CREATE TABLE {0}".format(table)
        cursor.execute(q)
        data = cursor.fetchone()[1]

        # strip CREATE TABLE and all table options
        i = data.find('(')
        j = data.rfind(')')
        return data[i:j + 1]
