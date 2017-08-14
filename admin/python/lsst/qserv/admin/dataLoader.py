# LSST Data Management System
# Copyright 2014-2016 AURA/LSST.
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
from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
from io import StringIO
import logging
import os
import re
import shutil
import subprocess
import tempfile

# -----------------------------
# Imports for other modules --
# -----------------------------
from lsst.qserv import css
from lsst.qserv.admin.partConfig import PartConfig
from lsst.qserv.admin.chunkMapping import ChunkMapping
from lsst.qserv.wmgr.client import ServerError

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------


def _mysql_identifier_validator(db_or_table_name):
    """
    Check database and table name to prevent SQL-injection
    see http://dev.mysql.com/doc/refman/5.0/en/identifiers.html
    other query parameters will be processed by MySQL-python:
    see http://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-execute.html

    @param db_or_table_name: a mysql database or table name
    @return True if name match MySQL requirements
    """
    if not hasattr(_mysql_identifier_validator, "name_validator"):
        name_validator = re.compile(r'^[0-9a-zA-Z_\$]+$')
    is_correct = name_validator.match(db_or_table_name) is not None
    return is_correct

# ------------------------
# Exported definitions --
# ------------------------


class DataLoader(object):
    """
    DataLoader class defines all logic for loading data, including data
    partitioning, CSS updating, etc. It is driven by a set of configuration
    files which are passed to constructor.
    """

    def __init__(self, configFiles, czarWmgr, workerWmgrMap={}, chunksDir="./loader_chunks",
                 chunkPrefix='chunk', keepChunks=False, skipPart=False, oneTable=False,
                 css=None, cssClear=False, indexDb='qservMeta', tmpDir=None,
                 emptyChunks=None, deleteTables=False, loggerName=None):
        """
        Constructor parses all arguments and prepares for execution.

        @param configFiles:  Sequence of the files defining all partitioning options.
        @param czarWmgr:     WmgrClient instance for czar node.
        @param workerWmgrMap: Dictionary mapping worker host name to corresponding
                             WmgrClient instance. May be empty, in which case czar
                             server will be used for all data.
        @param chunksDir:    Temporary directory to store chunks files, will be created
                             if does not exist.
        @param chunkPrefix:  File name prefix for generated chunk files.
        @param keepChunks:   Chunks will not be deleted if this argument is set to True.
        @param skipPart:     If set to True then partitioning will not be performed
                             (chunks should exist already).
        @param oneTable:     If set to True then load all data into one table, do not
                             create chunk tables.
        @param css:          Instance of CssAccess class, None if CSS operations are disabled.
        @param cssClear:     If true then CSS info for a table will be deleted first.
        @param indexDb:      Name of  database for object indices, index is generated for director
                             table when it is partitioned, use empty string to disable index.
        @param tmpDir:       Temporary directory to store uncompressed files. If None then directory
                             inside chunksDir will be used. Will be created if does not exist.
        @param emptyChunks:  Path name for "empty chunks" file, may be None.
        @param deleteTables: If True then existing tables in database will be deleted.
        @param loggerName:   Logger name used for logging all messages from loader.
        """

        if not loggerName:
            loggerName = __name__
        self._log = logging.getLogger(loggerName)

        self.configFiles = configFiles
        self.czarWmgr = czarWmgr
        self.workerWmgrMap = workerWmgrMap.copy()
        self.chunksDir = chunksDir
        self.tmpDir = tmpDir
        self.chunkPrefix = chunkPrefix
        self.keepChunks = keepChunks
        self.skipPart = skipPart
        self.oneTable = oneTable
        self.css = css
        self.cssClear = cssClear
        self.indexDb = None if oneTable else indexDb
        self.emptyChunks = emptyChunks
        self.deleteTables = deleteTables

        self.chunkRe = re.compile('^' + self.chunkPrefix + '_(?P<id>[0-9]+)(?P<ov>_overlap)?[.]txt$')
        self.cleanupDirs = []
        self.cleanupFiles = []
        self.unzipDir = None   # directory used for uncompressed data
        self.schema = None     # "CREATE TABLE" statement
        self.chunks = set()    # set of chunks that were loaded
        self.chunkMap = None
        self.createdChunks = set()

        # parse all config files, this can raise an exception
        self.partOptions = PartConfig(configFiles)

        # Logic is slightly complicated here, so pre-calculate options that we need below:
        # 1. If self.skipPart and self.oneTable are both true then we skip partitioning
        #    even for partitioned tables and load original data. So if self.skipPart and
        #    self.oneTable are both true then we say table is not partitioned
        # 2. Partitioning is done only for partitioned table, if self.skipPart is true then
        #    pre-partitioned data must exist already and we skip calling partitioner

        # is table partitioned (or pre-partitioned)?
        self.partitioned = self.partOptions.partitioned

        # do we need to run partitioner?
        self.callPartitioner = self.partitioned and not self.skipPart

    def load(self, database, table, schema, data):
        """
        Do actual loading based on parameters defined in constructor.
        This will throw exception if anything goes wrong.

        @param database:     Database name.
        @param table:        Table name.
        @param schema:       File name which contains SQL with CREATE TABLE/VIEW.
        @param data:         List of file names with data, may be empty (e.g. when
                             defining views instead of tables).
        """

        if not _mysql_identifier_validator(table):
            raise ValueError('MySQL table name not allowed: ' + table)
        if not _mysql_identifier_validator(database):
            raise ValueError('MySQL database name not allowed: ' + database)

        try:
            return self._run(database, table, schema, data)
        finally:
            self._cleanup()

    def _run(self, database, table, schema, data):
        """
        Do loading only, cleanup is done in _cleanup()
        """

        # see if database is already defined in CSS and get its partitioning info
        if self.css is not None:
            self._checkCss(database, table)

        # make chunk mapper
        self.chunkMap = ChunkMapping(list(self.workerWmgrMap.keys()), database, table, self.css)

        # make chunks directory or check that there are usable data there already
        self._makeOrCheckChunksDir(data)

        # uncompress data files that are compressed, this is only needed if
        # table is not partitioned or if we are not reusing existing chunks
        files = data
        if not (self.partitioned and self.skipPart and not self.oneTable):
            files = self._gunzip(data)

        # run partitioner if necessary
        if files and self.callPartitioner:
            self._runPartitioner(files)

        # drop existing tables
        if self.deleteTables:
            self._deleteTable(database, table)

        # create table
        self._createTable(database, table, schema)

        # load data
        self._loadData(database, table, files)

        # create special dummy chunk
        self._createDummyChunk(database, table)

        # create index on czar size
        self._makeIndex(database, table)

        # update CSS with info for this table
        if self.css is not None:
            self._updateCss(database, table)

        # optionally make emptyChunks file
        self._makeEmptyChunks()

    def _cleanup(self):
        """
        Do cleanup, remove all temporary files, this should not throw.
        """

        # remove tmp files
        for fName in self.cleanupFiles:
            try:
                self._log.debug('Deleting temporary file: %r', fName)
                os.unlink(fName)
            except Exception as exc:
                self._log.error('Failed to remove temporary file: %r', exc)

        # remove directories
        for dirName in self.cleanupDirs:
            try:
                self._log.debug('Deleting directory: %r', dirName)
                shutil.rmtree(dirName)
            except Exception as exc:
                self._log.error('Failed to remove directory: %r', exc)

    def _checkCss(self, database, table):
        """
        Check CSS for existing configuration and see if it matches ours.
        Throws exception if any irregularities are observed.
        """

        self._log.info('Verifying CSS info for table %r', table)

        # get striping info
        try:
            striping = self.css.getDbStriping(database)
            self._log.debug('CSS database striping info: %r', striping)
        except css.NoSuchDb:
            # we'll create it later
            return

        # check parameters
        self._checkPartParam(self.partOptions, 'part.num-stripes', striping.stripes, int)
        self._checkPartParam(self.partOptions, 'part.num-sub-stripes', striping.subStripes, int)
        self._checkPartParam(self.partOptions, 'part.default-overlap', striping.overlap, float)

        # also check that table does not exist in CSS, or optionally remove it
        cssTableExists = self.css.containsTable(database, table)
        if cssTableExists:
            if self.cssClear:
                # try to remove it
                self.css.dropTable(database, table)
            else:
                self._log.error('Table is already defined in CSS')
                raise RuntimeError('table exists in CSS')

    @staticmethod
    def _checkPartParam(partOptions, partKey, cssValue, optType=str):
        """
        Check that partitioning parameters are compatible. Throws exception
        if there is a mismatch.
        """
        optValue = optType(partOptions[partKey])
        if optValue != cssValue:
            raise ValueError('Option %r does not match CSS: %r != %r' %
                             (partKey, optValue, cssValue))

    def _makeOrCheckChunksDir(self, data):
        '''Create directory for chunk data or check that it exists, throws in case of errors.'''

        # only need it for partitioned table
        if not self.partitioned:
            return

        # in case we do skip-part but load into one table then we just take
        # data from command line if it is specified
        if self.oneTable and self.skipPart and data:
            return

        chunks_dir = self.chunksDir

        # if it exists it must be directory
        exists = False
        if os.path.exists(chunks_dir):
            exists = True
            if not os.path.isdir(chunks_dir):
                self._log.error('Path for chunks exists but is not a directory: %r', chunks_dir)
                raise IOError('chunk path is not directory: ' + chunks_dir)

        if self.skipPart:
            # directory must exist and have some files (chunk_index.bin at least)
            if not exists:
                self._log.error('Chunks directory does not exist: %r', chunks_dir)
                raise RuntimeError('chunk directory is missing')
            path = os.path.join(chunks_dir, 'chunk_index.bin')
            if not os.path.exists(path):
                self._log.error('Could not find required file (chunk_index.bin) in chunks directory')
                raise RuntimeError('chunk_index.bin is missing')
        else:
            if exists:
                # must be empty, we do not want any extraneous stuff there
                if os.listdir(chunks_dir):
                    self._log.error('Chunks directory is not empty: %r', chunks_dir)
                    raise RuntimeError('chunks directory is not empty: ' + chunks_dir)
            else:
                try:
                    self._log.debug('Creating chunks directory %r', chunks_dir)
                    os.makedirs(chunks_dir)
                    # will remove it later
                    if not self.keepChunks:
                        self.cleanupDirs.append(chunks_dir)
                except Exception as exc:
                    self._log.error('Failed to create chunks directory: %r', exc)
                    raise

    def _runPartitioner(self, files):
        '''Run partitioner to fill chunks directory with data, returns 0 on success.'''

        def fileList(dirName):
            '''Generate a sequence of file names in directory, exclude directories'''
            for fName in os.listdir(dirName):
                path = os.path.join(dirName, fName)
                if os.path.isfile(path):
                    yield path

        # build arguments list
        partitioner = 'sph-partition-matches' if self.partOptions.isRefMatch else 'sph-partition'
        args = [partitioner, '--out.dir', self.chunksDir, '--part.prefix', self.chunkPrefix]
        for config in self.configFiles:
            args += ['--config-file', config]
        for data in files:
            args += ['--in', data]

        try:
            # run partitioner
            self._log.info('run partitioner on files: %r', ' '.join(files))
            self._log.debug('Run shell command: %r', ' '.join(args))
            subprocess.check_output(args=args)
        except Exception as exc:
            self._log.error('Failed to run partitioner: %r', exc)
            raise
        finally:
            # some chunk files may have been created, add them to cleanup list
            if not self.keepChunks:
                self.cleanupFiles += list(fileList(self.chunksDir))

    def _gunzip(self, data):
        """
        Uncompress compressed input files to a temporary directory.
        Returns list of input file names with compressed files replaced by
        uncompressed file location. Throws exception in case of errors.
        """

        result = []
        for infile in data:

            # we rely on file extension to decide whether it is compressed or not,
            # for more reliable way we could use something like "magic" module
            if infile.endswith('.gz'):

                if self.tmpDir is None:

                    # use chunks directory for that
                    if os.path.exists(self.chunksDir):
                        if not os.path.isdir(self.chunksDir):
                            self._log.error('Path for chunks is not a directory: %r', self.chunksDir)
                            raise IOError('chunk path is not directory: ' + self.chunksDir)
                    else:
                        # create it, but don't forget to delete it later
                        self._log.debug('Creating chunks directory %r', self.chunksDir)
                        os.makedirs(self.chunksDir)
                        if not self.keepChunks:
                            self.cleanupDirs.append(self.chunksDir)

                    try:
                        self.tmpDir = tempfile.mkdtemp(dir=self.chunksDir)
                        # need to remove it later, before chunks dir
                        self.cleanupDirs.insert(0, self.tmpDir)
                    except Exception as exc:
                        self._log.critical('Failed to create temp directory for uncompressed files: %r', exc)
                        raise
                    self._log.debug('Created temporary directory %r', self.tmpDir)
                else:
                    # check and create if needed
                    if os.path.exists(self.tmpDir):
                        if not os.path.isdir(self.tmpDir):
                            self._log.critical('Temporary location is not a directory: %r', self.tmpDir)
                            raise IOError('Temporary location is not a directory: ' + self.tmpDir)
                    else:
                        try:
                            os.mkdir(self.tmpDir)
                            self._log.debug('Created temporary directory %r', self.tmpDir)
                            # need to remove it later
                            self.cleanupDirs.append(self.tmpDir)
                        except Exception as exc:
                            self._log.critical('Failed to create temp directory: %r', exc)
                            raise

                # construct output file name
                outfile = os.path.basename(infile)
                outfile = os.path.splitext(outfile)[0]
                outfile = os.path.join(self.tmpDir, outfile)

                # will cleanup it later
                self.cleanupFiles.append(outfile)

                self._log.info('Uncompressing %r to %r', infile, outfile)
                try:
                    finput = open(infile)
                    foutput = open(outfile, 'wb')
                    cmd = ['gzip', '-d', '-c']
                    subprocess.check_call(args=cmd, stdin=finput, stdout=foutput)
                except Exception as exc:
                    self._log.critical('Failed to uncompress data file: %r', exc)
                    raise

            else:

                # file is already uncompressed
                self._log.debug('Using input file which is not compressed: %r', infile)
                outfile = infile

            result.append(outfile)

        return result

    def _connections(self, useCzar, useWorkers):
        """
        Returns a list of wmgr "connections", for each conection there is a
        tuple (name, connection) where name is something like "czar" or
        "worker lsst-dbdev2". If czar connection is included then it
        is always first in the list.

        @param useCzar:     if True then include czar in the list
        @param useWorkers:  if True then include all workers in the list
        """
        res = []
        if useCzar:
            res += [("czar", self.czarWmgr)]
        if useWorkers:
            for worker, wmgr in self.workerWmgrMap.items():
                res += [('worker ' + worker, wmgr)]
        return res

    def _deleteTable(self, database, table):
        """
        Drop existing table and all chunks.
        """

        self._log.info('Deleting existing table %r (and chunks)', table)

        for name, wmgr in self._connections(useCzar=True, useWorkers=True):
            self._log.info('Deleting table from %r', name)
            wmgr.dropTable(database, table, dropChunks=True, mustExist=False)

    def _createTable(self, database, table, schema):
        """
        Create table in the database. Just executes whatever SQL was given to
        us in a schema file. Additionally applies fixes to a schema after loading.
        """

        # read table schema
        try:
            self.schema = open(schema).read()
        except Exception as exc:
            self._log.critical('Failed to read table schema file: %r', exc)
            raise

        # create table on czar and every worker
        for name, wmgr in self._connections(useCzar=True, useWorkers=True):
            self._log.info('Creating table %r in %r', table, name)
            chunkColumns = bool(self.partitioned)
            try:
                wmgr.createTable(database, table, schema=self.schema, chunkColumns=chunkColumns)
            except ServerError as exc:
                if exc.code == 409:
                    self._log.info('Table %r exists in %r', table, name)
                else:
                    self._log.critical('Failed to create table %r in %r', table, name)
                    raise

    def _loadData(self, database, table, files):
        """
        Load data into existing table.
        """
        if not self.partitioned or (self.oneTable and self.skipPart and files):
            # load files given on command line
            self._loadNonChunkedData(database, table, files)
        else:
            # load data from chunk directory
            self._loadChunkedData(database, table)

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

    def _loadChunkedData(self, database, table):
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

                # just load everything into existing table, do not load overlaps
                if not overlap:
                    self._loadOneFile(self.czarWmgr, database, table, path, csvPrefix)
                else:
                    self._log.info('Ignore overlap file %r', path)

            else:

                # Partitioner may potentially produce empty overlap files even
                # in cases when we should not make overlap tables. Check and
                # filter out empty files or complain about non-empty.
                if overlap and not self.partOptions.isSubChunked:
                    # check contents, try to read some data and strip spaces
                    data = open(path).read(1024).strip()
                    if data:
                        raise RuntimeError('Found non-empty overlap file for non-subchunked table: ' + path)
                    else:
                        self._log.info('Ignore empty overlap file %r', path)
                        continue

                if self.workerWmgrMap:
                    # find database for this chunk
                    worker = self.chunkMap.worker(chunkId)
                    wmgr = self.workerWmgrMap.get(worker)
                    if wmgr is None:
                        raise RuntimeError('Existing chunk mapping is not in the list of workers: ' + worker)
                    self._log.info('load chunk %r to worker %r', chunkId, worker)
                else:
                    # all goes to single node
                    self._log.info('load chunk %r to czar', chunkId)
                    wmgr = self.czarWmgr

                # make tables if needed
                if chunkId not in self.createdChunks:
                    try:
                        wmgr.createChunk(database, table, chunkId, overlap=self.partOptions.isSubChunked)
                        self.createdChunks.add(chunkId)
                    except ServerError as exc:
                        if exc.code == 409:
                            self._log.info('Chunk %r exists for table %r', chunkId, table)
                        else:
                            self._log.critical('Failed to create chunk %r for table %r', chunkId, table)
                            raise

                # load data into chunk table
                self._loadOneFile(wmgr, database, table, path, csvPrefix, chunkId=chunkId, overlap=overlap)

    @staticmethod
    def _chunkTableName(table, chunkId, overlap):
        """
        Return full chunk table name or overlap table name.
        """
        ctable = table
        if overlap:
            ctable += 'FullOverlap'
        ctable += '_'
        ctable += str(chunkId)
        return ctable

    def _createDummyChunk(self, database, table):
        """
        Make special dummy chunk in case of partitioned data.
        """

        if not self.partitioned or self.oneTable:
            # only do it for true partitioned stuff
            return

        # this is only needed on worker (or czar if there are no workers)
        connections = self._connections(useCzar=False, useWorkers=True)
        if not connections:
            connections = self._connections(useCzar=True, useWorkers=False)

        for name, wmgr in connections:

            self._log.info('Make dummy chunk table for %r', table)

            # just make regular chunk with special ID, do not load any data
            try:
                wmgr.createChunk(database, table, 1234567890, overlap=self.partOptions.isSubChunked)
            except ServerError as exc:
                if exc.code == 409:
                    self._log.info('Dummy chunk 1234567890 exists for table %r', table)
                else:
                    self._log.critical('Failed to create dummy chunk 1234567890 for table %r', table)
                    raise

    def _loadNonChunkedData(self, database, table, files):
        """
        Load non-chunked data into mysql table. We use (unzipped) files that
        we got for input.
        """

        # As we read from input files (which are also input files for partitioner)
        # we use "in.csv" option for that.
        csvPrefix = "in.csv"

        # this is only needed on workers (or czar if there are no workers)
        connections = self._connections(useCzar=False, useWorkers=True)
        if not connections:
            connections = self._connections(useCzar=True, useWorkers=False)

        for name, wmgr in connections:
            self._log.info('load non-chunked data to %r', name)
            for file in files:
                self._loadOneFile(wmgr, database, table, file, csvPrefix)

    def _loadOneFile(self, wmgr, database, table, path, csvPrefix, chunkId=None, overlap=None):
        """Load data from a single file into existing table"""

        self._log.info('load table %r.%r from file %r', database, table, path)

        # need to know special characters used in csv
        # default delimiter is the same as in partitioner
        special_chars = {'delimiter': '\t',
                         'enclose': '',
                         'escape': '\\',
                         'newline': '\n'}

        data = {}
        for name, default in special_chars.items():
            data[name] = self.partOptions.get(csvPrefix + '.' + name, default)

        try:
            file = open(path)
        except IOError as exc:
            self._log.error('failed to open file %r', path)
            raise

        wmgr.loadData(database, table, file, fileName=path, chunkId=chunkId, overlap=overlap,
                      delimiter=data['delimiter'], enclose=data['enclose'],
                      escape=data['escape'], terminate=data['newline'])

    def _updateCss(self, database, table):
        """
        Update CSS with information about loaded table and database.
        """

        # create database in CSS if not there yet
        if not self.css.containsDb(database):
            self._log.info('Creating database CSS info')
            options = self.partOptions.cssDbOptions()
            striping = css.StripingParams(options['nStripes'], options['nSubStripes'],
                                          0, options['overlap'])
            self.css.createDb(database, striping, options['storageClass'], 'RELEASED')

        # define options for table
        options = self.partOptions.cssTableOptions()
        schema = self._schemaForCSS(database, table)

        if options.get('match', False):
            matchParams = css.MatchTableParams(options['dirTable1'], options['dirColName1'],
                                               options['dirTable2'], options['dirColName2'],
                                               options['flagColName'])
            self._log.info('Creating table CSS info for match table')
            self.css.createMatchTable(database, table, schema, matchParams)
        else:
            if 'dirTable' in options:
                # partitioned table
                pParams = css.PartTableParams(options['dirDb'], options['dirTable'], options['dirColName'],
                                              options['latColName'], options['lonColName'],
                                              options['overlap'], True, options['subChunks'])
                sParams = css.ScanTableParams(options['lockInMem'], options['scanRating'])
            else:
                pParams = css.PartTableParams()
                sParams = css.ScanTableParams()
            self.css.createTable(database, table, schema, pParams, sParams)

        # save chunk mapping too
        self._log.info('Saving updated chunk map to CSS')
        self.chunkMap.save()

    def _schemaForCSS(self, database, table):
        """
        Returns schema string for CSS, which is a create table only without
        create table, only column definitions
        """

        schema = self.czarWmgr.tableSchema(database, table)
        # strip CREATE TABLE
        i = schema.find('(')
        return schema[i:].encode('utf_8')

    def _makeEmptyChunks(self):
        """
        Generate empty chunks file, should be called after loading is complete.
        """

        if not self.emptyChunks:
            # need a file name
            return

        # only makes sense for true partitioned tables
        if not self.partitioned:
            self._log.info('Table is not partitioned, will not make empty chunks file %r', self.emptyChunks)
            return

        # max possible number of chunks
        nStripes = int(self.partOptions['part.num-stripes'])
        maxChunks = 2 * nStripes ** 2

        self._log.info('Making empty chunk list (max.chunk=%d) %r', maxChunks, self.emptyChunks)

        emptyChunkDir = os.path.dirname(self.emptyChunks)
        try:
            os.makedirs(emptyChunkDir)
        except OSError:
            if not os.path.isdir(emptyChunkDir):
                raise

        out = open(self.emptyChunks, 'w')
        for chunk in range(maxChunks):
            if chunk not in self.chunks:
                print(chunk, file=out)

    def _makeIndex(self, database, table):
        """
        Generate object index in czar meta database.
        """

        # only makes sense for director table
        if not self.partitioned or \
           not self.partOptions.isDirector(database, table) or \
           not self.indexDb:
            return

        metaTable = database + '__' + table
        self._log.info('Generating index %r.%r', self.indexDb, metaTable)

        # try to delete existing table first
        # self.czarWmgr.dropTable(self.indexDb, metaTable, mustExist=False)

        # index column
        idxCol = self.partOptions['id']

        # get index column type from original table
        idxColType = 'BIGINT'
        for col in self.czarWmgr.tableColumns(database, table):
            if col['name'] == idxCol:
                idxColType = col['type']
                break

        # make a table, InnoDB engine is required for scalability
        schema = "CREATE TABLE IF NOT EXISTS {table} ({column} {type} NOT NULL PRIMARY KEY, chunkId INT, subChunkId INT)"
        schema += " ENGINE = INNODB"
        schema = schema.format(table=metaTable, column=idxCol, type=idxColType)
        self.czarWmgr.createTable(self.indexDb, metaTable, schema=schema)

        # call one of the two methods
        if self.workerWmgrMap:
            self._makeIndexMultiNode(database, table, metaTable, idxCol)
        else:
            self._makeIndexSingleNode(database, table, metaTable, idxCol)

    def _makeIndexMultiNode(self, database, table, metaTable, idxCol):
        """
        Generate object index in czar meta database in case chunks are on a separate
        server from index database. It needs to copy all index data over network,
        may need special optimization or parameters.
        """

        # load data from all chunks
        for chunk in self.chunks:

            # get worker name for this chunk
            wname = self.chunkMap.worker(chunk)
            wmgr = self.workerWmgrMap[wname]

            self._loadChunkIndex(wmgr, database, table, chunk, metaTable, idxCol)

    def _makeIndexSingleNode(self, database, table, metaTable, idxCol):
        """
        Generate object index in czar meta database in case all chunks are also on czar.
        """

        # TODO: there is for sure more efficient method than copying data locally

        # load data from all chunks
        for chunk in self.chunks:

            self._loadChunkIndex(self.czarWmgr, database, table, chunk, metaTable, idxCol)

    def _loadChunkIndex(self, wmgr, database, table, chunk, metaTable, idxCol):
        """
        Load secondary index with data from a single chunk.
        """

        # get index data from worker (or czar)
        columns = (idxCol, 'chunkId', 'subChunkId')
        indexData = wmgr.getIndex(database, table, chunkId=chunk, columns=columns)

        # dump it into a in-memory file
        data = StringIO()
        for row in indexData:
            data.write("%d\t%d\t%d\n" % tuple(row))
        data.seek(0)

        # send that file to czar
        self.czarWmgr.loadData(self.indexDb, metaTable, data)
