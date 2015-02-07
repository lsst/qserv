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
from lsst.qserv.admin.chunkMapping import ChunkMapping

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

    def __init__(self, configFiles, mysqlConn, workerConnMap={}, chunksDir="./loader_chunks",
                 chunkPrefix='chunk', keepChunks=False, skipPart=False, oneTable=False,
                 qservAdmin=None, cssClear=False, indexDb='qservMeta',
                 emptyChunks=None, deleteTables=False, loggerName=None):
        """
        Constructor parses all arguments and prepares for execution.

        @param configFiles:  Sequence of the files defininig all partitioning options.
        @param mysqlConn:    Mysql connection object for czar-side database.
        @param workerConnMap: Dictionary mapping worker host name to corresponding
                             mysql connection object. May be empty, in which case czar
                             server will be used for all data.
        @param chunksDir:    Temporary directory to store chunks files, will be created
                             if does not exist.
        @param chunkPrefix:  File name prefix for generated chunk files.
        @param keepChunks:   Chunks will not be deleted if this argument is set to True.
        @param skipPart:     If set to True then partitioning will not be performed
                             (chunks should exist already).
        @param oneTable:     If set to True then load all data into one table, do not
                             create chunk tables.
        @param qservAdmin:   Instance of QservAdmin class, None if CSS operations are disabled.
        @param cssClear:     If true then CSS info for a table will be deleted first.
        @param indexDb:      Name of  database for object indices, index is generated for director
                             table when it is partitioned, use empty string to disable index.
        @param emptyChunks:  Path name for "empty chunks" file, may be None.
        @param deleteTables: If True then existing tables in database will be deleted.
        @param loggerName:   Logger name used for logging all messages from loader.
        """

        if not loggerName:
            loggerName = __name__
        self._log = logging.getLogger(loggerName)

        self.configFiles = configFiles
        self.mysql = mysqlConn
        self.workerConnMap = workerConnMap.copy()
        self.chunksDir = chunksDir
        self.chunkPrefix = chunkPrefix
        self.keepChunks = keepChunks
        self.skipPart = skipPart
        self.oneTable = oneTable
        self.css = qservAdmin
        self.cssClear = cssClear
        self.indexDb = indexDb
        self.emptyChunks = emptyChunks
        self.deleteTables = deleteTables

        self.chunkRe = re.compile('^' + self.chunkPrefix + '_(?P<id>[0-9]+)(?P<ov>_overlap)?[.]txt$')
        self.cleanupChunksDir = False
        self.unzipDir = None   # directory used for uncompressed data
        self.schema = None     # "CREATE TABLE" statement
        self.chunks = set()    # set of chunks that were loaded
        self.chunkMap = None

        # parse all config files, this can raise an exception
        self.partOptions = PartConfig(configFiles)

        # Logic is slightly complicated here, so pre-calculate options that we need below:
        # 1. If self.skipPart and self.oneTable are both true then we skip partitioning
        #    even for partitioned tables and load original data. So if self.skipPart and
        #    self.oneTable are both true then we say table is not partitioned
        # 2. Partitioning is done only for partitioned table, if self.skipPart is true then
        #    pre-partitioned data must exist already and we skip calling partitioner

        # is table needs to be partitioned (or is it pre-partitioned)?
        self.partitioned = self.partOptions.partitioned and \
                           not (self.skipPart and self.oneTable)

        # do we need to run partitioner?
        self.callPartitioner = self.partitioned and not self.skipPart

        # do we ever need input files? They are needed as input for partitioner or loader
        # if table is not partitioned
        self.needInput = not self.partitioned or not self.skipPart


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
        self.chunkMap = ChunkMapping(self.workerConnMap.keys(), database, table, self.css)

        # make chunks directory or check that there are usable data there already
        self._makeOrCheckChunksDir()

        # uncompress data files that are compressed, this is only needed if
        # table is not partitioned or if we are not reusing existing chunks
        files = data
        if self.needInput:
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

        # remove dir with unzipped files
        if self.unzipDir is not None:
            try:
                self._log.debug('Deleting directory: %s', self.unzipDir)
                shutil.rmtree(self.unzipDir)
            except Exception as exc:
                self._log.error('Failed to remove unzipped files: %s', exc)

        # remove chunks directory, only if we created it
        if not self.keepChunks and self.cleanupChunksDir:
            try:
                self._log.debug('Deleting directory: %s', self.chunksDir)
                shutil.rmtree(self.chunksDir)
            except Exception as exc:
                self._log.error('Failed to remove chunks directory: %s', exc)


    def _checkCss(self, database, table):
        """
        Check CSS for existing configuration and see if it matches ours.
        Throws exception f any irregulatrities are observed.
        """

        self._log.info('Verifying CSS info for table %s', table)

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
        self._checkPartParam(self.partOptions, 'part.default-overlap', partConfig, 'overlap', float)

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

        if self.partitioned and self.skipPart:
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
                    self._log.debug('Creating chunks directory %s', chunks_dir)
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
            self._log.info('run partitioner on files: %s', ' '.join(files))
            self._log.debug('Run shell command: %s', ' '.join(args))
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
                    self._log.debug('Created temporary directory %s', self.unzipDir)

                # construct output file name
                outfile = os.path.basename(infile)
                outfile = os.path.splitext(outfile)[0]
                outfile = os.path.join(self.unzipDir, outfile)

                self._log.info('Uncompressing %s to %s', infile, outfile)
                try:
                    finput = open(infile)
                    foutput = open(outfile, 'wb')
                    cmd = ['gzip', '-d', '-c']
                    subprocess.check_call(args=cmd, stdin=finput, stdout=foutput)
                except Exception as exc:
                    self._log.critical('Failed to uncompress data file: %s', exc)
                    raise

            result.append(outfile)

        return result


    def _connections(self, useCzar, useWorkers):
        """
        Returns a list of connections, for each conection there is a
        tuple (name, connection) where name is something like "czar" or
        "worker lsst-dbdev2". If czar connection is included then it
        is always first in the list.

        @param useCzar:     if True then include czar in the list
        @param useWorkers:  if True then include all workers in the list
        """
        res = []
        if useCzar:
            res += [("czar", self.mysql)]
        if useWorkers:
            for worker, conn in self.workerConnMap.items():
                res += [('worker ' + worker, conn)]
        return res


    def _deleteTable(self, database, table):
        """
        Drop existing table and all chunks
        """

        self._log.info('Deleting existing table %s (and chunks)', table)

        # regexp matching all chunk table names
        tblre = re.compile('^' + table + '((FullOverlap)?_[0-9]+)?$')

        for name, conn in self._connections(useCzar=True, useWorkers=True):

            self._log.info('Deleting table from %s', name)

            cursor = conn.cursor()

            q = 'SHOW TABLES FROM ' + database
            cursor.execute(q)
            tables = [row[0] for row in cursor.fetchall()]
            for tbl in tables:
                if tblre.match(tbl):
                    q = 'DROP TABLE IF EXISTS {0}.{1}'.format(database, tbl)
                    self._log.debug('query: %s', q)
                    cursor.execute(q)

            cursor.close()

    def _createTable(self, database, table, schema):
        """
        Create table in the database. Just executes whatever SQL was given to
        us in a schema file. Additionally applies fixes to a schema after loading.
        """

        # read table schema
        try:
            self.schema = open(schema).read()
        except Exception as exc:
            self._log.critical('Failed to read table schema file: %s', exc)
            raise

        # create table on czar and every worker
        for name, conn in self._connections(useCzar=True, useWorkers=True):

            cursor = conn.cursor()

            # create table
            try:
                # have to use "USE db" here
                q = "USE %s" % database
                self._log.debug('query: %s', q)
                cursor.execute(q)
                self._log.info('Creating table %s in %s', table, name)
                self._log.debug('query: %s', self.schema)
                cursor.execute(self.schema)
            except Exception as exc:
                self._log.critical('Failed to create mysql table: %s', exc)
                raise

            # finish with this session, otherwise ALTER TABLE will fail
            del cursor

            # add/remove chunk columns for partitioned tables only
            if self.partitioned:
                self._alterTable(table, conn)


    def _alterTable(self, table, conn):
        """
        Change table schema, drop _chunkId, _subChunkId, add chunkId, subChunkId
        """

        cursor = conn.cursor()

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

            if mods:
                self._log.info('Altering schema for table %s', table)

                q = 'ALTER TABLE %s ' % table
                q += ', '.join(mods)

                self._log.debug('query: %s', q)
                cursor.execute(q)
        except Exception as exc:
            self._log.critical('Failed to alter mysql table: %s', exc)
            raise

    def _loadData(self, database, table, files):
        """
        Load data into existing table.
        """
        if self.partitioned:
            self._loadChunkedData(database, table)
        else:
            self._loadNonChunkedData(database, table, files)


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

                # just load everything into existing table
                self._loadOneFile(self.mysql, database, table, path, csvPrefix)

            else:

                if self.workerConnMap:
                    # find database for this chunk
                    worker = self.chunkMap.worker(chunkId)
                    conn = self.workerConnMap.get(worker)
                    if conn is None:
                        raise RuntimeError('Existing chunk mapping is not in the list of workers: %s',
                                           worker)
                    self._log.info('load chunk %s to worker %s', chunkId, worker)
                else:
                    # all goes to single node
                    self._log.info('load chunk %s to czar', chunkId)
                    conn = self.mysql

                # make table if needed
                ctable = self._makeChunkTable(conn, database, table, chunkId, overlap)

                # load data into chunk table
                self._loadOneFile(conn, database, ctable, path, csvPrefix)


    def _createDummyChunk(self, database, table):
        """
        Make special dummy chunk in case of partitioned data
        """

        if not self.partitioned or self.oneTable:
            # only do it for true partitioned stuff
            return

        # this is only needed on worker (or czar if there are no workers)
        connections = self._connections(useCzar=False, useWorkers=True)
        if not connections:
            connections = self._connections(useCzar=True, useWorkers=False)

        for name, conn in connections:

            self._log.info('Make dummy chunk table for %s', name)

            if not self.partOptions.isView:
                # just make regular chunk with special ID, do not load any data
                self._makeChunkTable(conn, database, table, 1234567890, False)
            else:
                # TODO: table is a actually a view, need somethig special. Old loader was
                # creating new view just by renaming Table to Table_1234567890, I'm not sure
                # this is a correct procedure. In any case here is the code that does it

                cursor = conn.cursor()
                q = "RENAME TABLE {0}.{1} to {0}.{1}_1234567890".format(database, table)

                self._log.debug('Rename view')
                self._log.debug('query: %s', q)
                cursor.execute(q)


    def _makeChunkTable(self, conn, database, table, chunkId, overlap):
        """ Create table for a chunk if it does not exist yet. Returns table name. """

        # build a table name
        ctable = table
        ctable += ['_', 'FullOverlap_'][overlap]
        ctable += str(chunkId)

        cursor = conn.cursor()

        # make table using DDL from non-chunked one
        q = "CREATE TABLE IF NOT EXISTS {2}.{0} LIKE {2}.{1}".format(ctable, table, database)

        self._log.info('Make chunk table: %s', ctable)
        self._log.debug('query: %s', q)
        cursor.execute(q)

        return ctable


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

        for name, conn in connections:
            self._log.info('load non-chunked data to %s', name)
            for file in files:
                self._loadOneFile(conn, database, table, file, csvPrefix)


    def _loadOneFile(self, conn, database, table, file, csvPrefix):
        """Load data from a single file into existing table"""
        cursor = conn.cursor()

        # need to know field separator, default is the same as in partitioner.
        separator = self.partOptions.get(csvPrefix + '.delimiter', '\t')
        enclose = self.partOptions.get(csvPrefix + '.enclose', '"')
        escape = self.partOptions.get(csvPrefix + '.escape', '\\')
        escape = self.partOptions.get(csvPrefix + '.newline', '\n')


        self._log.info('load table %s from file %s', table, file)
        q = r"LOAD DATA LOCAL INFILE '%s' INTO TABLE %s.%s FIELDS TERMINATED BY '%s' " % \
            (file, database, table, separator)
        q += r"ENCLOSED BY '%s' ESCAPED BY '%s' LINES TERMINATED BY '%s'" % (enclose,
                                                                            escape,
                                                                            newline)
        self._log.debug('query: %s', q)
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
            self._log.info('Creating database CSS info')
            options = self.partOptions.cssDbOptions()
            self.css.createDb(database, options)

        # define options for table
        options = self.partOptions.cssTableOptions()
        options['schema'] = self._schemaForCSS(database, table)

        self._log.info('Creating table CSS info')
        self.css.createTable(database, table, options)

        # save chunk mapping too
        self._log.info('Saving updated chunk map to CSS')
        self.chunkMap.save()

    def _schemaForCSS(self, database, table):
        """
        Returns schema string for CSS, which is a create table only without
        create table, only column definitions
        """

        cursor = self.mysql.cursor()

        # make table using DDL from non-chunked one
        q = "SHOW CREATE TABLE {0}.{1}".format(database, table)
        cursor.execute(q)
        data = cursor.fetchone()[1]

        # strip CREATE TABLE and all table options
        i = data.find('(')
        j = data.rfind(')')
        return data[i:j + 1]


    def _makeEmptyChunks(self):
        """
        Generate empty chunks file, should be called after loading is complete.
        """

        if not self.emptyChunks:
            # need a file name
            return

        # only makes sense for true partitioned tables
        if not self.partitioned:
            self._log.info('Table is not partitioned, will not make empty chunks file %s', self.emptyChunks)
            return

        # max possible number of chunks
        nStripes = int(self.partOptions['part.num-stripes'])
        maxChunks = 2 * nStripes ** 2

        self._log.info('Making empty chunk list (max.chunk=%d) %s', maxChunks, self.emptyChunks)

        emptyChunkDir = os.path.dirname(self.emptyChunks)
        try:
            os.makedirs(emptyChunkDir)
        except OSError:
            if not os.path.isdir(emptyChunkDir):
                raise

        out = open(self.emptyChunks, 'w')
        for chunk in range(maxChunks):
            if chunk not in self.chunks:
                print >> out, chunk


    def _makeIndex(self, database, table):
        """
        Generate object index in czar meta database.
        """

        # only makes sense for director table
        if not self.partitioned or not self.partOptions.isDirector(table) or not self.indexDb:
            return

        metaTable = self.indexDb + '.' + database + '__' + table
        self._log.info('Generating index %s', metaTable)

        czarCursor = self.mysql.cursor()

        if self.deleteTables:
            q = "DROP TABLE IF EXISTS {0}".format(metaTable)
            self._log.debug('query: %s', q)
            czarCursor.execute(q)

        # index column
        idxCol = self.partOptions['id']

        # get index column type from original table
        idxColType = 'BIGINT'
        q = "SHOW COLUMNS FROM {0}.{1}".format(database, table)
        self._log.debug('query: %s', q)
        czarCursor.execute(q)
        for row in czarCursor.fetchall():
            if row[0] == idxCol:
                idxColType = row[1]
                break

        # make a table
        q = "CREATE TABLE {table} ({column} {type} NOT NULL PRIMARY KEY, chunkId INT, subChunkId INT)"
        q = q.format(table=metaTable, column=idxCol, type=idxColType)
        self._log.debug('query: %s', q)
        czarCursor.execute(q)

        czarCursor.close()

        # call one of the two methods
        if self.workerConnMap:
            self._makeIndexMultiNode(database, table, metaTable, idxCol)
        else:
            self._makeIndexSingleNode(database, table, metaTable, idxCol)


    def _makeIndexMultiNode(self, database, table, metaTable, idxCol):
        """
        Generate object index in czar meta database in case chunks are on a separate
        server from index database. It needs to copy all index data over network,
        may need special optimization or parameters.
        """

        czarCursor = self.mysql.cursor()

        # load data from all chunks
        for chunk in self.chunks:

            # get worker name for this chunk
            wname = self.chunkMap.worker(chunk)

            conn = self.workerConnMap[wname]
            wCursor = conn.cursor()

            q = "SELECT {0}, chunkId, subChunkId FROM {1}.{2}_{3}"
            q = q.format(idxCol, database, table, chunk)
            self._log.debug('query: %s', q)
            wCursor.execute(q)

            q = "INSERT INTO {0} VALUES (%s, %s, %s)".format(metaTable)
            while True:
                seq = wCursor.fetchmany(1000000)
                if not seq:
                    break
                czarCursor.executemany(q, seq)

            wCursor.close()

        czarCursor.close()

    def _makeIndexSingleNode(self, database, table, metaTable, idxCol):
        """
        Generate object index in czar meta database in case all chunks are also on czar.
        """

        cursor = self.mysql.cursor()

        # load data from all chunks
        for chunk in self.chunks:
            q = "INSERT INTO {0} SELECT {1}, chunkId, subChunkId FROM {2}.{3}_{4}"
            q = q.format(metaTable, idxCol, database, table, chunk)
            self._log.debug('query: %s', q)
            cursor.execute(q)

        cursor.close()
