#
# LSST Data Management System
# Copyright 2015 AURA/LSST.
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

"""
Module defining Flask blueprint for database management.

@author Andy Salnikov - salnikov@slac.stanford.edu
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
from contextlib import closing, contextmanager
import gzip
import logging
import os
import re
import shutil
import tempfile
from threading import Thread

#-----------------------------
# Imports for other modules --
#-----------------------------
from .config import Config
from .errors import errorResponse, ExceptionResponse
from flask import Blueprint, json, request, url_for
from lsst.db import db
from lsst.qserv.admin.qservAdminException import QservAdminException
import MySQLdb
from MySQLdb.constants import FIELD_TYPE
from werkzeug.urls import url_decode

#----------------------------------
# Local non-exported definitions --
#----------------------------------

_log = logging.getLogger('dbMgr')

# pattern for valid db/table/column names
_idNameRe = re.compile(r'^[a-zA-Z_][0-9a-zA-Z_]*$')

def _validateId(idType, identifier):
    """
    Validate identifier, throws exception if identifier contains illegal characters
    """
    if not _idNameRe.match(identifier):
        raise ExceptionResponse(400, "InvalidArgument",
                                "{0} name is invalid: '{1}'".format(idType, identifier))

def _validateDbName(dbName):
    """ Validate database name """
    return _validateId("Database", dbName)

def _validateTableName(tblName):
    """ Validate table name """
    return _validateId("Table", tblName)

def _validateColumnName(columnName):
    """ Validate column name """
    return _validateId("Column", columnName)

# list of database names that we want to ignore
_specialDbs = set(['mysql', 'information_schema'])

# pattern for CREATE TABLE
_createTableRe = re.compile(r'^\s*CREATE\s+TABLE\s+(\w\.)?(\w+)', re.I)

def _dbDict(dbName):
    """ Make database instance dict out of db name """
    return dict(name=dbName, uri=url_for('.dropDb', dbName=dbName))

def _tblDict(dbName, tblName):
    """ Make table instance dict out of table name """
    return dict(name=tblName, uri=url_for('.deleteTable', dbName=dbName, tblName=tblName))

def _chunkDict(dbName, tblName, chunkId):
    """ Make table instance dict out of table name """
    uri = url_for('.deleteChunk', dbName=dbName, tblName=tblName, chunkId=chunkId)
    return dict(chunkId=chunkId, uri=uri, chunkTable=False, overlapTable=False)

def _getArgFlag(mdict, option, default=True):
    """
    Extracts specified argument from request arguments and converts it to boolean flag.
    Argument is required ot have value, possible values are '0', '1', 'yes', 'no', 'true', 'false'.
    """
    value = mdict.get(option, None)
    if value is None:
        return default
    value = value.strip().lower()
    if value not in ('0', '1', 'yes', 'no', 'true', 'false'):
        raise ExceptionResponse(400, "InvalidArgument",
                                "Unexpected value of '%s' option: \"%s\"" % (option, value))
    return value in ('1', 'yes', 'true')

def _typeCode2Name(code):
    """
    Convert mysql type code to type name, returns None if ther is no mapping.
    This is mysql-specific, it would be better to have technology-neutral way
    to provide the same mapping.
    """
    for name in dir(FIELD_TYPE):
        if getattr(FIELD_TYPE, name) == code:
            return name
    return None

#------------------------
# Exported definitions --
#------------------------

dbService = Blueprint('dbService', __name__, template_folder='dbService')


@dbService.errorhandler(db.DbException)
@dbService.errorhandler(MySQLdb.Error)
@dbService.errorhandler(MySQLdb.Warning)
def dbExceptionHandler(error):
    """ All leaked DbException exceptions make 500 error """
    return errorResponse(500, error.__class__.__name__, str(error))

@dbService.route('', methods=['GET'])
def listDbs():
    """ Return the list of databases """

    _log.debug('request: %s', request)
    _log.debug('GET => get database list')

    # use non-privileged account, may limit the list of databases returned.
    dbConn = Config.instance().dbConn()
    dbs = dbConn.execCommandN('SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA')

    dbs = [row[0] for row in dbs]
    _log.debug('dbs = %s', dbs)

    dbData = []
    for dbName in dbs:
        if dbName not in _specialDbs:
            dbData += [_dbDict(dbName)]

    return json.jsonify(results=dbData)


@dbService.route('', methods=['POST'])
def createDb():
    """
    Create new database, database name comes in a query string. In addition to creating
    database itself this method also grants all privileges on this database to regular
    non-privileged account.

    Following parameters are expected to come in a request (in request body
    with application/x-www-form-urlencoded content like regular form):
        db: database name (required)
    """

    _log.debug('request: %s', request)
    _log.debug('request.form: %s', request.form)
    _log.debug('POST => make database')

    # get database name from query
    dbName = request.form.get('db', '').strip()
    if not dbName:
        raise ExceptionResponse(400, "MissingArgument", "Database name argument (db) is missing")

    # validate it
    _validateDbName(dbName)

    # create database, use privileged account
    dbConn = Config.instance().privDbConn()
    try:
        dbConn.createDb(dbName)
    except db.DbException as exc:
        _log.error('exception when creating database %s: %s', dbName, exc)
        if exc.errCode() == db.DbException.DB_EXISTS:
            raise ExceptionResponse(409, "DatabaseExists", "Database %s already exists" % dbName)
        raise

    _log.debug('database %s created', dbName)

    # grant full access to non-privileged account
    cmd = "GRANT ALL PRIVILEGES ON {0}.* TO '{1}'@'{2}'"
    user = Config.instance().dbUser
    hostnames = ['localhost']
    for host in hostnames:
        try:
            dbConn.execCommand0(cmd.format(dbName, user, host))
        except db.DbException as exc:
            _log.error('exception when adding grants on database: %s', exc)
            raise ExceptionResponse(500, "GrantFailed", "Database %s created but GRANT failed" % dbName,
                                    str(exc))

    _log.debug('grants added')

    # return representation for new database, 201 code is for CREATED
    response = json.jsonify(result=_dbDict(dbName))
    response.status_code = 201
    return response


@dbService.route('/<dbName>', methods=['DELETE'])
def dropDb(dbName):
    """ Drop database """

    _log.debug('request: %s', request)
    _log.debug('request.form: %s', request.form)
    _log.debug('DELETE => drop database')

    # validate it
    _validateDbName(dbName)

    # use non-privileged account
    dbConn = Config.instance().dbConn()
    try:
        dbConn.dropDb(dbName)
    except db.DbException as exc:
        _log.error('exception when dropping database %s: %s', dbName, exc)
        if exc.errCode() == db.DbException.DB_DOES_NOT_EXIST:
            raise ExceptionResponse(404, "DatabaseMissing", "Database %s does not exist" % dbName)
        raise

    _log.debug('successfully dropped database %s', dbName)

    # return representation for deleted database
    return json.jsonify(result=_dbDict(dbName))


@dbService.route('/<dbName>/tables', methods=['GET'])
def listTables(dbName):
    """ Return the list of tables in a database """

    _log.debug('request: %s', request)
    _log.debug('GET => get table list')

    # validate it
    _validateDbName(dbName)

    # check the db exists (listTables() does not fail on non-existing databases)
    dbConn = Config.instance().dbConn()
    if not dbConn.dbExists(dbName):
        raise ExceptionResponse(404, "DatabaseMissing", "Database %s does not exist" % dbName)

    # list tables
    tables = dbConn.listTables(dbName)
    _log.debug('tables=%s', tables)
    tblData = [_tblDict(dbName, tblName) for tblName in tables]

    return json.jsonify(results=tblData)


@dbService.route('/<dbName>/tables', methods=['POST'])
def createTable(dbName):
    """
    Create new table, following parameters are expected to come in a request
    (in request body with application/x-www-form-urlencoded content like regular form):

    table:          table name (optional)
    schemaSource:   source for schema name, possible values: 'request', 'css',
                    (default: 'request')
    schema:         complete "CREATE TABLE ..." statement (optional)

    If `schemaSource` is 'request' then request must include `schema` parameter
    which is an SQL DDL statement starting with 'CREATE TABLE TableName ...'.
    Table name will be extracted from SQL statement in this case, if `table`
    parameter is specified then it must be the same as table name in SQL statement.

    If `schemaSource` is 'css' then `table` parameter must be specified. Table
    schema will be extracted from CSS in this case, `schemaSource` must not be given.
    """

    _log.debug('request: %s', request)
    _log.debug('request.form: %s', request.form)
    _log.debug('POST => create table')

    # validate database name
    _validateDbName(dbName)

    # get parameters
    tblName = request.form.get('table', '').strip()
    schemaSource = request.form.get('schemaSource', 'request').strip().lower()
    schema = request.form.get('schema', '').strip()

    if tblName:
        _validateTableName(tblName)

    # verify parameters
    if schemaSource == 'css':
        if not tblName:
            raise ExceptionResponse(400, "MissingArgument",
                                    "Required `table` parameter is missing (schemaSource=css)")
        if schema:
            raise ExceptionResponse(400, "ConflictingArgument",
                                    "`schema` parameter cannot be given with schemaSource=css")
    elif schemaSource == 'request':
        if not schema:
            raise ExceptionResponse(400, "MissingArgument",
                                    "Required `schema` parameter is missing (schemaSource=request)")
    else:
        raise ExceptionResponse(400, "InvalidArgument",
                                "`schemaSource` parameter has unexpected value \"%s\"" % schemaSource)

    if schemaSource == 'request':

        # get table name from schema
        match = _createTableRe.match(schema)
        if match is None:
            raise ExceptionResponse(400, "InvalidArgument",
                                    "`schema` parameter is not a CREATE TABLE statement")
        if match.group(1):
            raise ExceptionResponse(400, "InvalidArgument",
                                    "CREATE TABLE statement includes database name")
        schemaTblName = match.group(2)
        _log.debug('table name from schema in request: %s', schemaTblName)
        if tblName:
            if schemaTblName != tblName:
                _log.error('table name does not match schema table name')
                raise ExceptionResponse(400, "InvalidArgument",
                                        "Table name in `schema` does not match `table`: %s vs %s" %
                                        (schemaTblName, tblName))
        else:
            tblName = schemaTblName
            _validateTableName(tblName)

        # need to drop CREATE TABLE part, everything before (
        idx = schema.find('(')
        if idx < 0:
            _log.error('schema is missing opening parenthesis: %s', schema)
            raise ExceptionResponse(400, "InvalidArgument",
                                    "CREATE TABLE statement has no open parenthesis")
        schema = schema[idx:]
        _log.debug('schema from request: %s', schema)

    elif schemaSource == 'css':

        # get table schema from CSS
        try:
            css = Config.instance().qservAdmin()
            schema = css.getTableSchema(dbName, tblName)
            _log.debug('schema from CSS: %s', schema)
        except QservAdminException as exc:
            _log.error('Failed to retrieve table schema from CSS: %s', exc)
            raise ExceptionResponse(500, "CSSError", "Failed to retrieve table schema from CSS", str(exc))

        # schema in CSS is stored without CREATE TABLE, so we are already OK

    # create table
    dbConn = Config.instance().dbConn()
    try:
        dbConn.createTable(tblName, schema, dbName)
    except db.DbException as exc:
        _log.error('Exception when creating table: %s', exc)
        if exc.errCode() == db.DbException.TB_EXISTS:
            raise ExceptionResponse(409, "TableExists", "Table %s.%s already exists" % (dbName, tblName))
        raise

    _log.debug('table %s.%s created succesfully', dbName, tblName)

    # return representation for new database, 201 code is for CREATED
    response = json.jsonify(result=_tblDict(dbName, tblName))
    response.status_code = 201
    return response


@dbService.route('/<dbName>/tables/<tblName>', methods=['DELETE'])
def deleteTable(dbName, tblName):
    """
    Drop a table and optionally all chunk/overlap tables.

    Following parameters are expected to come in a request (in query string):
        dropChunks: boolean flag, false by default, accepted values:
                    '0', '1', 'yes', 'no', 'false', 'true'
    """

    _log.debug('request: %s', request)
    _log.debug('DELETE => drop table')

    # validate names
    _validateDbName(dbName)
    _validateTableName(tblName)

    # get options
    dropChunks = _getArgFlag(request.args, 'dropChunks', False)
    _log.debug('dropChunks: %s', dropChunks)

    dbConn = Config.instance().dbConn()
    if not dbConn.dbExists(dbName):
        raise ExceptionResponse(404, "DatabaseMissing", "Database %s does not exist" % dbName)

    try:
        # drop chunks first
        nChunks = 0
        if dropChunks:
            # regexp matching all chunk table names
            tblRe = re.compile('^' + tblName + '(FullOverlap)?_[0-9]+$')
            for table in dbConn.listTables(dbName):
                if tblRe.match(table):
                    _log.debug('dropping chunk table %s.%s', dbName, table)
                    dbConn.dropTable(table, dbName)
                    nChunks += 1

        # drop main table
        _log.debug('dropping main table %s.%s', dbName, tblName)
        dbConn.dropTable(tblName, dbName)

    except db.DbException as exc:
        _log.error('Exception when dropping tables: %s', exc)
        if exc.errCode() == db.DbException.TB_DOES_NOT_EXIST:
            chunkMsg = ""
            if nChunks:
                chunkMsg = ", but {0} chunk tables have been dropped".format(nChunks)
            raise ExceptionResponse(404, "TableMissing",
                                    "Table %s.%s does not exist%s" % (dbName, tblName, chunkMsg))
        raise

    return json.jsonify(result=_tblDict(dbName, tblName))


@dbService.route('/<dbName>/tables/<tblName>/chunks', methods=['GET'])
def listChunks(dbName, tblName):
    """ Return the list of chunks in a table. For non-chunked table empty list is returned. """

    _log.debug('request: %s', request)
    _log.debug('GET => get chunk list')

    # validate params
    _validateDbName(dbName)
    _validateTableName(tblName)

    # check the db exists (listTables() does not fail on non-existing databases)
    dbConn = Config.instance().dbConn()
    if not dbConn.tableExists(tblName, dbName):
        raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

    # regexp matching chunk table names
    # TODO: we need some central location for things like this
    tblRe = re.compile('^' + tblName + '(FullOverlap)?_([0-9]+)$')
    chunks = {}
    for table in dbConn.listTables(dbName):
        match = tblRe.match(table)
        if match is not None:
            chunkId = int(match.group(2))
            chunk = chunks.get(chunkId)
            if chunk is None:
                chunk = _chunkDict(dbName, tblName, chunkId)
                chunks[chunkId] = chunk
            if match.group(1) is None:
                chunk['chunkTable'] = True
            else:
                chunk['overlapTable'] = True

    _log.debug('found chunks: %s', chunks.keys())

    return json.jsonify(results=chunks.values())


@dbService.route('/<dbName>/tables/<tblName>/chunks', methods=['POST'])
def createChunk(dbName, tblName):
    """
    Create new chunk, following parameters are expected to come in a request
    (in request body with application/x-www-form-urlencoded content like regular form):

    chunkId:        chunk ID, non-negative integer
    overlapFlag:    if true then create overlap table too (default is true),
                    accepted values: '0', '1', 'yes', 'no', 'false', 'true'
    """

    _log.debug('request: %s', request)
    _log.debug('request.form: %s', request.form)
    _log.debug('POST => create chunk')

    # validate params
    _validateDbName(dbName)
    _validateTableName(tblName)

    # parse chunk number
    chunkId = request.form.get('chunkId', None)
    if chunkId is None:
        raise ExceptionResponse(400, "MissingArgument", "Chunk ID argument (chunkId) is missing")
    try:
        chunkId = int(chunkId)
        if chunkId < 0:
            raise ExceptionResponse(400, "InvalidArgument", "Chunk ID argument (chunkId) is negative")
    except ValueError:
        raise ExceptionResponse(400, "InvalidArgument", "Chunk ID argument (chunkId) is not an integer")

    overlapFlag = _getArgFlag(request.form, 'overlapFlag', True)

    # check that table exists
    dbConn = Config.instance().dbConn()
    if not dbConn.tableExists(tblName, dbName):
        raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

    chunkRepr = _chunkDict(dbName, tblName, chunkId)

    # make chunk table names
    # TODO: we need some central location for things like this
    tables = {'chunkTable': tblName + '_' + str(chunkId)}
    if overlapFlag:
        tables['overlapTable'] = tblName + 'FullOverlap_' + str(chunkId)
    _log.debug('will create tables: %s', tables)
    for tblType, chunkTable in tables.items():

        # make table using DDL from non-chunked one
        query = "CREATE TABLE {2}.{0} LIKE {2}.{1}".format(chunkTable, tblName, dbName)
        _log.debug('make chunk table: %s', chunkTable)
        try:
            dbConn.execCommand0(query)
        except db.DbException as exc:
            _log.error('Exception when creating table: %s', exc)
            if exc.errCode() == db.DbException.TB_EXISTS:
                raise ExceptionResponse(409, "TableExists",
                                        "Table %s.%s already exists" % (dbName, chunkTable))
            raise

        chunkRepr[tblType] = True

    response = json.jsonify(result=chunkRepr)
    response.status_code = 201
    return response


@dbService.route('/<dbName>/tables/<tblName>/chunks/<int:chunkId>', methods=['DELETE'])
def deleteChunk(dbName, tblName, chunkId):
    """ Delete chunk from a table, both chunk data and overlap data is dropped. """

    _log.debug('request: %s', request)
    _log.debug('request.form: %s', request.form)
    _log.debug('DELETE => delete chunk')

    # validate params
    _validateDbName(dbName)
    _validateTableName(tblName)
    if chunkId < 0:
        raise ExceptionResponse(400, "InvalidArgument", "Chunk ID argument is negative")

    # check that table exists
    dbConn = Config.instance().dbConn()
    if not dbConn.tableExists(tblName, dbName):
        raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

    chunkRepr = None

    # chunk data table
    # TODO: we need some central location for things like this
    table = tblName + '_' + str(chunkId)
    if dbConn.tableExists(table, dbName):

        # drop table
        _log.debug('drop chunk table: %s', table)
        dbConn.dropTable(table, dbName)

        chunkRepr = _chunkDict(dbName, tblName, chunkId)
        chunkRepr['chunkTable'] = True

    # overlap data table
    # TODO: we need some central location for things like this
    table = tblName + 'FullOverlap_' + str(chunkId)
    if dbConn.tableExists(table, dbName):

        # drop table
        _log.debug('drop chunk table: %s', table)
        dbConn.dropTable(table, dbName)

        if chunkRepr is not None:
            chunkRepr['overlapTable'] = True
        else:
            # chunk data table is missing
            _log.error('Chunk does not exist, but overlap does')
            raise ExceptionResponse(404, "ChunkDeleteFailed", "Cannot delete chunk data table",
                                    "Chunk %s is not found for table %s.%s (but overlap table was deleted)" %
                                    (chunkId, dbName, tblName))

    if chunkRepr is None:
        # nothing found
        _log.error('Chunk does not exist')
        raise ExceptionResponse(404, "ChunkDeleteFailed", "Cannot delete chunk data table",
                                "Chunk %s is not found for table %s.%s" % (chunkId, dbName, tblName))

    return json.jsonify(result=chunkRepr)


@dbService.route('/<dbName>/tables/<tblName>/data', methods=['POST'])
@dbService.route('/<dbName>/tables/<tblName>/chunks/<int:chunkId>/data', methods=['POST'])
@dbService.route('/<dbName>/tables/<tblName>/chunks/<int:chunkId>/overlap', methods=['POST'])
def loadData(dbName, tblName, chunkId=None):
    """
    Upload data into a table or chunk using the file format supported by mysql
    command LOAD DATA [LOCAL] INFILE.

    For this method we expect all data to come in one request, and in addition
    to table data we also need few parameters which define how data is formatted
    (things like column separator, line terminator, escape, etc.) The whole
    request must be multipart/form-data and contain two parts:
    - one with the name "load-options" which contains set of options encoded
      with usual application/x-www-form-urlencoded content type, options are:
      - delimiter - defaults to TAB
      - enclose - defaults to empty string (strings are not enclosed)
      - escape - defaults to backslash
      - terminate - defaults to newline
      - compressed - "0" or "1", by default is guessed from file extension (.gz)
    - one with the file data (name "table-data"), the data come in original
      format with binary/octet-stream content type and binary encoding, and it
      may be compressed with gzip.
    """

    def _copy(tableData, fifoName, compressed):
        """ Helper method to run data copy in a separate thread """
        if compressed:
            # gzip.GzipFile supports 'with' starting with python 2.7
            with gzip.GzipFile(fileobj=tableData.stream, mode='rb') as src:
                with open(fifoName, 'wb') as dst:
                    shutil.copyfileobj(src, dst)
                    _log.info('uncompressed table data to file %s', fifoName)
        else:
            tableData.save(fifoName)
            _log.info('copied table data to file %s', fifoName)

    @contextmanager
    def tmpDirMaker():
        """ Special context manager which creates/destroys temporary directory """
        # create and return directory
        tmpDir = tempfile.mkdtemp()
        yield tmpDir

        # do not forget to delete it on exit
        try:
            _log.info('deleting temporary directory %s', tmpDir)
            shutil.rmtree(tmpDir)
        except Exception as exc:
            _log.warning('failed to delete temporary directory %s: %s', tmpDir, exc)

    @contextmanager
    def threadStartJoin(thread):
        """ Special context manager which starts a thread and joins it on exit """
        thread.start()
        yield thread
        _log.debug('joining data copy thread')
        thread.join()

    _log.debug('request: %s', request)
    _log.debug('POST => load data into chunk or overlap')

    # validate params
    _validateDbName(dbName)
    _validateTableName(tblName)

    # check that table exists
    dbConn = Config.instance().dbConn()
    if not dbConn.tableExists(tblName, dbName):
        raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

    # determine chunk table name (if loading to chunk)
    if chunkId is not None:
        if request.path.endswith('/overlap'):
            tblName = tblName + 'FullOverlap_' + str(chunkId)
        else:
            tblName = tblName + '_' + str(chunkId)
        if not dbConn.tableExists(tblName, dbName):
            raise ExceptionResponse(404, "ChunkTableMissing",
                                    "Chunk table %s.%s does not exist" % (dbName, tblName))

    # optional load-options
    options = dict(delimiter='\t', enclose='', escape='\\', terminate='\n', compressed=None)
    formOptions = request.form.get('load-options')
    _log.debug('formOptions: %s', formOptions)
    if formOptions:
        formOptions = url_decode(formOptions)

        # check that there are no non-recognized options
        for key, value in formOptions.items():
            if key not in options:
                raise ExceptionResponse(400, "IllegalOption",
                                        "unrecognized option %s in load-options" % key)
            options[key] = value
        _log.debug('options: %s', options)

    # get table data
    table_data = request.files.get('table-data')
    if table_data is None:
        _log.debug("table-data part is missing from request")
        raise ExceptionResponse(400, "MissingFileData", "table-data part is missing from request")
    with closing(table_data) as data:
        _log.debug('data: name=%s filename=%s', data.name, data.filename)

        # named pipe to send data to mysql, make it in a temporary dir
        with tmpDirMaker() as tmpDir:
            fifoName = os.path.join(tmpDir, 'tabledata.dat')
            os.mkfifo(fifoName, 0600)

            # do we need to uncompress?
            compressed = _getArgFlag(options, 'compressed', None)
            if compressed is None:
                compressed = data.filename.endswith('.gz')

            _log.debug('starting data copy thread')
            args = data, fifoName, compressed
            with threadStartJoin(Thread(target=_copy, args=args)) as copyThread:

                # Build the query, we use LOCAL data loading to avoid messing with grants and
                # file protection.
                sql = "LOAD DATA LOCAL INFILE %(file)s INTO TABLE {0}.{1}".format(dbName, tblName)
                sql += " FIELDS TERMINATED BY %(delimiter)s ENCLOSED BY %(enclose)s \
                         ESCAPED BY %(escape)s LINES TERMINATED BY %(terminate)s"

                del options['compressed']
                options['file'] = fifoName

                # execute query
                _log.debug("query: %s, data: %s", sql, options)
                cursor = dbConn._conn.cursor()
                cursor.execute(sql, options)
                count = cursor.rowcount

    return json.jsonify(result=dict(status="OK", count=count))


@dbService.route('/<dbName>/tables/<tblName>/index', methods=['GET'])
@dbService.route('/<dbName>/tables/<tblName>/chunks/<int:chunkId>/index', methods=['GET'])
def getIndex(dbName, tblName, chunkId=None):
    """
    Return index data (array of [objectId, chunkId, subChunkId] arrays). This only works
    on partitined tables and is only supposed to be used with director table (but there is
    no check currently that table is a director table.

    Expects one parameter 'columns' which specifies comma-separated list of three column
    names. Default column names are "objectId", "chunkId", "subChunkId". Result returns
    columns in the same order as they are specified in 'columns' argument.
    """

    _log.debug('request: %s', request)
    _log.debug('GET => get index')

    # validate params
    _validateDbName(dbName)
    _validateTableName(tblName)
    if chunkId is not None and chunkId < 0:
        raise ExceptionResponse(400, "InvalidArgument", "Chunk ID argument is negative")

    # get column names and validate
    columns = request.args.get('columns', "objectId,chunkId,subChunkId").strip()
    columns = columns.split(',')
    if len(columns) != 3:
        raise ExceptionResponse(400, "InvalidArgument",
                                "'columns' parameter requires comma-separated list of three column names")

    # check that table exists
    dbConn = Config.instance().dbConn()
    if not dbConn.tableExists(tblName, dbName):
        raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

    # regexp matching chunk table names (but not overlap tables).
    # TODO: we need some central location for things like this
    tblRe = re.compile('^' + tblName + '_([0-9]+)$')
    tables = []
    for table in dbConn.listTables(dbName):
        match = tblRe.match(table)
        if match is not None:
            if chunkId is not None:
                if chunkId == int(match.group(1)):
                    tables.append(table)
                    break                 # only one table can match
            else:
                tables.append(table)

    # we expect at least one chunk table to be found
    if not tables:
        _log.error('no matching chunk tables found for table %s.%s chunkId=%s',
                   dbName, tblName, chunkId)
        raise ExceptionResponse(404, "NoMatchingChunks", "Failed to find any chunk data table",
                                "No matching cunks for table %s.%s chunkId=%s" % (dbName, tblName, chunkId))

    _log.debug("tables to scan: %s", tables)

    # TODO: list of lists is probably not the most efficient storage
    result = []
    for table in tables:
        query = "SELECT {0}, {1}, {2} FROM {3}.{4}"
        query = query.format(columns[0], columns[1], columns[2], dbName, table)

        # TODO: to be replaced with dbConn.cursor()
        cursor = dbConn._conn.cursor()
        _log.debug('query: %s', query)
        cursor.execute(query)

        if cursor.description:
            descr = [dict(name=d[0], type=_typeCode2Name(d[1])) for d in cursor.description]
        else:
            descr = [dict(name=name) for name in columns]
        _log.debug("description: %s", descr)

        while True:
            rows = cursor.fetchmany(1000000)
            if not rows:
                break
            for row in rows:
                result.append(list(row))

    _log.debug("retrieved %d index rows", len(result))

    return json.jsonify(result=dict(rows=result, description=descr))
