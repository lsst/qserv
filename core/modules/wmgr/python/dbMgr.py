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

# --------------------------------
#  Imports of standard modules --
# --------------------------------
from contextlib import closing, contextmanager
import gzip
import logging
import os
import re
import shutil
import tempfile
from threading import Thread

# -----------------------------
# Imports for other modules --
# -----------------------------
from .config import Config
from .errors import errorResponse, ExceptionResponse
from flask import Blueprint, json, request, url_for
from werkzeug.urls import url_decode
from sqlalchemy.exc import NoSuchTableError, OperationalError, SQLAlchemyError

from lsst.db import utils
import lsst.qserv.css


# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

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


def _columnDict(row):
    """ Make column dict out of column row """
    return dict(name=row[0], type=row[1], null=row[2], key=row[3], default=row[4])


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

# ------------------------
# Exported definitions --
# ------------------------


dbService = Blueprint('dbService', __name__, template_folder='dbService')


@dbService.errorhandler(SQLAlchemyError)
def dbExceptionHandler(error):
    """ All leaked database-related exceptions generate 500 error """
    return errorResponse(500, error.__class__.__name__, str(error))


@dbService.route('', methods=['GET'])
def listDbs():
    """ Return the list of databases """

    _log.debug('request: %s', request)
    _log.debug('GET => get database list')

    # use non-privileged account, may limit the list of databases returned.
    with Config.instance().dbEngine().begin() as dbConn:
        dbs = utils.listDbs(dbConn)

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
    with Config.instance().privDbEngine().begin() as dbConn:
        try:
            utils.createDb(dbConn, dbName)
        except utils.DatabaseExistsError as e:
            _log.error('exception when creating database %s: %s', dbName, e)
            raise ExceptionResponse(409, "DatabaseExists", "Database %s already exists" % dbName)

        _log.debug('database %s created', dbName)

        # grant full access to non-privileged account
        cmd = "GRANT ALL PRIVILEGES ON {0}.* TO '{1}'@'{2}'"
        user = Config.instance().dbUser
        hostnames = ['localhost']
        for host in hostnames:
            try:
                dbConn.execute(cmd.format(dbName, user, host))
            except OperationalError as exc:
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
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.dbExists(dbConn, dbName):
            raise ExceptionResponse(404, "DatabaseMissing", "Database %s does not exist" % dbName)
        try:
            utils.dropDb(dbConn, dbName)
        except SQLAlchemyError as exc:
            _log.error('Db exception when dropping database %s: %s', dbName, exc)
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
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.dbExists(dbConn, dbName):
            raise ExceptionResponse(404, "DatabaseMissing", "Database %s does not exist" % dbName)

        # list tables
        tables = utils.listTables(dbConn, dbName)

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
    chunkColumns:   boolean flag, false by default, accepted values:
                    '0', '1', 'yes', 'no', 'false', 'true'. If set to true then
                    delete columns "_chunkId", "_subChunkId" from table (if they
                    exist) and add columns "chunkId", "subChunkId" (if they don't exist)

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
    chunkColumns = _getArgFlag(request.form, 'chunkColumns', False)

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

    with Config.instance().dbEngine().begin() as dbConn:

        # create table
        if schemaSource == 'request':

            # assume that we have correct SQL already
            try:
                utils.useDb(dbConn, dbName)
                utils.createTableFromSchema(dbConn, schema)
            except utils.TableExistsError as exc:
                _log.error('Exception when creating table: %s', exc)
                raise ExceptionResponse(409, "TableExists", "Table %s.%s already exists" % (dbName, tblName))

        elif schemaSource == 'css':

            # get table schema from CSS
            try:
                css = Config.instance().cssAccess()
                schema = css.getTableSchema(dbName, tblName)
                _log.debug('schema from CSS: %s', schema)
            except lsst.qserv.css.CssError as exc:
                _log.error('Failed to retrieve table schema from CSS: %s', exc)
                raise ExceptionResponse(500, "CSSError", "Failed to retrieve table schema from CSS", str(exc))

            # schema in CSS is stored without CREATE TABLE, so we are already OK
            try:
                utils.createTable(dbConn, tblName, schema, dbName)
            except utils.TableExistsError as exc:
                _log.error('Exception when creating table: %s', exc)
                raise ExceptionResponse(409, "TableExists", "Table %s.%s already exists" % (dbName, tblName))
        _log.debug('table %s.%s created succesfully', dbName, tblName)

        if chunkColumns:
            # Change table schema, drop _chunkId, _subChunkId, add chunkId, subChunkId

            try:
                table = '`{0}`.`{1}`'.format(dbName, tblName)

                # get current column set
                q = "SHOW COLUMNS FROM %s" % table
                result = dbConn.execute(q)
                rows = result.fetchall()
                columns = set(row[0] for row in rows)

                # delete rows
                toDelete = set(["_chunkId", "_subChunkId"]) & columns
                mods = ['DROP COLUMN %s' % col for col in toDelete]

                # create rows, want them in that order
                toAdd = ["chunkId", "subChunkId"]
                mods += ['ADD COLUMN %s INT(11) NOT NULL' % col for col in toAdd if col not in columns]

                if mods:
                    _log.info('Altering schema for table %s', table)

                    q = 'ALTER TABLE %s ' % table
                    q += ', '.join(mods)

                    _log.debug('query: %s', q)
                    dbConn.execute(q)
            except Exception as exc:
                _log.error('Failed to alter database table: %s', exc)
                raise ExceptionResponse(500, "DbError", "Failed to alter database table", str(exc))

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

    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.dbExists(dbConn, dbName):
            raise ExceptionResponse(404, "DatabaseMissing", "Database %s does not exist" % dbName)

        try:
            # drop chunks first
            nChunks = 0
            if dropChunks:
                # regexp matching all chunk table names
                tblRe = re.compile('^' + tblName + '(FullOverlap)?_[0-9]+$')
                for table in utils.listTables(dbConn, dbName):
                    if tblRe.match(table):
                        _log.debug('dropping chunk table %s.%s', dbName, table)
                        utils.dropTable(dbConn, table, dbName)
                        nChunks += 1

            # drop main table
            _log.debug('dropping main table %s.%s', dbName, tblName)
            utils.dropTable(dbConn, tblName, dbName)

        except NoSuchTableError as exc:
            _log.error('Exception when dropping table: %s', exc)
            chunkMsg = ""
            if nChunks:
                chunkMsg = ", but {0} chunk tables have been dropped".format(nChunks)
            raise ExceptionResponse(404, "TableMissing",
                                    "Table %s.%s does not exist%s" % (dbName, tblName, chunkMsg))

    return json.jsonify(result=_tblDict(dbName, tblName))


@dbService.route('/<dbName>/tables/<tblName>/schema', methods=['GET'])
def tableSchema(dbName, tblName):
    """ Return result of SHOW CREATE TABLE statement for given table. """

    _log.debug('request: %s', request)
    _log.debug('GET => show create table')

    # validate params
    _validateDbName(dbName)
    _validateTableName(tblName)

    # check the db exists (listTables() does not fail on non-existing databases)
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.tableExists(dbConn, tblName, dbName):
            raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

        query = "SHOW CREATE TABLE `%s`.`%s`" % (dbName, tblName)
        rows = dbConn.execute(query)

    return json.jsonify(result=rows.first()[1])


@dbService.route('/<dbName>/tables/<tblName>/columns', methods=['GET'])
def tableColumns(dbName, tblName):
    """ Return result of SHOW COLUMNS statement for given table. """

    _log.debug('request: %s', request)
    _log.debug('GET => show columns')

    # validate params
    _validateDbName(dbName)
    _validateTableName(tblName)

    # check the db exists (listTables() does not fail on non-existing databases)
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.tableExists(dbConn, tblName, dbName):
            raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

        query = "SHOW COLUMNS FROM `%s`.`%s`" % (dbName, tblName)
        rows = dbConn.execute(query)
        columns = [_columnDict(row) for row in rows]

    return json.jsonify(results=columns)


@dbService.route('/<dbName>/tables/<tblName>/chunks', methods=['GET'])
def listChunks(dbName, tblName):
    """ Return the list of chunks in a table. For non-chunked table empty list is returned. """

    _log.debug('request: %s', request)
    _log.debug('GET => get chunk list')

    # validate params
    _validateDbName(dbName)
    _validateTableName(tblName)

    # check the db exists (listTables() does not fail on non-existing databases)
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.tableExists(dbConn, tblName, dbName):
            raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

        # regexp matching chunk table names
        # TODO: we need some central location for things like this
        tblRe = re.compile('^' + tblName + '(FullOverlap)?_([0-9]+)$')
        chunks = {}
        for table in utils.listTables(dbConn, dbName):
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

    _log.debug('found chunks: %s', list(chunks.keys()))

    return json.jsonify(results=list(chunks.values()))


@dbService.route('/<dbName>/tables/<tblName>/chunks', methods=['POST'])
def createChunk(dbName, tblName):
    """
    Create new chunk, following parameters are expected to come in a request
    (in request body with application/x-www-form-urlencoded content like regular form):

    chunkId:        chunk ID, non-negative integer
    overlapFlag:    if true then create overlap table too (default is true),
                    accepted values: '0', '1', 'yes', 'no', 'false', 'true'

    This method is supposed to handle both regular tables and views.
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
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.tableExists(dbConn, tblName, dbName):
            raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

        chunkRepr = _chunkDict(dbName, tblName, chunkId)

        # make chunk table names
        # TODO: we need some central location for things like this
        tables = {'chunkTable': tblName + '_' + str(chunkId)}
        if overlapFlag:
            tables['overlapTable'] = tblName + 'FullOverlap_' + str(chunkId)
        _log.debug('will create tables: %s', tables)
        for tblType, chunkTable in tables.items():

            # check if this table is actually a view
            if utils.isView(dbConn, tblName, dbName):

                # view needs more complicated algorithm to copy its definition, first copy
                # its current definition, then rename existing view and then re-create it again

                _log.debug('table %s is a view', tblName)

                # get its current definition
                query = "SHOW CREATE VIEW `{0}`.`{1}`".format(dbName, tblName)
                rows = dbConn.execute(query)
                viewDef = rows.first()[1]

                # rename it
                query = "RENAME TABLE `{0}`.`{1}` to `{0}`.`{2}`".format(dbName, tblName, chunkTable)
                dbConn.execute(query)

                # re-create it
                dbConn.execute(viewDef)

            else:

                # make table using DDL from non-chunked one
                _log.debug('make chunk table: %s', chunkTable)
                try:
                    utils.createTableLike(dbConn, dbName, chunkTable, dbName, tblName)
                except utils.TableExistsError as exc:
                    _log.error('Db exception when creating table: %s', exc)
                    raise ExceptionResponse(409, "TableExists",
                                            "Table %s.%s already exists" % (dbName, chunkTable))

                if tblType == 'overlapTable':
                    _fixOverlapIndices(dbConn, dbName, chunkTable)

            chunkRepr[tblType] = True

    response = json.jsonify(result=chunkRepr)
    response.status_code = 201
    return response


def _fixOverlapIndices(dbConn, database, ctable):
    """
    Replaces unique indices on overlap table with non-unique
    """

    # Query to fetch all unique indices with corresponding column names,
    # this is not very portable.
    query = "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS " \
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND NON_UNIQUE = 0"
    _log.debug('query: %s, data: %s', query, (database, ctable))
    result = dbConn.execute(query, (database, ctable))

    # map index name to list of columns
    indices = {}
    for idx, seq, column in result.fetchall():
        indices.setdefault(idx, []).append((seq, column))

    # replace each index with non-unique one
    for idx, columns in indices.items():

        _log.info('Replacing index %s on table %s.%s', idx, database, ctable)

        # drop existing index, PRIMARY is a keyword so it must be quoted
        query = 'DROP INDEX `{0}` ON `{1}`.`{2}`'.format(idx, database, ctable)
        _log.debug('query: %s', query)
        dbConn.execute(query)

        # column names sorted by index sequence number
        colNames = ', '.join([col[1] for col in sorted(columns)])

        # create new index
        if idx == 'PRIMARY':
            idx = 'PRIMARY_NON_UNIQUE'
        query = 'CREATE INDEX `{0}` ON `{1}`.`{2}` ({3})'.format(idx, database, ctable, colNames)
        _log.debug('query: %s', query)
        dbConn.execute(query)


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
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.tableExists(dbConn, tblName, dbName):
            raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

        chunkRepr = None

        # chunk data table
        # TODO: we need some central location for things like this
        table = tblName + '_' + str(chunkId)
        if utils.tableExists(dbConn, table, dbName):

            # drop table
            _log.debug('drop chunk table: %s', table)
            utils.dropTable(dbConn, table, dbName)

            chunkRepr = _chunkDict(dbName, tblName, chunkId)
            chunkRepr['chunkTable'] = True

        # overlap data table
        # TODO: we need some central location for things like this
        table = tblName + 'FullOverlap_' + str(chunkId)
        if utils.tableExists(dbConn, table, dbName):

            # drop table
            _log.debug('drop chunk table: %s', table)
            utils.dropTable(dbConn, table, dbName)

            if chunkRepr is not None:
                chunkRepr['overlapTable'] = True
            else:
                # chunk data table is missing
                _log.error('Chunk does not exist, but overlap does')
                raise ExceptionResponse(404, "ChunkDeleteFailed", "Cannot delete chunk data table",
                                        "Chunk %s is not found for table %s.%s "
                                        "(but overlap table was deleted)" % (chunkId, dbName, tblName))

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
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.tableExists(dbConn, tblName, dbName):
            raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

        # determine chunk table name (if loading to chunk)
        if chunkId is not None:
            if request.path.endswith('/overlap'):
                tblName = tblName + 'FullOverlap_' + str(chunkId)
            else:
                tblName = tblName + '_' + str(chunkId)
            if not utils.tableExists(dbConn, tblName, dbName):
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
                os.mkfifo(fifoName, 0o600)

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
                    results = dbConn.execute(sql, options)
                    count = results.rowcount

    return json.jsonify(result=dict(status="OK", count=count))


@dbService.route('/<dbName>/tables/<tblName>/index', methods=['GET'])
@dbService.route('/<dbName>/tables/<tblName>/chunks/<int:chunkId>/index', methods=['GET'])
def getIndex(dbName, tblName, chunkId=None):
    """
    Return index data (array of (objectId, chunkId, subChunkId) tuples). This only works
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
    with Config.instance().dbEngine().begin() as dbConn:
        if not utils.tableExists(dbConn, tblName, dbName):
            raise ExceptionResponse(404, "TableMissing", "Table %s.%s does not exist" % (dbName, tblName))

        # regexp matching chunk table names (but not overlap tables).
        # TODO: we need some central location for things like this
        tblRe = re.compile('^' + tblName + '_([0-9]+)$')
        tables = []
        for table in utils.listTables(dbConn, dbName):
            match = tblRe.match(table)
            if match is not None:
                if chunkId is not None:
                    if chunkId == int(match.group(1)):
                        tables.append(table)
                        break  # only one table can match
                else:
                    tables.append(table)

        # we expect at least one chunk table to be found
        if not tables:
            _log.error('No matching chunk tables found for table %s.%s chunkId=%s',
                       dbName, tblName, chunkId)
            raise ExceptionResponse(404, "NoMatchingChunks", "Failed to find any chunk data table",
                                    "No matching chunks for table %s.%s chunkId=%s" %
                                    (dbName, tblName, chunkId))

        _log.debug("tables to scan: %s", tables)

        # TODO: list of lists is probably not the most efficient storage
        result = []
        for table in tables:
            query = "SELECT {0}, {1}, {2} FROM {3}.{4}"
            query = query.format(columns[0], columns[1], columns[2], dbName, table)

            _log.debug('query: %s', query)
            allRows = dbConn.execute(query)

            if allRows.keys():
                descr = [dict(name=d[0], type=utils.typeCode2Name(dbConn, d[1]))
                         for d in allRows.cursor.description]
            else:
                descr = [dict(name=name) for name in columns]
            _log.debug("description: %s", descr)

            while True:
                rows = allRows.fetchmany(1000000)
                if not rows:
                    break
                for row in rows:
                    result.append(tuple(row))

    _log.debug("retrieved %d index rows", len(result))

    return json.jsonify(result=dict(rows=result, description=descr))

#
# Resource /<dbName>/chunks represents sets of (non-empty) chunks in the
# database. Currently this set is populated from "empty chunk list" file,
# in the future it will likely be derived from other information such as
# secondary index. Chunk set is cached in czar memory, when chunk set is
# updated the cache needs to be reset.
#
# Currently we support only one operation on chunk set - resetting the cache
# which is implemented via PUT method on /<dbName>/chunks/cache resource
#


@dbService.route('/<dbName>/chunks/cache', methods=['PUT'])
def resetChunksCache(dbName):
    """
    Force czar to reset chunk cache.

    This method only makes sense for czar wmgr, and unlike other methods it
    interacts with czar (via mysql-proxy).
    """

    _log.debug('request: %s', request)
    _log.debug('PUT => reset chunk cache')

    # validate params
    _validateDbName(dbName)

    # proxy does not support COMMIT so make sure we do not use transactions here
    # by using connect() over usual begin()
    with Config.instance().proxyDbEngine().connect() as dbConn:

        try:
            query = "FLUSH QSERV_CHUNKS_CACHE FOR {}".format(dbName)
            dbConn.execute(query)
        except Exception as exc:
            _log.error('exception executing FLUSH QSERV_CHUNKS_CACHE: %s', exc)
            raise ExceptionResponse(500, "FLushFailed",
                                    "FLUSH QSERV_CHUNKS_CACHE failed for database %s" % dbName, str(exc))

    return json.jsonify(result=dict(status="OK"))
