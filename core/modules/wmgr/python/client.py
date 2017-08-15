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
Module defining WmgrClient class and related methods.

@author Andy Salnikov <salnikov@slac.stanford.edu>
"""

# --------------------------------
#  Imports of standard modules --
# --------------------------------
from past.builtins import basestring
import io
import logging

# -----------------------------
# Imports for other modules --
# -----------------------------
import requests
from requests.compat import urlencode

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

# used as default value to distinguish from None
_None = object()

_log = logging.getLogger(__name__)


class _MPEncoder(object):
    """
    Special class for streamable multi-part encoded body.

    Idea is stolen from requests-toolbelt MultipartEncoder class.

    This class implements partial file interface.
    """

    def __init__(self, data, boundary=None, encoding='utf-8'):
        """
        Data is a sequence of dictionaries, each dict these keys:

        name:        part name, required
        data:        file object or string with data, required
        filename:    optional part file name
        content_type:   optional content type
        headers:     optional dict with headers
        """

        self._encoding = encoding
        self._boundary = boundary or 'part-X-of-the-multipart-request'
        self.content_type = 'multipart/form-data; boundary=' + self._boundary
        self._data = self._processData(data)
        self._size = self._getSize(self._data)

    def _processData(self, items):
        """
        Process input data and transform into internal buffers
        """

        result = []

        currentBuf = None

        # add special item at the end as a closing part
        items = list(items) + [dict(name=None, data=None)]

        for item in items:
            # first two elements are required
            name = item['name']
            data = item['data']
            filename = item.get('filename')
            content = item.get('content_type')
            headers = item.get('headers')

            if currentBuf is None:
                currentBuf = io.BytesIO()

            currentBuf.write(self._makeHeader(name, filename, content, headers))

            if name is not None:
                if isinstance(data, basestring):
                    currentBuf.write(data.encode(self._encoding))
                    currentBuf.write('\r\n'.encode(self._encoding))
                else:
                    currentBuf.seek(0)
                    result += [currentBuf]
                    result += [data]
                    currentBuf = io.BytesIO()
                    currentBuf.write('\r\n'.encode(self._encoding))

        if currentBuf is not None:
            currentBuf.seek(0)
            result += [currentBuf]

        return result

    def _makeHeader(self, name=None, filename=None, content=None, headers=None):
        """
        Generate single part header, return as a string.
        """

        if name is None:
            # post-data boundary
            data = ['--' + self._boundary + '--']
        else:

            data = ['--' + self._boundary]

            disp = 'Content-Disposition: form-data; name="{0}"'.format(name)
            if filename:
                disp += '; filename="{0}"'.format(filename)
            data.append(disp)

            if content:
                data.append('Content-Type: {0}'.format(content))
            data.append('Content-Transfer-Encoding: binary')

            if headers:
                for key, val in headers.items():
                    data.append('{0}: {1}'.format(key, val))

            data.append('')

        data.append('')

        return '\r\n'.join(data).encode(self._encoding)

    @staticmethod
    def _getSize(items):
        """
        Calculate total body size.
        """
        size = 0
        for piece in items:
            piece.seek(0, 2)
            size += piece.tell()
            piece.seek(0)
        return size

    def __len__(self):
        """
        Return total body size.
        """
        return self._size

    def read(self, size=-1):
        """
        Return next chunk of data.
        """
        while self._data:
            result = self._data[0].read(size)
            if result:
                return result
            del self._data[0]
        return ""

# ------------------------
# Exported definitions --
# ------------------------


class ClientException(Exception):
    """ Base class for all exceptions in this module"""
    pass


class CommunicationError(ClientException):
    """
    Exception raised if server communication failed.
    """

    def __init__(self, msg):
        ClientException.__init__(self, 'Server communication failure: ' + msg)


class ServerError(ClientException):
    """
    Exception raised if server sent us HTTP error code.
    """

    def __init__(self, code, body):
        ClientException.__init__(self, 'Server returned error: %s (body: "%s")' % (code, body))
        self.code = code


class ServerResponseError(ClientException):
    """
    Exception raised if server response is incomprehensible
    (e.g. missing expected keys in JSON)
    """

    def __init__(self, msg, *args):
        ClientException.__init__(self, 'Server response error: ' + msg, *args)

# ---------------------
#  Class definition --
# ---------------------


class WmgrClient(object):
    """
    This class provides Python API for worker management service.

    Worker management service defines RESTful HTTP interface, to simplify
    client communication via HTTP this new class was defined which hides
    details of HTTP protocol and data formatting. WmgrClient class supports
    communication with single instance of worker management service.
    """

    # ----------------
    #  Constructor  --
    # ----------------

    def __init__(self, host, port, secretFile=None, user=None, passwd=None, auth="digest"):
        """
        Make new client instance.

        Client needs to specify endpoint for wmgr (host and port number) and
        optionally provide athentication parameters - auth type and either
        secretFile or (user, passwd) pair. If auth is 'none' then authentication
        is not used and all other parameters are not used. If none of the
        secretFile or user/passwd is given it is equivalent to auth='none.
        For authentication to work auth must be set to one of the 'basic'
        or 'digest' and either secretFile or user/passwd must be given.

        @param host:  remote host name where wmgr service runs
        @param port:  port number for wmgr service
        @param secretFile:  path to a file with secret
        @param user:  user name
        @param passwd:  password
        @param auth:  authentication type, one of 'none', 'basic', 'digest'.
        """

        if secretFile and (user or passwd):
            raise ValueError('WmgrClient: cannot specify secretFile and user or passwd')
        if auth not in ('none', 'basic', 'digest'):
            raise ValueError('WmgrClient: auth is not one of none, basic or digest')

        # read secret file
        if secretFile:
            user, passwd = self.readSecret(secretFile)

        self.host = host
        if self.host == 'localhost':
            self.host = '127.0.0.1'
        self.port = port
        self.auth = None
        if user is not None or passwd is not None:
            if auth == 'basic':
                self.auth = requests.auth.HTTPBasicAuth(user, passwd)
            elif auth == 'digest':
                self.auth = requests.auth.HTTPDigestAuth(user, passwd)

    @staticmethod
    def readSecret(fileName):
        """
        Reads secret file and returns (user, password) tuple
        """
        try:
            _log.debug('Reading secret from file: %s', fileName)
            secret = open(fileName).read().strip()
            if ':' not in secret:
                raise RuntimeError("invalid content of secret file (missing colon): " + fileName)
            secret = secret.split(':', 1)
            if not secret[0] or not secret[1]:
                raise RuntimeError("invalid content of secret file (empty fields): " + fileName)
            return secret
        except Exception as exc:
            raise RuntimeError("failed to read secret file: " + str(exc))

    def databases(self):
        """
        Returns the list of database names.

        @raise ClientException: in case of problems
        """
        _log.debug('get database list')
        result = self._requestJSON('dbs', '')
        return self._getKey(result, 'name')

    def createDb(self, dbName):
        """
        Create new database.

        @raise ClientException: in case of problems
        """
        _log.debug('create database: %s', dbName)
        self._requestJSON('dbs', '', method='POST', data=dict(db=dbName))

    def dropDb(self, dbName, mustExist=True):
        """
        Delete existing database. If mustExist is True and database does not exist then
        exception is raised.

        @raise ClientException: in case of problems
        """
        _log.debug('drop database: %s', dbName)
        try:
            self._requestJSON('dbs', dbName, method='DELETE')
        except ServerError as exc:
            # if db does not exist then it's OK
            if exc.code != 404 or mustExist:
                raise

    def tables(self, dbName):
        """
        Returns the list of table names in given database.

        @raise ClientException: in case of problems
        """
        _log.debug('get tables, database: %s', dbName)
        result = self._requestJSON('dbs', dbName + '/tables')
        return self._getKey(result, 'name')

    def createTable(self, dbName, tableName, schema=None, chunkColumns=False):
        """
        Create new table.

        Table schema ("CREATE TABLE ...") may be specified in schema argument,
        if schema is None then table schema will be loaded from CSS. If chunkColumns
        is True then delete colums "_chunkId", "_subChunkId" from table (if they
        exist) and add columns "chunkId", "subChunkId" (if they don't exist).

        @raise ClientException: in case of problems
        """

        _log.debug('create table: %s.%s', dbName, tableName)
        data = dict(table=tableName, chunkColumns=str(int(chunkColumns)))
        if schema:
            data['schema'] = schema
        else:
            data['schemaSource'] = 'CSS'
        self._requestJSON('dbs', dbName + '/tables', method='POST', data=data)

    def dropTable(self, dbName, tableName, dropChunks=True, mustExist=True):
        """
        Delete existing table. If dropChunks is True then delete all chunks tables as well.
        If mustExist is True and table does not exist then exception is raised.

        @raise ClientException: in case of problems
        """
        _log.debug('drop table: %s.%s', dbName, tableName)
        params = dict(dropChunks=str(int(dropChunks)))
        try:
            self._requestJSON('dbs', dbName + '/tables/' + tableName, method='DELETE', params=params)
        except ServerError as exc:
            # if db does not exist then it's OK
            if exc.code != 404 or mustExist:
                raise

    def tableSchema(self, dbName, tableName):
        """
        Return result of SHOW CREATE TABLE statement for given table.

        @return: string
        @raise ClientException: in case of problems
        """
        _log.debug('get table schema, table: %s.%s', dbName, tableName)
        resource = dbName + '/tables/' + tableName + '/schema'
        result = self._requestJSON('dbs', resource)
        return result

    def tableColumns(self, dbName, tableName):
        """
        Return result of SHOW COLUMNS statement for given table.

        @return: list of dicts
        @raise ClientException: in case of problems
        """
        _log.debug('get table columns, table: %s.%s', dbName, tableName)
        resource = dbName + '/tables/' + tableName + '/columns'
        result = self._requestJSON('dbs', resource)
        return result

    def chunks(self, dbName, tableName):
        """
        Returns the list of chunks in given table.

        @return: list of integers
        @raise ClientException: in case of problems
        """
        _log.debug('get chunks, table: %s.%s', dbName, tableName)
        resource = dbName + '/tables/' + tableName + '/chunks'
        result = self._requestJSON('dbs', resource)
        return self._getKey(result, 'chunkId')

    def createChunk(self, dbName, tableName, chunkId, overlap):
        """
        Create new chunk, this should work with both tables and view.

        If overlap is True then create overlap table in addition to chunk table.

        @raise ClientException: in case of problems
        """

        _log.debug('create table: %s.%s', dbName, tableName)
        overlapFlag = 'yes' if overlap else 'no'
        data = dict(chunkId=chunkId, overlapFlag=overlapFlag)
        resource = dbName + '/tables/' + tableName + '/chunks'
        self._requestJSON('dbs', resource, method='POST', data=data)

    def deleteChunk(self, dbName, tableName, chunkId):
        """
        Delete existing chunk.

        @raise ClientException: in case of problems
        """
        _log.debug('delete chunk: %s.%s[%s]', dbName, tableName, chunkId)
        resource = dbName + '/tables/' + tableName + '/chunks/' + str(chunkId)
        self._requestJSON('dbs', resource, method='DELETE')

    def getIndex(self, dbName, tableName, chunkId=None, columns=None):
        """
        Return index data (array of [objectId, chunkId, subChunkId] arrays). This only works
        on partitined tables and is only supposed to be used with director table.

        If chunkId is None then index data for all chunks is returned, otherwise only
        for specified chunkId (must be an integer number).

        Optional parameter columns can be yused to specify a sequence of three column
        names for for objectId, chunkId, and subChunkId (in that order), by default
        ("objectId", "chunkId", "subChunkId") is used.

        @raise ClientException: in case of problems
        """

        if columns is None:
            columns = "objectId,chunkId,subChunkId"
        else:
            columns = ','.join(columns)

        if chunkId is None:
            resource = dbName + '/tables/' + tableName + '/index'
        else:
            resource = dbName + '/tables/' + tableName + '/chunks/' + str(chunkId) + '/index'

        result = self._requestJSON('dbs', resource, params=dict(columns=columns))
        return self._getKey(result, 'rows')

    def loadData(self, dbName, tableName, dataFile, fileName='table.data', chunkId=None, overlap=False,
                 compressed=None, delimiter=None, enclose=None, escape=None, terminate=None):
        """
        Upload data into a table or chunk using the file format supported by mysql
        command LOAD DATA [LOCAL] INFILE.

        @param dbName:     Database name
        @param tableName:  Table name
        @param dataFile:   File object for file with data, file must be open in binary
                           mode and return bytes in Python3 (or plain str in Python2)
        @param fileName:   File name to use, if data is compressed then pass name which
                           ends with .gz or set compressed argument to True.
        @param chunkId:    Chunk ID, number, if None then table is not partitioned
        @param overlap:    If True then load data in overlap table, only meaningful if
                           chunkId is not None
        @param compressed: If True then file is gzip-compressed, if False then
                           file is not compressed, if None then .gz extension means
                           that file is compressed
        @param delimiter:  Field delimiter in file, default is TAB
        @param enclose:    Field enclosing character, default is empty
        @param escape:     Escape character, default is backslash
        @param terminate:  Line termination character, default is newline

        @return: Number of rows added to a table.
        @raise ClientException: in case of problems
        """

        _log.debug('load data: %s.%s chunkId=%s overlap=%s', dbName, tableName, chunkId, overlap)

        # resource URL
        resource = dbName + '/tables/' + tableName
        if chunkId is None:
            resource += '/data'
        else:
            resource += '/chunks/' + str(chunkId)
            resource += '/overlap' if overlap else '/data'

        # options dictionary
        options = {}
        if delimiter is not None:
            options['delimiter'] = delimiter
        if enclose is not None:
            options['enclose'] = enclose
        if escape is not None:
            options['escape'] = escape
        if terminate is not None:
            options['terminate'] = terminate
        if compressed is not None:
            options['compressed'] = terminate

        # urlencode options
        options = urlencode(options)

        # we have to encode data in multi-part container and we also want to
        # stream the data from a file, this combination is not supported natively
        # in requests so we want to do special "stream" to produce the data
        data = []
        if options:
            data.append(dict(name='load-options', data=options,
                             content_type='application/x-www-form-urlencoded'))
        data.append(dict(name='table-data', data=dataFile, filename=fileName,
                         content_type='application/octet-stream'))
        stream = _MPEncoder(data)

        result = self._requestJSON('dbs', resource, method='POST', data=stream,
                                   headers={'Content-Type': stream.content_type})
        return self._getKey(result, 'count')

    def resetChunksCache(self, dbName):
        """
        Reset chunk cache (a.k.a. empty chunks list) for specified database name.

        @param dbName:     Database name
        @raise ClientException: in case of problems
        """

        _log.debug('reset chunk cache: %s', dbName)

        # resource URL
        resource = dbName + '/chunks/cache'

        result = self._requestJSON('dbs', resource, method='PUT')

    def services(self):
        """
        Return the list of service names.

        @raise ClientException: in case of problems
        """
        _log.debug('get service list')
        result = self._requestJSON('services', '')
        return self._getKey(result, 'name')

    def serviceState(self, service):
        """
        Return service state.

        This method returns string describing current service state, currently
        defined states are "active" and "stopped".

        @param service: Service name.

        @return: String, either "active" or "stopped"
        @raise ClientException: in case of problems
        """
        _log.debug('get service state: %s', service)
        result = self._requestJSON('services', service)
        return self._getKey(result, 'state')

    def serviceAction(self, service, action):
        """
        Execute action on a service.

        @param service: Service name.
        @param action: String, one of 'stop', 'start', 'restart'

        @return: New service state, either "active" or "stopped"
        @raise ClientException: in case of problems
        """
        _log.debug('service action: %s %s', service, action)
        data = dict(action=action)
        result = self._requestJSON('services', service, method='PUT', data=data)
        return self._getKey(result, 'state')

    def xrootdDbs(self):
        """
        Return the list of database names known to xrootd.

        @raise ClientException: in case of problems
        """
        _log.debug('get xrd db list')
        result = self._requestJSON('xrootd', 'dbs')
        return self._getKey(result, 'name')

    def xrootdRegisterDb(self, dbName, restart=True, allowDuplicate=False):
        """
        Register new database with xrootd.

        @param dbName:  New database name
        @param restart:  If true then xrootd will be restarted

        @raise ClientException: in case of problems
        """
        _log.debug('register database in xrootd: %s', dbName)
        data = dict(db=dbName, xrootdRestart=str(int(restart)))
        try:
            self._requestJSON('xrootd', 'dbs', method='POST', data=data)
        except ServerError as exc:
            # may need to ignore 409 if db is registered already
            if exc.code != 409 or not allowDuplicate:
                raise

    def xrootdUnregisterDb(self, dbName, restart=True):
        """
        Remove database from xrootd registration.

        @param dbName:  Database name
        @param restart:  If true then xrootd will be restarted

        @raise ClientException: in case of problems
        """
        _log.debug('unregister database from xrootd: %s', dbName)
        params = dict(xrootdRestart=str(int(restart)))
        self._requestJSON('xrootd', 'dbs/' + dbName, method='DELETE', params=params)

    def xrootdChunks(self, dbName):
        """
        Returns list of chunks for a given database which are known to xrootd.

        @raise ClientException: in case of problems
        """
        _log.debug('list chunks in xrootd: %s', dbName)
        result = self._requestJSON('xrootd', 'dbs/' + dbName, method='GET')
        return self._getKey(result, 'chunkId')

    # ----------  private methods ----------------

    def _baseURL(self):
        """
        Returns initial part of service URL.
        """
        return 'http://{0}:{1}'.format(self.host, self.port)

    def _request(self, svc, resource, method='GET', params=None, data=None, headers=None):
        """
        Execute HTTP request, return response object.

        @param svc:       Service name, one of "dbs", "services", "xrootd", etc.
        @param resource:  Resource name
        @param method:    HTTP method name
        @param params:    Request parameters, passed to requests library
        @param data:      Request data, passed to requests library
        @param headers:   Request headers, passed to requests library

        @return: requests.Response instance
        @raise CommunicationError: exception generated if communication with server failed
        @raise ServerError: exception generated if server sent any HTTP error code
        """
        url = self._baseURL() + '/' + svc
        if resource:
            url += '/' + resource
        try:
            response = requests.request(method, url, params=params, data=data, headers=headers,
                                        auth=self.auth)
            _log.debug('Received response: %s', response)
            _log.debug('Response body: %s', response.content)
            response.raise_for_status()
            return response
        except requests.HTTPError as exc:
            raise ServerError(exc.response.status_code, exc.response.text)
        except requests.exceptions.RequestException as exc:
            # RequestException is a base class for all exceptions generated
            # by requests including HTTPError
            raise CommunicationError(str(exc))

    def _requestJSON(self, svc, resource, method='GET', params=None, data=None, headers=None):
        """
        Execute HTTP request, decode response data as JSON object. Returns
        data from JSON object corresponding to "result" or "results" key.

        @param svc:       Service name, one of "dbs", "services", "xrootd", etc.
        @param resource:  Resource name
        @param method:    HTTP method name
        @param params:    Request parameters, passed to requests library
        @param data:      Request data, passed to requests library
        @param headers:   Request headers, passed to requests library

        @return: content of "result" or "results" key in JSON object
        @raise CommunicationError: exception generated if communication with server failed
        @raise ServerError: exception generated if server sent any HTTP error code
        @raise ServerResponseError: exception generated if server response is not JSON
        """
        response = self._request(svc, resource, method=method, params=params, data=data, headers=headers)
        try:
            jdata = response.json()
            _log.debug('JSON response: %s', jdata)
        except Exception as exc:
            raise ServerResponseError('JSON parsing failed', str(exc), response.text)

        # use our standard convention for result/results key
        result = jdata.get('result', _None)
        if result is not _None:
            return result
        result = jdata.get('results', _None)
        if result is _None:
            raise ServerResponseError("Missing 'result' or 'results' key", jdata)

        # check type
        if not isinstance(result, list):
            raise ServerResponseError("Unexpected type of 'results' key", type(result))

        return result

    @staticmethod
    def _getKey(result, key):
        """
        Returns key value in the result, throw exception if key is not found.
        Result can be a dict or a list of dicts, for list the returned value is
        the list of values.
        """
        try:
            if isinstance(result, list):
                return [obj[key] for obj in result]
            else:
                return result[key]
        except KeyError:
            raise ServerResponseError('Missing "%s" key' % key, result)
