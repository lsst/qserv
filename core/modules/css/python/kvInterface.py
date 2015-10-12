#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2014 AURA/LSST.
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
This module implements interface to the Central State System (CSS).

@author  Jacek Becla, SLAC; Daniel L. Wang, SLAC


Known issues and todos:
 * recover from lost connection by reconnecting
 * need to catch *all* exceptions that ZooKeeper and Kazoo might raise, see:
   http://kazoo.readthedocs.org/en/latest/api/client.html
 * issue: watcher is currently using the "_zk", and bypasses the official API!
"""

# standard library imports
import logging
import json
import sys
import time

# third-party software imports
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from sqlalchemy.engine.url import make_url


# local imports
from lsst.db.exception import produceExceptionClass
from . import cssLib

def _parsConnInfo(connInfo):
    if '/' not in connInfo:
        # expect "host:port" for zookeeper
        if ':' not in connInfo:
            raise KvException(KvException.INVALID_CONNECTION, connInfo)
        return dict(technology='zoo', connection=connInfo, timeout=10000)

    # parse connection string
    try:
        url = make_url(connInfo)
    except Exception as exc:
            raise KvException(KvException.INVALID_CONNECTION, connInfo)

    cssConfig = {'technology': url.drivername}
    if url.host:
        cssConfig['hostname'] = url.host
    if url.port:
        cssConfig['port'] = url.port
    if url.username:
        cssConfig['username'] = url.username
    if url.password:
        cssConfig['password'] = url.password
    if url.database:
        cssConfig['database'] = url.database
    if url.query and 'unix_socket' in url.query:
        cssConfig['socket'] = url.query['unix_socket']
    return cssConfig


####################################################################################
KvException = produceExceptionClass('KvException', [
        (2010, "INVALID_CONNECTION", "Invalid connection information."),
        (2015, "KEY_EXISTS",         "Key already exists."),
        (2020, "KEY_DOES_NOT_EXIST", "Key does not exist."),
        (2025, "KEY_INVALID",        "Invalid key."),
        (2030, "MISSING_PARAM",      "Missing parameter."),
        (9998, "NOT_IMPLEMENTED",    "Feature not implemented yet."),
        (9999, "INTERNAL",           "Internal error.")])

####################################################################################

# JSON-coding/decoding
def encodePacked(aDict):
    """Encode a dict into a packed value suitable for inserting
    into a CSS kv store
    @return string representing the encoded dict (currently, a JSON obj)"""
    return json.dumps(aDict)

def decode(packed):
    """Decode a packed value in CSS (packed in JSON).
    @return dict representing the decoded input string.
    """
    return json.loads(packed)

# constants
jsonsuffix = ".json"

########################################################################
class KvInterface(object):
    """
    @brief KvInterface class defines interface to the Central State Service CSS).
    """
    @classmethod
    def newImpl(cls, **kwargs):
        """
        Initialize a KvInterface.

        Accepts one of the two keyword arguments, `connInfo` or `config`.
        If `connInfo` is present then it is treated the same as if
        `config=dict(technology='zoo', connection=connInfo, timeout=10000)`
        argument is provided.

        `config` is a dictionary containing all needed parameters, there is
        one required key "technology" in the dictionary, all other keys
        depend on the value of "technology" key. Here are possible values:
        - 'zoo': possible other keys:
            - connection: a string like "127.0.0.1:2121"
            - timeout: timeout value in milliseconds
        - 'mem': other keys:
            - connection: either a string representing a file name to read
              the data from or an instance of KvInterface
        - 'mysql': other keys (all optional):
            - hostname:  string with mysql server host name or IP address
            - port: integer port number of mysql server (could be string)
            - socket: unix socket name
            - username: mysql user name
            - password: user password
            - database: database name
        - 'fake': no extra keys
        """
        if ("connInfo" in kwargs) and kwargs["connInfo"]:
            cfg = _parsConnInfo(kwargs["connInfo"])
        elif "config" in kwargs:
            cfg = kwargs["config"]
        else:
            raise KvException(KvException.MISSING_PARAM, "<None>")

        if cfg["technology"] == "zoo":
            cfgClean = cfg.copy()
            cfgClean.pop("technology")
            return KvInterfaceZoo(**cfgClean)
        elif cfg["technology"] == "mysql":
            mycfg = cssLib.MySqlConfig()
            if 'hostname' in cfg: mycfg.hostname = cfg['hostname']
            if 'port' in cfg and cfg['port']: mycfg.port = int(cfg['port'])
            if 'socket' in cfg: mycfg.socket = cfg['socket']
            if 'username' in cfg: mycfg.username = cfg['username']
            if 'password' in cfg: mycfg.password = cfg['password']
            if 'database' in cfg: mycfg.dbName = cfg['database']
            kvi = cssLib.KvInterfaceImplMySql(mycfg)
            return KvInterfaceMem(kvi)
        elif cfg["technology"] == "mem":
            return KvInterfaceMem(cfg["connection"])
        else:
            raise KvException(KvException.MISSING_PARAM,
                              "technology key has unexpected value: " + cfg["technology"])

    @property
    def lastModified(self):
        """Return time of last update to the css cluster for a node.
        If the value is different from previous invocations, the
        caller may invoke refresh() to update the snapshot.

        @param path of the node.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def create(self, k, v='', sequence=False, ephemeral=False):
        """
        Add a new key/value entry. Create entire path as necessary.

        @param sequence  Sequence flag -- if set to True, a 10-digit, 0-padded
                         suffix (unique sequential number) will be added to the key.

        @return string   Real path to the just created node.

        @raise     KvException if the key k already exists.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def exists(self, k):
        """
        Check if a given key exists.

        @param k Key.

        @return boolean  True if the key exists, False otherwise.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def get(self, k):
        """
        Return value for a key.

        @param k   Key.

        @return string  Value for a given key.

        @raise     Raise KvException if the key doesn't exist.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def getChildren(self, k):
        """
        Return the list of the children of the node k.

        @param k   Key.

        @return    List_of_children of the node k.

        @raise     Raise KvException if the key does not exists.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def set(self, k, v):
        """
        Set value for a given key. Raise exception if the key doesn't exist.

        @param k  Key.
        @param v  Value.

        @raise     Raise KvException if the key doesn't exist.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def setForce(self, k, v):
        """
        Set value for a given key, overwriting the key's current value
        if it exists, and force-creating a new key otherwise.

        @param k  Key.
        @param v  Value.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def delete(self, k, recursive=False):
        """
        Delete a key, including all children if recursive flag is set.

        @param k         Key.
        @param recursive Flag. If set, all existing children nodes will be
                         deleted.

        @raise     Raise KvException if the key doesn't exist.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def dumpAll(self, fileH=sys.stdout):
        """
        Returns entire contents in a string.
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def getLockObject(self, k, id):
        """
        @param k         Key.
        @param id        Name to use for this lock contender. This can be useful
                         for querying to see who the current lock contenders are.

        @return lock object
        """
        raise KvException(KvException.NOT_IMPLEMENTED)

    def visitPrefix(self, p, nodeFunc, acceptFunc=lambda n: True):
        """Visit a path recursively, in prefix-order. Call nodeFunc(path)
        on each node. For a node (and its children) to be visited,
        acceptFunc(path) must return true. Parent paths are visited before
        their children.

        For example, if
        acceptFunc("/") returns false and _visit begins at "/", no
        nodeFunc will not be called. Similarly, if
        acceptFunc("/foo") returns false, then _visit("/",...) will
        visit every node (and call nodeFunc) except /foo and its
        children.
        """
        try:
            children = self.getChildren(p)
            nodeFunc(p)
            if p == "/": p = "" # Prevent // in concatenated path.
            for child in children:
                self.visitPrefix(p + "/" + child, nodeFunc, acceptFunc)
        except NoNodeError:
            self._logger.warning("Caught NoNodeError: accessed deleted node.")

    def visitPostfix(self, p, nodeFunc, acceptFunc=lambda n: True):
        """Visit a path recursively, in postfix-order. Call nodeFunc(path)
        on each node. For a node (and its children) to be visited,
        acceptFunc(path) must return true.

        In contrast to visitPrefix, parent paths are visited after their
        children.

        See visitPrefix for more details on nodeFunc and acceptFunc
        """
        try:
            children = self.getChildren(p)
            if p == "/": p = "" # Prevent // in concatenated path.
            for child in children:
                self.visitPostfix(p + "/" + child, nodeFunc, acceptFunc)
            nodeFunc(p)
        except NoNodeError:
            self._logger.warning("Caught NoNodeError, someone deleted node just now")

    def isPacked(self, path):
        """@return path-root for unpacking if path indicates a packed value.
        Otherwise return None.

        Currently, if a path has a .json suffix, this indicates that its data
        value is encoded in json.

        For example, a zookeeper node with path /foo/bar.json will
        have a json object as its data. If the contents are: { "name"
        : "John", "rank":"private"}, then an unpacking will yield
        logical paths: /foo/bar/name -> John, /foo/bar/rank ->
        private."""
        if path.endswith(jsonsuffix):
            return path[:-len(jsonsuffix)]
        return None

    def getUnpacked(self, path):
        """
        @param path path to the packed key (i.e. /some/path/key.json )
        @return a dict containing the keys and values stored at a path
        """
        return decode(self.get(path))


    class Printer:
        """A helper class for printing values from KvInterface objects"""
        def __init__(self, kv):
            self.lines = []
            self.kv = kv
        def visit(self, p):
            data = self.kv.get(p)
            self.lines.append("%s\t%s" % (p, data))
        pass


    pass

########################################################################
# KvInterfaceZoo: Zk-based interface for KvInterface
########################################################################
class KvInterfaceZoo(KvInterface):
    """
    @brief KvInterfaceZoo class implements a zk-backed key-value store
    for Qserv CSS.
    """

    def __init__(self, connection, timeout):
        """
        Initialize the interface.

        @param connection  "host:port" of zk instance
                           or instances "host1:port1,host2:port2,..."
        @param timeout retry timeout in millisecond
        """
        self._logger = logging.getLogger("CSS")
        if not connection:
            raise KvException(KvException.INVALID_CONNECTION, "<None>")

        self._zk = KazooClient(hosts=connection,
                               # timeout in ms, kazoo expects seconds
                               timeout=(float(timeout)/1000.0))
        self._zk.start()

    @property
    def lastModified(self):
        """Return time of last update to the css cluster for a node.
        If the value is different from previous invocations, the
        caller may invoke refresh() to update the snapshot.

        @param path of the node.
        """
        if self._filename:
            stat = os.stat(self._filename)
            return stat.st_mtime
        # raising exception because the code below is wrong.
        # The last_modified for node "/" reflects modification time
        # of that node *only*, and not the underlying children nodes.
        raise KvException(KvException.NOT_IMPLEMENTED)

        # use zk
        data, stat = self._zk.get("/")
        return stat.last_modified

    def create(self, k, v='', sequence=False, ephemeral=False):
        """
        Add a new key/value entry. Create entire path as necessary.

        @param sequence  Sequence flag -- if set to True, a 10-digit, 0-padded
                         suffix (unique sequential number) will be added to the key.

        @return string   Real path to the just created node.

        @raise     KvException if the key k already exists.
        """
        self._logger.debug("CREATE '%s' --> '%s', seq=%s, eph=%s" % \
                              (k, v, sequence, ephemeral))
        try:
            return self._zk.create(k, v, sequence=sequence,
                                   ephemeral=ephemeral, makepath=True)
        except NodeExistsError:
            self._logger.error("in create(), key %s exists" % k)
            raise KvException(KvException.KEY_EXISTS, k)

    def exists(self, k):
        """
        Check if a given key exists.

        @param k Key.

        @return boolean  True if the key exists, False otherwise.
        """
        ret = (self._zk.exists(k) != None)
        self._logger.debug("EXISTS '%s': %s" % (k, ret))
        return ret

    def get(self, k):
        """
        Return value for a key.

        @param k   Key.

        @return string  Value for a given key.

        @raise     Raise KvException if the key doesn't exist.
        """
        try:
            v, stat = self._zk.get(k)
            self._logger.debug("GET '%s' --> '%s'" % (k, v))
            return v
        except NoNodeError:
            self._logger.error("in get(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)

    def getChildren(self, k):
        """
        Return the list of the children of the node k.

        @param k   Key.

        @return    List_of_children of the node k.

        @raise     Raise KvException if the key does not exists.
        """
        try:
            self._logger.debug("GETCHILDREN '%s'" % (k))
            return self._zk.get_children(k)
        except NoNodeError:
            self._logger.error("in getChildren(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)

    def set(self, k, v):
        """
        Set value for a given key. Raise exception if the key doesn't exist.

        @param k  Key.
        @param v  Value.

        @raise     Raise KvException if the key doesn't exist.
        """
        try:
            self._logger.debug("SET '%s' --> '%s'" % (k, v))
            self._zk.set(k, v)
        except NoNodeError:
            self._logger.error("in set(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)

    def setForce(self, k, v):
        """
        Set value for a given key, overwriting the key's current value
        if it exists, and force-creating a new key otherwise.

        @param k  Key.
        @param v  Value.
        """
        self._logger.debug("SETFORCE '%s' --> '%s'" % (k, v))
        try:
            self._zk.set(k, v)
        except NoNodeError:
            self._zk.create(k, v)

    def delete(self, k, recursive=False):
        """
        Delete a key, including all children if recursive flag is set.

        @param k         Key.
        @param recursive Flag. If set, all existing children nodes will be
                         deleted.

        @raise     Raise KvException if the key doesn't exist.
        """
        try:
            if k == "/": # zookeeper will fail badly if we try to delete root node
                if recursive:
                    for child in self.getChildren("/"):
                        if child != "zookeeper": # skip zookeeper internals
                            self._logger.debug("DELETE '/%s'" % (child))
                            self._zk.delete("/%s" % child, recursive=True)
                else:
                    pass
            else:
                self._logger.debug("DELETE '%s'" % (k))
                self._zk.delete(k, recursive=recursive)
        except NoNodeError:
            self._logger.error("in delete(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)

    def dumpAll(self, fileH=sys.stdout):
        """
        Returns entire contents.
        """
        contents = self._printNode("/")
        if fileH:
            fileH.write(contents)
        else:
            print contents

    def getLockObject(self, k, id):
        """
        @param k         Key.
        @param id        Name to use for this lock contender. This can be useful
                         for querying to see who the current lock contenders are.

        @return lock object
        """
        self._logger.debug("Getting lock '%s' on '%s'" % (id, k))
        lockK = "%s.LOCK" % k
        return self._zk.Lock(lockK, id)

    def _printNode(self, p, fileH=None):
        """
        Print content of one key/value to stdout. Note, this function is recursive.

        @param p  Path.
        """
        printer = KvInterface.Printer(self)
        self.visitPrefix(p, printer.visit,
                         lambda p: not p.startswith("/zookeeper"))
        return "\n".join(printer.lines)

    def _deleteNode(self, p):
        """
        Delete one path and its children.

        @param p  Path.
        """
        # Leverage recursion provided by zk-persistence
        try:
            if p == "/":
                children = self._zk.get_children(p)
                for child in ifilter(lambda c: c != "zookeeper", children):
                    self._zk.delete("%s%s" % (p, child), recursive=True)
            else:
                self._zk.delete(p, recursive=True)
        except NoNodeError:
            self._logger.warning("KvInterfaceZoo._deleteNode(%s), key does not exist" % k)

########################################################################
# KvInterfaceMem: in-memory impl of KvInterface, pre-loaded with a file
########################################################################
class KvInterfaceMem(KvInterface):
    """
    @brief KvInterfaceMem class implements a file-initialized
    key-value store for Qserv CSS.

    """
    def __init__(self, filename):
        """
        Initialize the interface.

        @param filename backing file (type == 'str'), or C++ Kvi
        """
        self._logger = logging.getLogger("CSS")
        if filename:
            if type(filename) == type(""):
                self.load(filename)
            else:
                self._filename = "UnderlyingC++Kvi"
                self._kvi = filename

    def load(self, filename):
        self._filename = filename
        self._kvi = cssLib.KvInterfaceImplMem(self._filename)

    @property
    def lastModified(self):
        """Return time of last update to the css cluster for a node.
        If the value is different from previous invocations, the
        caller may invoke refresh() to update the snapshot.

        @param path of the node.
        """
        stat = os.stat(self._filename)
        return stat.st_mtime

    def create(self, k, v='', sequence=False, ephemeral=False):
        """
        Add a new key/value entry. Create entire path as necessary.

        @return string   Real path to the just created node.

        @raise     KvException if the key k already exists.
        """
        if not sequence and self._kvi.exists(k):
            self._logger.error("in create(), key %s exists" % k)
            raise KvException(KvException.KEY_EXISTS, k)
        else:
            return self._kvi.create(k, v, bool(sequence))

    def exists(self, k):
        """
        Check if a given key exists.

        @param k Key.

        @return boolean  True if the key exists, False otherwise.
        """
        ret = self._kvi.exists(k)
        self._logger.debug("EXISTS '%s': %s" % (k, ret))
        return ret

    def get(self, k):
        """
        Return value for a key.

        @param k   Key.

        @return string  Value for a given key.

        @raise     Raise KvException if the key doesn't exist.
        """
        try:
            v = self._kvi.get(k)
            self._logger.debug("GET '%s' --> '%s'" % (k, v))
            return v
        except cssLib.NoSuchKey:
            self._logger.error("in get(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)

    def getChildren(self, k):
        """
        Return the list of the children of the node k.

        @param k   Key.

        @return    List_of_children of the node k.

        @raise     Raise KvException if the key does not exists.
        """
        try:
            self._logger.debug("GETCHILDREN '%s'" % (k))
            return self._kvi.getChildren(k)
        except cssLib.NoSuchKey:
            self._logger.error("in getChildren(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)

    def set(self, k, v):
        """
        Set value for a given key. Raise exception if the key doesn't exist.

        @param k  Key.
        @param v  Value.

        @raise     Raise KvException if the key doesn't exist.
        """
        self._logger.debug("SET '%s' --> '%s'" % (k, v))
        if not self._kvi.exists(k):
            self._logger.error("in getChildren(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)
        self._kvi.set(k, v)

    def setForce(self, k, v):
        """
        Set value for a given key, overwriting the key's current value
        if it exists, and force-creating a new key otherwise.

        @param k  Key.
        @param v  Value.
        """
        self._logger.debug("SETFORCE '%s' --> '%s'" % (k, v))
        self._kvi.set(k, v)

    def delete(self, k, recursive=False):
        """
        Delete a key, including all children if recursive flag is set.

        @param k         Key.
        @param recursive Flag. If set, all existing children nodes will be
                         deleted.

        @raise     Raise KvException if the key doesn't exist.
        """
        if recursive:
            self.visitPostfix(k, lambda p: self._kvi.deleteKey(p))
        else:
            self._kvi.deleteKey(k)

    def dumpAll(self, fileH=sys.stdout):
        """
        Returns entire contents.
        """
        contents = self._printNode("/")
        if fileH:
            fileH.write(contents)
        else:
            print contents

    def getLockObject(self, k, id):
        """
        @param k         Key.
        @param id        Name to use for this lock contender. This can be useful
                         for querying to see who the current lock contenders are.

        @return lock object
        """
        self._logger.debug("Getting lock '%s' on '%s' (in-mem, NOP)" % (id, k))
        return self

    def _printNode(self, p, fileH=None):
        """
        Print content of one key/value to stdout. Note, this function is recursive.

        @param p  Path.
        """
        printer = KvInterface.Printer(self)
        self.visitPrefix(p, printer.visit, lambda p: True)
        return "\n".join(printer.lines)

    def _deleteNode(self, p):
        """
        Delete one znode. Note, this function is recursive.

        @param p  Path.
        """
        # May need to catch c++ exceptions
        self.visitPostfix(p, self._kvi.deleteKey, lambda p: True)

    def __enter__(self):
        """
        This instance can be used as a context manager, have to provide
        standard method which is no-op.
        """
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This instance can be used as a context manager, have to provide
        standard method which is no-op.
        """
        pass
