#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2014 LSST Corporation.
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

@author  Jacek Becla, SLAC


Known issues and todos:
 * recover from lost connection by reconnecting
 * need to catch *all* exceptions that ZooKeeper and Kazoo might raise, see:
   http://kazoo.readthedocs.org/en/latest/api/client.html
 * issue: watcher is currently using the "_zk", and bypasses the official API!
"""

# standard library imports
import logging
import sys
import time

# third-party software imports
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

# local imports
from lsst.db.exception import produceExceptionClass
from lsst.qserv.czar import KvInterfaceImplMem

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
class CssCacheFactory(object):
    """
    @brief Constructs CssCache objects that contain snapshots of the
    Central State Service CSS).
    Maintains own current snapshot (modifiable) and a copy(Read-only,
    shared among clients).
    """
    def __init__(self, **kwargs):
        """
        Initialize the interface.

        @param connInfo  Connection information for zk mode
        or @param config  Config containing css config (technology,
        connection, timeout keys)

        self._filename : filename for css info.
        self._zk : a KazooClient object, valid "if not self._filename:"
        """
        self._logger = logging.getLogger("CSS")
        self._filename = None
        if ("connInfo" in kwargs) and kwargs["connInfo"]:
            self._logger.info("Zk connection is: %s" % kwargs["connInfo"])
            self._cfg = { "technology" : "zoo",
                          "connection" : kwargs["connInfo"],
                          "timeout" : 10000 }
            self._setupBackend(self._cfg)
        elif "config" in kwargs:
            self._cfg = kwags["config"]
            pass
        else:
            raise KvException(KvException.MISSING_PARAM, "<None>")
        self.refresh()

    def _setupBackend(self, cfg):
        if cfg["technology"] == "zoo":
            self._zk = KazooClient(
                hosts=cfg["connection"],
                # timeout in ms, kazoo expects seconds
                timeout=(cfg["timeout"]/1000.0))
            self._zk.start()
        elif cfg["technology"] == "mem":
            self._file = str(cfg["connection"])
            pass
        else:
            raise KvException(KvException.INVALID_CONNECTION, "<None>")

    @property
    def lastUpdated(self):
        """Return time of last update to the css cluster for a node.
        If the value is different from previous invocations, the
        caller may invoke refresh() to update the snapshot.

        @param path of the node.
        """
        if self._filename:
            stat = os.stat(self._filename)
            return stat.st_mtime
        # use zk
        data, stat = self._zk.get("/")
        return stat.last_modified

    def refresh(self):
        """Update the local css state snapshot.
        Note, the first call hardcodes the result of the mode check."""
        if self._filename:
            self.refresh = self.refreshFile
            return self.refreshFile()
        self.refresh = self.refreshZk
        return self.refreshZk()

    def refreshFile(self):
        """Refresh using self._file . Rereads full state from
        self._filename . There isn't a clear way to avoid reading the
        whole file, so we won't, seeing as this mode is primarily
        (exclusively?) for debugging.
        entire tree.
        """
        self.snapshot = KvInterfaceImplMem(self._file)

    def refreshZk(self):
        """Refresh using self._zk .Currently, pulls completely new
        state. In the future, it should check modification/creation
        times against previous state in order to avoid visiting the
        entire tree.
        """
        self.snapshot = self.getKvInterfaceZoo()

    def getSnapshot(self):
        """Make a copy of the local css state"""
        return self.snapshot.clone()

    def getKvInterfaceZoo(self):
        """Compute a new css state snapshot and return it."""
        kvi = KvInterfaceImplMem()
        class KviFactory:
            jsonsuffix = ".json"
            def __init__(self, kvi):
                self.kvi = kvi
                pass

            def isPacked(self, path):
                """@return length of ".json" if path node ends in
                .json. If a node has a .json suffix, this indicates
                that its data value is encoded in json. For example, a
                zookeeper node with path /foo/bar.json will have a
                json object as its data. If the contents are: { "name"
                : "John", "rank":"private"}, then an unpacking will
                yield logical paths: /foo/bar/name -> John,
                /foo/bar/rank -> private."""
                return path.endswith(self.jsonsuffix)

            def acceptFunc(self, path):
                return path != "/zookeeper"
            def dataFunc(self, path, data):
                if self.isPacked(path):
                    self.insertObj(path[:len(self.jsonsuffix)],
                                   json.loads(data))
                    pass
                else:
                    print "Creating kvi key: %s = %s" % (path, data)
                    self.kvi.create(str(path), str(data))
            def insertObj(self, path, obj):
                if isinstance(obj, list):
                    # For now, apply comma separation for list elements.
                    if not obj: self.kvi.create(path, "")
                    self.kvi.create(path, ",".join(map(str, obj)))
                elif isinstance(obj, dict):
                    p = path
                    if p.endswith("/"): p = path[:-1] # clip the /
                    for k in obj:
                        self.insertObj(path + "/" + k, obj[k])
                else: self.kvi.create(path, obj)
            pass
        kf = KviFactory(kvi)
        self._visit("/", kf.dataFunc, kf.acceptFunc)
        return kvi

    def dump(self):
        """Read the current central CSS state and dump it.
        @return line-delimited CSS state."""
        class NodePrinter:
            def __init__(self):
                self.entries = []
                pass
            def dataFunc(self, path, data):
                self.entries.append(
                    path + '\t'
                    + (data if data else '\N'))
            pass
        np = NodePrinter()
        self._visit("/", np.dataFunc,
                    lambda p: p != "/zookeeper")
        return "\n".join(np.entries)

    def _visit(self, p, dataNodeFunc, acceptFunc):
        """Visit remote css nodes. Call dataNodeFunc(path, data) on
        each node. For a node (and its children) to be visited,
        acceptFunc(path) must return true.

        For example, if
        acceptFunc("/") returns false and _visit begins at "/", no
        dataNodeFunc will not be called. Similarly, if
        acceptFunc("/foo") returns false, then _visit("/",...) will
        visit every node (and call dataNodeFunc) except /foo and its
        children.
        """
        ##print "visit path=", p
        if not acceptFunc(p): return
        children = None
        data = None
        stat = None
        try:
            children = self._zk.get_children(p)
            data, stat = self._zk.get(p)
            dataNodeFunc(p, data)
            if p == "/": p = "" # Prevent // in concatenated path.
            for child in children:
                self._visit(p + "/" + child, dataNodeFunc, acceptFunc)
        except NoNodeError:
            self._logger.warning("Caught NoNodeError, someone deleted node just now")
            None

class Test:
    """Test basic css module behavior"""

    def go(self):
        cf = CssCacheFactory(connInfo="localhost:2181")
        print "Dump of zk",cf.dump()
        mykvi = cf.getSnapshot()

def selftest():
    t = Test()
    t.go()



class DummyObject:
    def create(self, k, v='', sequence=False, ephemeral=False):
        """
        Add a new key/value entry. Create entire path as necessary.

        @param sequence  Sequence flag -- if set to True, a 10-digid, 0-padded
                         suffix (unique sequential number) will be added to the key.

        @return string   Real path to the just created node.

        @raise     KvException if the key k already exists.
        """
        self._logger.info("CREATE '%s' --> '%s', seq=%s, eph=%s" % \
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
        self._logger.info("EXISTS '%s': %s" % (k, ret))
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
            self._logger.info("GET '%s' --> '%s'" % (k, v))
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
            self._logger.info("GETCHILDREN '%s'" % (k))
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
            self._logger.info("SET '%s' --> '%s'" % (k, v))
            self._zk.set(k, v)
        except NoNodeError:
            self._logger.error("in set(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)

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
                            self._logger.info("DELETE '/%s'" % (child))
                            self._zk.delete("/%s" % child, recursive=True)
                else:
                    pass
            else:
                self._logger.info("DELETE '%s'" % (k))
                self._zk.delete(k, recursive=recursive)
        except NoNodeError:
            self._logger.error("in delete(), key %s does not exist" % k)
            raise KvException(KvException.KEY_DOES_NOT_EXIST, k)

    def dumpAll(self, fileH=sys.stdout):
        """
        Returns entire contents.
        """
        self._printNode("/", fileH)

    def getLockObject(self, k, id):
        """
        @param k         Key.
        @param id        Name to use for this lock contender. This can be useful
                         for querying to see who the current lock contenders are.

        @return lock object
        """
        self._logger.info("Getting lock '%s' on '%s'" % (id, k))
        lockK = "%s.LOCK" % k
        return self._zk.Lock(lockK, id)

    def _printNode(self, p, fileH=None):
        """
        Print content of one key/value to stdout. Note, this function is recursive.

        @param p  Path.
        """
        children = None
        data = None
        stat = None
        try:
            children = self._zk.get_children(p)
            data, stat = self._zk.get(p)
            if fileH is not None:
                fileH.write(p)
                fileH.write('\t')
                fileH.write((data if data else '\N'))
                fileH.write('\n')
            else:
                print p, '\t', (data if data else '\N')
            for child in children:
                if p == "/":
                    if child != "zookeeper":
                        self._printNode("%s%s" % (p, child), fileH)
                else:
                    self._printNode("%s/%s" % (p, child), fileH)
        except NoNodeError:
            self._logger.warning("Caught NoNodeError, someone deleted node just now")
            None

    def _deleteNode(self, p):
        """
        Delete one znode. Note, this function is recursive.

        @param p  Path.
        """
        try:
            children = self._zk.get_children(p)
            for child in children:
                if p == "/":
                    if child != "zookeeper": # skip "/zookeeper"
                        self._deleteNode("%s%s" % (p, child))
                else:
                    self._deleteNode("%s/%s" % (p, child))
            if p != "/":
                self._zk.delete(p)
        except NoNodeError:
            pass
