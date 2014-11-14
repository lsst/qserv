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
import json
import sys
import time

# third-party software imports
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

# local imports
from lsst.db.exception import produceExceptionClass
from lsst.qserv.czar import KvInterface, KvInterfaceImplMem

def getSnapshot(kvi):
    pass


class Unpacker:
    jsonsuffix = ".json"
    def __init__(self, target):
        """accepts css.python KvInterface instance"""
        self.target = target
        pass

    def readTree(self, inputKvi):
        def acceptFunc(self, path):
            return path != "/zookeeper"

        def nodeFunc(path):
            packRoot = kvi.isPacked(path):
                obj = kvi.getUnpacked(path)
                self.insertObj(path[:-len(self.jsonsuffix)], obj)
            else:
                print "Creating kvi key: %s = %s" % (path, data)
                self.target.set(str(path), str(data))
        inputKvi.visitPrefix("/", nodeFunc, acceptFunc)

    def insertObj(self, path, obj):
        """Save an object into the kvi. This is equivalent to
        self.kvi.create if the object is primitive (e.g., a
        string). If the object is non-primitive, (e.g., it is
        a list or a dict), then expand it and insert its
        elements."""
        if isinstance(obj, list):
            # For now, apply comma separation for list elements.
            if not obj: # Empty list
                self.target.create(path, "")
            else:
                self.target.create(path, ",".join(map(str, obj)))
        elif isinstance(obj, dict):
            p = path
            if p.endswith("/"): p = path[:-1] # clip the /
            self.ensureKey(path)
            for k in obj:
                self.insertObj(path + "/" + k, str(obj[k]))
            else: self.target.set(str(path), str(obj))

    def ensureKey(self, path):
        """Ensure that a self.kvi.exists(path) evaluates to
        true. The entry is inserted with an empty string if it
        does not already exists."""
        if not self.target.exists(str(path)):
            self.target.create(str(path), "")
        pass
class Snapshot(object):
    """
    @brief Constructs CssCache objects that contain snapshots of the
    Central State Service CSS).
    Maintains own current snapshot (modifiable) and a copy(Read-only,
    shared among clients).
    """
    def __init__(self, kvi):
        """
        Create a snapshot of the specified kvi.
        """
        self.snapshot = KvInterfaceImplMem()
        u = Unpacker(self.snapshot)
        u.readTree(kvi)

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


class FakeStat:
    def __init__(self, lastModified):
        self.lastModified = lastModified
        pass
    pass

class FakeZk:
    def __init__(self):
        dummyStat = FakeStat(lastModified=time.time())
        dummyData = ("", dummyStat)
        def makeFakeData(txt):
            return (txt, dummyStat)
        recipe = json.dumps({ "name":"Apple Pie",
                              "cost":"$200",
                              "generation":3,
                              })
        lockData = json.dumps({"password" : "123password",
                               "lastSet" : time.time() })

        self.getDict = {
            "/" : dummyData,
            "/alice" : dummyData,
            "/bob" : dummyData,
            "/eve" : dummyData,
            "/alice/secret" : makeFakeData("My dog has fleas"),
            "/alice/secret.json" : makeFakeData(recipe),
            "/eve/LOCK" : dummyData,
            "/eve/LOCK.json" : makeFakeData(lockData)
            }
        self.getChildDict = {
            "/" : "alice bob eve".split(),
            "/alice" : "secret secret.json".split(),
            "/eve" : "LOCK LOCK.json".split()
            }
        pass
    def get(self, key):
        return self.getDict[key]
    def get_children(self, key):
        return self.getChildDict.get(key, [])

class Test:
    """Test basic css module behavior"""

    def testFake(self):
        fakeZk = FakeZk()
        config = { "technology" : "fake",
                   "connection" : fakeZk}
        cf = CssCacheFactory(config=config)
        print cf.dump()
        mykvi = cf.getSnapshot()
        getFunc = KvInterface.get
        assert getFunc(mykvi, "/alice/secret") == "My dog has fleas"
        assert getFunc(mykvi, "/eve/LOCK/password") == "123password"

    def tryZkConstruct(self):
        cf = CssCacheFactory(connInfo="localhost:2181")
        print "Dump of zk",cf.dump()
        mykvi = cf.getSnapshot()

    def go(self):
        self.tryZkConstruct()
        self.testFake()

def selftest():
    t = Test()
    t.go()

