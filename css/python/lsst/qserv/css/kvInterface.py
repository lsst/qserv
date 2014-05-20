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
class KvInterface(object):
    """
    @brief KvInterface class defines interface to the Central State Service CSS).

    @param connInfo  Connection information.
    """

    def __init__(self, connInfo):
        """
        Initialize the interface.
        """
        self._logger = logging.getLogger("CSS")
        if connInfo is None:
            raise KvException(KvException.INVALID_CONNECTION, "<None>")
        self._logger.info("conn is: %s" % connInfo)
        self._zk = KazooClient(hosts=connInfo)
        self._zk.start()

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
