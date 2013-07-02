# 
# LSST Data Management System
# Copyright 2009-2013 LSST Corporation.
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

# metadata module for lsst.qserv.master
#
# The metadata module contains functions, constants, and data related
# strictly to qserv's runtime metadata.  This includes the objectId
# index (secondary indexing) and the empty chunks file(s). This needs
# to be re-thought, now that the Qms is available. 

# Pkg imports
import config
import string

class Runtime:
    """The in-memory class for containing general Qserv frontend metadata. 
    """
    def __init__(self):
        self.metaDbName = config.config.get("mgmtdb", "db")
        self.emptyChunkInfo = {}
        self.defaultEmptyChunks = config.config.get("partitioner", 
                                        "emptyChunkListFile")
        print "Using %s as default empty chunks file." % (self.defaultEmptyChunks)
        self.emptyChunkInfo[""] = self.loadIntsFromFile(self.defaultEmptyChunks)
        pass

    def populateEmptyChunkInfo(self, dbName):
        """Populate self.emptyChunkInfo[dbName] with a set() containing 
        chunkIds for the empty chunks of the particular db. If no empty 
        chunk information can be found and loaded for the db, a default 
        empty chunks file is used."""
        sanitizedDbName = filter(lambda c: (c in string.letters) or
                                 (c in string.digits) or (c in ["_"]), 
                                 dbName)
        if sanitizedDbName != dbName:
            print "WARNING, dbName=", dbName,
            print "contains questionable characters. sanitized=",
            print sanitizedDbName
        name = "empty_%s.txt" % sanitizedDbName
        info = self.loadIntsFromFile(name)
        if info == None:
            print "Couldn't find %s, using %s." % (name, 
                                                   self.defaultEmptyChunks)
            self.emptyChunkInfo[dbName] = self.emptyChunkInfo[""]
        else:
            self.emptyChunkInfo[dbName] = info
        pass
    
    def loadIntsFromFile(self, filename):
        """Return a set() of ints from a file that is assumed to contain 
        ASCII-represented integers delimited with newline characters.
        @return set of ints, or None on any error.
        """
        def tolerantInt(i):
            try:
                return int(i)
            except:
                return None
            empty = set()
        try:
            if filename:
                s = open(filename).read()
                empty = set(map(tolerantInt, s.split("\n")))
        except:
            print filename, "not found while loading empty chunks file."
            return None
        return empty

        
# Module data
_myRuntime = None

# External interface
def getIndexNameForTable(tableName):
    """@return name of index table for @param tableName"""
    global _myRuntime
    if not _myRuntime: _myRuntime = Runtime()
    return _myRuntime.metaDbName + "." + tableName.replace(".","__")

def getMetaDbName():
    """@return the name of database containing the index tables
    (for objectId index tables)"""
    global _myRuntime
    if not _myRuntime: _myRuntime = Runtime()
    return _myRuntime.metaDbName 

def getEmptyChunks(dbName):
    """@return a set containing chunkIds (int) that are empty for a 
    given qserv dbName.
    """
    global _myRuntime
    if not _myRuntime: _myRuntime = Runtime()
    if dbName not in _myRuntime.emptyChunkInfo:
        _myRuntime.populateEmptyChunkInfo(dbName)
    return _myRuntime.emptyChunkInfo[dbName]

