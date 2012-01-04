# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

# createObjectIdIndex.py creates an ObjectId-to-ChunkId table for use
# with qserv.  It is invoked on a csv file that has a chunkId column
# and writes into a MySQL table.
#
# This code is in a "barely works" level of development and should be
# enhanced/rewritten soon. 

from itertools import imap
import os
import sys

objIdCol = 0
chunkIdCol = 10
file = "/u1/lsst/st99758/object_c.csv"

def tupleGen(col1, col2, iterable):
    for l in iterable:
        t = l.split(",")
        print "size",len(t)
        yield t[col1] + "," + t[col2]
    
def prepareFile(objIdCol, chunkIdCol, theFile, tmpLoc):
    tmpfile = os.path.join(tmpLoc, "tmp_objid_" + hex(hash(theFile)))
    g = tupleGen(objIdCol, chunkIdCol, open(theFile))
    out = open(tmpfile,"w")
    map(out.write, g)
    return (tmpfile)

def tryIt():
    print "using file", sys.argv[1]
    print prepareFile(0, 223, sys.argv[1], "/dev/shm/")

def makeCmd(tmpLoc, db, table, chunkId):
    tmpFile = os.path.join(tmpLoc, hex(hash(db+table+str(chunkId))))
    colList = ["objectId"]
    cmdParts = ["SELECT ",
                ",".join(colList),
                " INTO OUTFILE '",
                tmpFile,
                "' FIELDS TERMINATED BY ',' FROM ",
                "%s.%s_%d" % (db, table, chunkId),
                ";"]
    cmd = "".join(cmdParts)
    return (cmd, tmpFile, chunkId)

dropSql = "DROP TABLE ObjectChunkIndex;"
createSql = "CREATE TABLE ObjectChunkIndex (objectId bigint, chunkId int);"
loadTemplate = 'LOAD DATA INFILE "%s" INTO TABLE ObjectChunkIndex;'
finishSql = "ALTER TABLE ObjectChunkIndex ADD INDEX objIdIdx (objectId);"
files = []
files.append(makeCmd("/dev/shm/indTemp", "LSST", "Object", 107))
mysqlcmds = "\n".join([x[0] for x in files])
open("mysqlextract.sql","w").write(mysqlcmds)
os.system("mysql < mysqlextract.sql")
#sys.exit(0)
ll = []
for (c,t,cid) in files:
    ll.extend(["%d\t%d" % (int(l), cid) for l in open(t)])
    os.unlink(t)
targetCsv="/tmp/newindex.csv"
try:
    os.unlink(targetCsv)
except:
    pass
open(targetCsv,"w").write("\n".join(ll))
#sys.exit(0)
os.system("echo \"%s\" | mysql LSST" % (dropSql + createSql))
#print "echo \'%s\' | mysql LSST" % loadTemplate % targetCsv
os.system("echo \'%s\' | mysql LSST" % loadTemplate % targetCsv)
os.system("echo \'%s\' | mysql LSST" % finishSql)

    
    



