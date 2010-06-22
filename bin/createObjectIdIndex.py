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

    
    



