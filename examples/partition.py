import MySQLdb as sql


class TinySql:
    def __init__(self):
        self.connection = sql.connect()
        self.cursor = self.connection.cursor()
        self.parentDbName = "LSSTx"

    def execUnary(self, q):
        self.cursor.execute(q)
        return self.cursor.fetchall()[0][0]

    def getPartitions(self, field, count, tablename):
        fullName = self.parentDbName + "." + tablename
        # Find out ranges 
        maxv = self.execUnary("SELECT max(%s) FROM %s;" % (field, fullName))
        minv = self.execUnary("SELECT min(%s) FROM %s;" % (field, fullName))

        # Uniformly split range
        block = (maxv - minv) / count
        parts = [minv-1] 
        parts += [minv + (x*block) for x in range(1,count)] 
        parts += [maxv+1]
        return parts

    def defineChunkTable(self, chunkId, srcTable, targetTablePrefix):
        tblName = "%s.%s_%d" % (self.parentDbName,
                                targetTablePrefix, chunkId)
        srcTableName = "%s.%s" % (self.parentDbName, srcTable)
        tsql = "CREATE TABLE IF NOT EXISTS %s LIKE %s;" % (tblName,
                                                           srcTableName)
        csql = "ALTER TABLE %s ADD COLUMN subchunkId INT;" % tblName
        print "Inserting into table:", tsql
        self.cursor.execute(tsql)
        self.cursor.fetchall() # Ignore output
        try:
            self.cursor.execute(csql)  # May fail
            self.cursor.fetchall()
        except:
            pass ## silence Alter errors.
        srcName = srcTableName
        destName = tblName
        return (srcName, destName)
        
    def insertRows(self, spec, srcName, destName):
        (chunkId, subchunkId, ramin, ramax, declmin, declmax) = spec

        insqltemp = "INSERT INTO %s SELECT *,%d FROM %s where ra between %s and %s and decl between %s and %s;" 
        insql = insqltemp % (destName, subchunkId, srcName,
                             ramin, ramax, declmin, declmax)
        try:
            #print insql
            self.cursor.execute(insql)
            self.cursor.fetchall() ## drop results
        except:
            pass # fail silently now.
    def makePartMap(self, tuples):
        tblName = "test.partmap3"
        csql = """CREATE TABLE IF NOT EXISTS %s(
        chunkId INT, 
        subchunkId INT, 
        ramin FLOAT,
        ramax FLOAT,
        declmin FLOAT,
        declmax FLOAT);""" % tblName
        print "making part table", csql
        createSql = [csql]
        self.cursor.execute(csql)
        for t in tuples:
            tstr = ",".join([str(v) for v in list(t)])
            isql = "INSERT INTO %s VALUES (%s);" % (tblName, tstr)
            #print "Executing ",isql
            createSql.append(isql)
            self.cursor.execute(isql)
            self.cursor.fetchall() # Drop result
        
        return "\n".join(createSql)
                   

def split():
    if True:
        ra_chunks = 24
        ra_sub = 4 
        decl_chunks = 4
        decl_sub = 3
    else:
        ra_chunks = 3
        ra_sub = 1 
        decl_chunks = 4
        decl_sub = 3
        
    inputtable = "cat"
    s = TinySql()
    
    declParts = s.getPartitions("decl", decl_chunks * decl_sub, inputtable)
    raParts = s.getPartitions("ra", ra_chunks * ra_sub, inputtable)

    partTuples = []

    # chunkId and subchunkId numbered from 1+
    for rap in range(ra_chunks):
        for declp in range(decl_chunks):
            chunkId = 1 + (rap * decl_chunks) + declp
            tblNames = s.defineChunkTable(chunkId, inputtable, "Object")
            for ras in range(ra_sub):
                for decls in range(decl_sub):
                    subChunkId = (ras * decl_sub) + decls
                    partMapTuple = (chunkId, subChunkId, 
                                    raParts[ras + (rap*ra_sub)],
                                    raParts[1+ ras + (rap*ra_sub)],
                                    declParts[decls + (declp*decl_sub)],
                                    declParts[1+ decls + (declp*decl_sub)])
                    partTuples.append(partMapTuple)
                    s.insertRows(partMapTuple, tblNames[0], tblNames[1])
    csql = s.makePartMap(partTuples)
    filename = "partmapsql.sql"
    print "writing sql for partmap to ", filename
    open(filename,"w").write(csql)
    pass

if __name__=="__main__":
    print "okay, going to start"
    split()



## HOWTO: cleanup LSST.Object_*  and rebuild from LSST.cat
## echo "use LSST; show tables;" | mysql | grep Object | sed 's/\(.*\)/drop table \1;/' | mysql LSST
## python partition.py


