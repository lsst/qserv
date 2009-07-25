import MySQLdb as sql

class TinySql:
    def __init__(self):
        self.connection = sql.connect()
        self.cursor = self.connection.cursor()
    def execUnary(self, q):
        self.cursor.execute(q)
        return self.cursor.fetchall()[0][0]

    def getPartitions(self, field, count, tablename):
        # Find out ranges 
        maxv = self.execUnary("SELECT max(%s) FROM %s;" % (field, tablename))
        minv = self.execUnary("SELECT min(%s) FROM %s;" % (field, tablename))

        # Uniformly split range
        block = (maxv - minv) / count
        parts = [minv-1] 
        parts += [minv + (x*block) for x in range(1,count)] 
        parts += [maxv+1]
        return parts

    def insertRows(self, spec, srcTable, targetPrefix):
        # Unwrap
        (chunkId, subchunkId, ramin, ramax, declmin, declmax) = spec
        tblName = "%s_%d" % (targetPrefix, chunkId)
        tsql = "CREATE TABLE IF NOT EXISTS %s LIKE %s;" % (tblName, srcTable)
        csql = "ALTER TABLE %s ADD COLUMN subchunkId INT;" % tblName
        self.cursor.execute(tsql)
        self.cursor.fetchall() # Ignore output
        self.cursor.execute(csql)  # May fail
        self.cursor.fetchall()
        insqltemp = "INSERT INTO %s SELECT *,%d FROM %s;" 
        insql = insqltemp % (tblName, subchunkId, srcTable)
        self.cursor.execute(insql)
    def makePartMap(self, tuples):
        tblName = "test.partmap1"
        csql = """CREATE TABLE %s IF NOT EXISTS (
        chunkId INT, 
        subchunkId INT, 
        ramin FLOAT
        ramax FLOAT,
        declmin FLOAT,
        declmax FLOAT);""" % tblName
        self.cursor.execute(csql)
        for t in tuples:
            tstr = ",".join([str(v) v for v in list(t)])
            isql = "INSERT INTO %s VALUES (%s);" % (tblName, tstr)
            self.cursor.execute(isql)
            self.cursor.fetchall() # Drop result
                   

def split():
    ra_chunks = 2
    ra_sub = 2 
    decl_chunks = 4
    decl_sub = 3
    inputtable = "LSST.cat"
    s = TinySql()
    
    decParts = s.getPartitions("decl", decl_chunks * decl_sub, inputtable)
    raParts = s.getPartitions("ra", ra_chunks * ra_sub, inputtable)

    partTuples = []

    # chunkId and subchunkId numbered from 1+
    for rap in range(ra_chunks):
        for declp in range(decl_chunks):
            chunkId = 1 + (rap * decl_chunk) + declp
            for ras in range(ra_sub):
                for decls in range(decl_sub):
                    subChunkId = (ras * decl_sub) + decls
                    partMapTuple = (chunkId, subchunkId, 
                                    raParts[ras + (rap*ra_sub)],
                                    raParts[1+ ras + (rap*ra_sub)],
                                    declParts[decls + (declp*decl_sub)],
                                    declParts[1+ decls + (declp*decl_sub)])
                    partTuples.append(partMapTuple)
                    s.insertRows(partMapTuple, inputtable)
    s.makePartMap(partTuples)
    pass
