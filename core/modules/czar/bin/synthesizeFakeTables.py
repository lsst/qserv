#!/usr/bin/env python

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


# synthesizeFakeTables.py is intended to fill-out a set of
# complementary tables from an installed, partitioned set of Object
# tables.
# This was intended to build a richer data set for testing qserv
# functionality using only USNO-B's object data.
import getpass
from itertools import imap
import operator
from optparse import OptionParser
import random
import MySQLdb as sql
import sys
import time

dropSourceSql = "DROP TABLE IF EXISTS %s.%s"
createSourceSql = """CREATE TABLE IF NOT EXISTS %s.%s
(
	sourceId BIGINT NOT NULL,
	ampExposureId BIGINT NULL,
	filterId TINYINT NOT NULL,
	objectId BIGINT NULL,
	movingObjectId BIGINT NULL,
	procHistoryId INTEGER NOT NULL,
	ra DOUBLE NOT NULL,
	raErrForDetection FLOAT(0) NULL,
	raErrForWcs FLOAT(0) NOT NULL,
	decl DOUBLE NOT NULL,
	declErrForDetection FLOAT(0) NULL,
	declErrForWcs FLOAT(0) NOT NULL,
	xFlux DOUBLE NULL,
	xFluxErr FLOAT(0) NULL,
	yFlux DOUBLE NULL,
	yFluxErr FLOAT(0) NULL,
	raFlux DOUBLE NULL,
	raFluxErr FLOAT(0) NULL,
	declFlux DOUBLE NULL,
	declFluxErr FLOAT(0) NULL,
	xPeak DOUBLE NULL,
	yPeak DOUBLE NULL,
	raPeak DOUBLE NULL,
	declPeak DOUBLE NULL,
	xAstrom DOUBLE NULL,
	xAstromErr FLOAT(0) NULL,
	yAstrom DOUBLE NULL,
	yAstromErr FLOAT(0) NULL,
	raAstrom DOUBLE NULL,
	raAstromErr FLOAT(0) NULL,
	declAstrom DOUBLE NULL,
	declAstromErr FLOAT(0) NULL,
	raObject DOUBLE NULL,
	declObject DOUBLE NULL,
	taiMidPoint DOUBLE NOT NULL,
	taiRange FLOAT(0) NULL,
	psfFlux DOUBLE NOT NULL,
	psfFluxErr FLOAT(0) NOT NULL,
	apFlux DOUBLE NOT NULL,
	apFluxErr FLOAT(0) NOT NULL,
	modelFlux DOUBLE NOT NULL,
	modelFluxErr FLOAT(0) NOT NULL,
	petroFlux DOUBLE NULL,
	petroFluxErr FLOAT(0) NULL,
	instFlux DOUBLE NOT NULL,
	instFluxErr FLOAT(0) NOT NULL,
	nonGrayCorrFlux DOUBLE NULL,
	nonGrayCorrFluxErr FLOAT(0) NULL,
	atmCorrFlux DOUBLE NULL,
	atmCorrFluxErr FLOAT(0) NULL,
	apDia FLOAT(0) NULL,
	Ixx FLOAT(0) NULL,
	IxxErr FLOAT(0) NULL,
	Iyy FLOAT(0) NULL,
	IyyErr FLOAT(0) NULL,
	Ixy FLOAT(0) NULL,
	IxyErr FLOAT(0) NULL,
	snr FLOAT(0) NOT NULL,
	chi2 FLOAT(0) NOT NULL,
	sky FLOAT(0) NULL,
	skyErr FLOAT(0) NULL,
	extendedness FLOAT(0) NULL,
	flagForAssociation SMALLINT NULL,
	flagForDetection SMALLINT NULL,
	flagForWcs SMALLINT NULL,
	PRIMARY KEY (sourceId),
	INDEX ampExposureId (ampExposureId ASC),
	INDEX filterId (filterId ASC),
	INDEX movingObjectId (movingObjectId ASC),
	INDEX objectId (objectId ASC),
	INDEX procHistoryId (procHistoryId ASC)
) ENGINE=MyISAM"""

class SourceGenerator:
    def __init__(self):
        self._synthFields = ["sourceId",
                             "filterId",
                             "objectId",
                             "procHistoryId",
                             "ra",
                             "raErrForWcs",
                             "decl",
                             "declErrForWcs",
                             "taiMidPoint",
                             "psfFlux",
                             "psfFluxErr",
                             "apFlux",
                             "apFluxErr",
                             "modelFlux",
                             "modelFluxErr",
                             "instFlux",
                             "instFluxErr",
                             "snr",
                             "chi2"]
        self._fieldGens = map(lambda f:
                              (f, getattr(self, "_new"
                                          + f[0].capitalize() + f[1:])),
                              self._synthFields)
        self._insertSourceTmpl = "INSERT INTO %(db)s.%(table)s (" \
            + ",".join(self._synthFields) \
            + ") VALUES %(values)s"""
        self._lastId=0;
        self._lastProcId = 1
        self._procsPerSrc = 0.01
        self._validFilterNames = ['u', 'g', 'r', 'i', 'z', 'y']
        self._raDeclErr = 0.1
        self._raDeclErrErr = 0.5 * self._raDeclErr
        self._psfFluxErr = 0.4
        self._psfFluxErrErr = 0.5 * self._psfFluxErr
        self._psfFluxMean = 10 * self._psfFluxErr
        pass

    def getInsertTemplate(self):
        return self._insertSourceTmpl

    def generateValueStmt(self, obj):
        return "(" + ",".join(map(lambda x: x(obj),
                                   imap(operator.itemgetter(1),
                                        self._fieldGens)
                                   )
                              )+ ")"

    ## Bogus data field synthesizers
    def _newSourceId(self, obj):
        self._lastId += 1
        return str(self._lastId)

    def _newFilterId(self, obj):
        # randomly pick among valid filter ids.
        return str(random.randint(1, 1+len(self._validFilterNames)))

    def _newObjectId(self, obj):
        # link source to the spec'd object
        return str(obj.id)

    def _newProcHistoryId(self, obj):
        if random.random() < self._procsPerSrc:
            self._lastProcId += 1
        return str(self._lastProcId)

    def _newRa(self, obj):
        return str(obj.ra + random.gauss(0, self._raDeclErr))
    def _newRaErrForWcs(self, obj):
        return str(self._raDeclErr + random.gauss(0,self._raDeclErrErr))
    def _newDecl(self, obj):
        return str(obj.decl + random.gauss(0, self._raDeclErr))
    def _newDeclErrForWcs(self, obj):
        return str(self._raDeclErr + random.gauss(0,self._raDeclErrErr))
    def _newTaiMidPoint(self, obj):
        return str(time.time()) # Use "now" as Temps Atomique International
    def _newPsfFlux(self, obj):
        return str(random.gauss(self._psfFluxMean, self._psfFluxErr))
    def _newPsfFluxErr(self, obj):
        return str(random.gauss(self._psfFluxErr, self._psfFluxErrErr))
    def _newApFlux(self, obj):
        return self._newPsfFlux(obj) # use psf version
    def _newApFluxErr(self, obj):
        return self._newPsfFluxErr(obj) # use psf version
    def _newModelFlux(self, obj):
        return self._newPsfFlux(obj) # use psf version
    def _newModelFluxErr(self, obj):
        return self._newPsfFluxErr(obj) # use psf version
    def _newInstFlux(self, obj):
        return self._newPsfFlux(obj) # use psf version
    def _newInstFluxErr(self, obj):
        return self._newPsfFluxErr(obj) # use psf version
    def _newSnr(self, obj):
        return str(random.gauss(self._psfFluxMean, self._psfFluxErr)
                   / self._psfFluxErrErr)
    def _newChi2(self, obj):
        return str(random.uniform(0,1))

class PretendObject:
    def __init__(self, id_, ra, decl):
        self.id = id_
        self.ra = ra
        self.decl = decl

class App:
    ### public interface ###
    def __init__(self):
        self._objPrefix = "Object_"
        self._srcPrefix = "Source_"
        # Default mysql packet size limit = 1M.
        #  size of source table value is ~ 226 bytes as a string.
        # 4000*226 = 904,000
        # 3000*226 = 678,000
        self._rowBatchSize = 3000
        pass


    def run(self):
        self._processArgs()
        self._synthesize()
        pass

    ### private ###
    def _processArgs(self):
        """Handle command-line params"""
        parser = OptionParser()
        parser.add_option("-S", "--socket", dest="socketFile",
                          default="/u1/local/mysql.sock",
                          help="Use socket file FILE to connect to mysql",
                          metavar="FILE")
        parser.add_option("-u", "--user", dest="user",
                          default="qsmaster",
                          help="User for db login if not %default")
        parser.add_option("-p", "--password", dest="password",
                          default="",
                          help="Password for db login. ('-' prompts)")
        parser.add_option("-D", "--database", dest="database",
                          default="LSST",
                          help="Database. (default %default)")

        parser.add_option("-q", "--quiet",
                          action="store_false", dest="verbose", default=True,
                          help="don't print status messages to stdout")
        parser.add_option("--createTables",
                          action="store_true", dest="createTables",
                          default=False,
                          help="Create/overwrite destination tables before insert (default=%default)",
                          )
        parser.add_option("--multiply", dest="multiply",
                          action="store", type="int", default=1,
                          help="Set sources per object (default %default)")
        parser.add_option("-w", "--write",
                          action="store_true", dest="write",
                          help="Write SQL to db rather than to console. (default=%default)",
                          default=False)
        parser.add_option("--cripple",
                          action="store_true", dest="cripple", default=False,
                          help="Limit the functionality to ease debugging")
        (options, args) = parser.parse_args()
        self._options = options
        if options.password == "-":
            options.password = getpass.getpass()
        self._srcGen = SourceGenerator()

    def _synthesize(self):
        """Do everything."""
        try:
            conn = sql.connect(unix_socket=self._options.socketFile,
                               user=self._options.user,
                               passwd=self._options.password)
        except Exception, e:
            print >>sys.stderr, e
            print >>sys.stderr, "Couldn't connect to db with socket"
            print >>sys.stderr, "Bailing out."
            return

        self._getTableList(conn)
        srcStatements = self._computeSynthesizeCmds(conn)
        if self._options.write:
            self._applySql(conn, srcStatements)
        else:
            print "\n".join(srcStatements)
        pass

    def _applySql(self, conn, stmts):
        c = conn.cursor()
        try:
            for s in stmts:
                c.execute(s)
                c.fetchall() # need to detect errors
        except Exception, e:
            print "Error writing new Source tables.", e
        pass
    def _getTableList(self, connection):
        c = connection.cursor()
        c.execute("show tables in %s" % self._options.database)

        def isGood(t):
            return t[:len(self._objPrefix)] == self._objPrefix
        cands = filter(isGood, imap(operator.itemgetter(0), c.fetchall()))
        #print "Candidates", cands
        self._objects = cands
        self._chunks = map(lambda n: n[len(self._objPrefix):], cands)

    def _computeSynthesizeCmds(self, connection):
        c = connection.cursor()
        cmds = []
        for i in self._chunks:
            if self._options.createTables:
                cmds.append(dropSourceSql %(self._options.database,
                                            self._srcPrefix + str(i)))
                cmds.append(createSourceSql %(self._options.database,
                                              self._srcPrefix + str(i)))
            cmds.extend(self._makeSrcStmt(c, i))
            if self._options.cripple:
                break # do only one right now.
        return cmds

    def _makeSrcStmt(self, cursor, chunk):
        """Return list of sql statements that insert rows into the table."""
        output = []
        def flushStmt(output, values):
            output.append(self._srcGen.getInsertTemplate() % {
                    "table" : self._srcPrefix + str(chunk),
                    "db" : self._options.database,
                    "values" : ",".join(values) })
            values = []

        stmt  = "SELECT * FROM %s.%s" % (self._options.database,
                                         self._objPrefix + str(chunk))
        cursor.execute(stmt)
        values = []
        for s in cursor.fetchall():
            for i in range(self._options.multiply):
                values.append(self._srcGen.generateValueStmt(self._formObj(s)))
            if len(values) > self._rowBatchSize:
                flushStmt(output, values)
                values = []
            pass
        if values:
            flushStmt(output, values)
        return output


    def _formObj(self, objtuple):
        # USNO-B object
        #| id | ra | decl | pm_ra | pm_raErr | pm_decl
        #   0    1     2       3       4          5
        #| pm_declErr | epoch  | bMag  | bMagF | rMag  | rMagF
        #     6          7        8       9       10      11
        #| bMag2 | bMagF2 | rMag2 | rMagF2 | chunkId | subChunkId
        #    12      13       14      15       16          17

        # PT1 Object:
        # objectId | iauId | ra_PS | ra_PS_Sigma | decl_PS | decl_PS_Sigma
        #    0        1        2       3               4         5
        # radecl_PS_Cov | ra_SG | ra_SG_Sigma | decl_SG | decl_SG_Sigma
        #    6              7        8              9         10
        # (using ra_PS and decl_PS, we would have [0,2,4] below.)

        return PretendObject(*map(lambda x: x(objtuple),
                                  imap(operator.itemgetter, [0,1,2])))

    def _cleanupSrc(self):
        template = "DROP TABLE IF EXISTS %s"
        stmt = template % ",".join(imap(lambda c: self._srcPrefix+str(c) % c,
                                        self._chunks))


if __name__ == "__main__":
    a = App()
    a.run()
