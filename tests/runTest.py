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

# testAppInterface.py : A module with Python unittest code for testing
# functionality available through the appInterface module.  Currently
# only includes minimal fuzz testing and (unfinished) query replaying.

import MySQLdb as sql
import optparse
import os
import re
import stat
import tempfile


class RunTests():

    def init(self, user, password, socket, qservHost, qservPort, caseNo, outDir, mode, verboseMode):
        self._user = user
        self._password = password
        self._socket = socket
        self._qservHost = qservHost
        self._qservPort = qservPort
        self._dbName = "qservTest_case%s_%s" % (caseNo, mode)
        self._caseNo = caseNo
        self._outDir = outDir
        self._mode = mode
        self._verboseMode = verboseMode

    def connectNoDb(self):
        print "Connecting via socket", self._socket, "as", self._user
        self._conn = sql.connect(user=self._user,
                                 passwd=self._password,
                                 unix_socket=self._socket)
        self._cursor = self._conn.cursor()

    def connect2Db(self):
        if self._mode == 'q':
            if self._qservHost == "" or self._qservPort == 0:
                raise Exception, "Need a valid host and port for qserv mode"
            print "Connecting to %s:%i as %s to db %s" % \
                (self._qservHost, self._qservPort, self._user, self._dbName)
            self._conn = sql.connect(user=self._user,
                                     passwd=self._password,
                                     host=self._qservHost,
                                     port=self._qservPort,
                                     db=self._dbName)
        else:
            print "Connecting via socket", self._socket, "as", self._user, \
                "to db", self._dbName
            self._conn = sql.connect(user=self._user,
                                     passwd=self._password,
                                     unix_socket=self._socket,
                                     db=self._dbName)
        self._cursor = self._conn.cursor()

    def runQueries(self, stopAt):
        if self._mode == 'q':
            withQserv = True
            myOutDir = "%s/q" % self._outDir
        else:
            withQserv = False
            myOutDir = "%s/m" % self._outDir
        os.makedirs(myOutDir)
        # because mysqld will write there
        os.chmod(myOutDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        qDir = "case%s/queries/" % self._caseNo
        print "Testing queries from %s" % qDir
        queries = sorted(os.listdir(qDir))
        noQservLine = re.compile('[\w\-\."%% ]*-- noQserv')
        for qFN in queries:
            if qFN.endswith(".sql"):
                if int(qFN[:4]) <= stopAt:
                    qF = open(qDir+qFN, 'r')
                    qText = ""
                    for line in qF:
                        line = line.rstrip().lstrip()
                        line = re.sub(' +', ' ', line)
                        if withQserv and line.startswith("-- withQserv"):
                            qText += line[13:] # skip the "-- withQserv" text
                        elif line.startswith("--") or line == "":
                            pass # do nothing with commented or empty lines
                        else:
                            qData = noQservLine.search(line)
                            if not withQserv:
                                if qData:
                                    qText += qData.group(0)[:-10]
                                else:
                                    qText += line
                            elif not qData:
                                qText += line
                        qText += ' '
                    qText += " INTO OUTFILE '%s/%s'" % \
                        (myOutDir, qFN.replace('.sql', '.txt'))
                    print "Running %s: %s\n" % (qFN, qText)
                    self.runQuery(qText)

    def disconnect(self):
        print "Disconnecting"
        self._cursor.close()
        self._conn.close()

    def runQuery(self, query):
        self._cursor.execute(query)
        rows = self._cursor.fetchall()
        if self._verboseMode:
            for r in rows:
                print r

    # creates database and load all tables caseXX/data/
    # schema should be in <table>.schema
    # data should be in <table>.tsv.gz
    def loadData(self):
        print "Creating database %s" % self._dbName
        self.connectNoDb()
        self._cursor.execute("DROP DATABASE %s" % self._dbName)
        self._cursor.execute("CREATE DATABASE %s" % self._dbName)
        self.disconnect()

        self.connect2Db()
        inputDir = "case%s/data" % self._caseNo
        print "Loading data from %s" % inputDir
        files = os.listdir(inputDir)
        for f in files:
            if f.endswith('.schema'):
                tableName = f[:-7]
                schemaFile = "%s/%s" % (inputDir, f)
                dF = "%s/%s.tsv.gz" % (inputDir, tableName)
                # check if the corresponding data file exists
                if not os.path.exists(dF):
                    raise Exception, "File: '%s' not found" % dF
                # uncompress data file into temp location
                dataFile = "%s.%s.tsv" % (tempfile.mktemp(), tableName)
                cmd = "gunzip -c %s > %s" % (dF, dataFile)
                print "  Uncompressing: ", cmd
                os.system(cmd)
                # load the table. Note, how we do it depends
                # whether we load to plain mysql or qserv
                self.loadTable(tableName, schemaFile, dataFile)
                # remove temporary file
                os.unlink(dataFile)
        self.disconnect()

    def loadTable(self, tableName, schemaFile, dataFile):
        # treat Object and Source differently, they need to be partitioned
        if self._mode == 'q' and (tableName == 'Object' or tableName == 'Source'):
            self.loadPartitionedTable(tableName, schemaFile, dataFile)
        else:
            self.loadRegularTable(tableName, schemaFile, dataFile)

    def loadRegularTable(self, tableName, schemaFile, dataFile):
        # load schema
        cmd = "mysql -u%s -p%s %s < %s" % \
            (self._user, self._password, self._dbName, schemaFile)
        print "  Loading schema:", cmd
        os.system(cmd)
        # load data
        q = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" % \
            (dataFile, tableName)
        print "  Loading data:  ", q
        self._cursor.execute(q)

    def loadPartitionedTable(self, tableName, schemaFile, dataFile):
        print '''
mkdir tmpDir; cd tmpDir; mkdir object; cd object
python partition.py -PObject -t 2  -p  4      /tmp/Object.csv -S 10 -s 2 > loadO
mysql -u<u> -p<p> qservTest_case01_q < loadO

cd ../; mkdir source; cd source
python partition.py -PSource -t 33 -p 34 -o 0 /tmp/Source.csv -S 10 -s 2 > loadS
mysql -u<u> -p<p> qservTest_case01_q < loadS

../qserv-git-master/worker/tools/qsDbTool -a /u/sf/becla/.lsst/dbAuth.txt -i test register qservTest_case01_q Object Source
../qserv-git-master/worker/tools/qsDbTool -a /u/sf/becla/.lsst/dbAuth.txt -i test -b /u1/qserv/xrootd-run export qservTest_case01_q

/u/sf/danielw/ctools/bin/makeEmptyChunks.py -o /u1/qserv/qserv-run/emptyChunks_test.txt 0 7200 /u1/qserv/qserv-git-ticket1934/tmp/object/stripe_*
        '''
        raw_input("Press Enter to continue...")

        #tmpDir = tempfile.mktemp()
        #partExec = "python ../master/examples/partition.py"
        #os.mkdir(tmpDir)
        #os.cd(tmpDir)
        #cmd = "%s -P%s -t 2 -p 4 %s -S 10 -s 2" % \
        #    (partExec, tableName, tmpDir)
        #os.system(cmd)
        # then load each generated file from tmpDir/stripe_*/* 

        # this will work for tables with overlap only
        # e.g., it works for Object table:
        #templTable = 'rplante_PT1_2_u_pt12prod_im3000.Object' # FIXME
        #for f1 in os.listdir(tmpDir):
        #    if f1.startswith('stripe_'):
        #        for f2 in os.listdir('%s/%s' % (tmpDir, f1)):
        #            if f2.endswith('.csv'):
        #                t = f2[:-4]
        #                print 'CREATE TABLE %s LIKE %s;' % (t, templTable)
        #                print 'ALTER TABLE %s CHANGE chunkId deleteMe1 INT, CHANGE subChunkId deleteMe2 INT, ADD COLUMN chunkId INT, ADD COLUMN subChunkId INT;' % t
        #                if f2.rfind('Overlap') >0:
        #                    print 'ALTER TABLE %s DROP PRIMARY KEY, ADD PRIMARY KEY(objectId, subChunkId);' % t
        #                print "LOAD DATA LOCAL INFILE '%s/%s/%s' INTO TABLE %s FIELDS TERMINATED BY ',';" % (baseDir, f1, f2, t)
        #                print 'ALTER TABLE %s DROP COLUMN deleteMe1, DROP COLUMN deleteMe2;\n' % t


def runIt(user, pwd, theSocket, qservHost, qservPort, caseNo, outDir, stopAt, mode, verboseMode):
    x = RunTests()
    x.init(user, pwd, theSocket, qservHost, qservPort, caseNo, outDir, mode, verboseMode)
    x.loadData()
    x.connect2Db()
    x.runQueries(stopAt)
    x.disconnect()


def main():
    op = optparse.OptionParser()
    op.add_option("-c", "--caseNo", dest="caseNo",
                  default="01",
                  help="test case number")
    op.add_option("-a", "--authFile", dest="authFile",
                  help="File with mysql connection info")
    op.add_option("-s", "--stopAt", dest="stopAt",
                  default = 799,
                  help="Stop at query with given number")
    op.add_option("-o", "--outDir", dest="outDir",
                  default = "/tmp",
                  help="Full path to directory for storing temporary results. The results will be stored in <OUTDIR>/qservTest_case<CASENO>/")
    op.add_option("-m", "--mode", dest="mode",
                  default = False,
                  help="Mode: 'm' - plain mysql, 'q' - qserv, 'b' - both")
    op.add_option("-v", "--verbose", dest="verboseMode",
                  default = 'n',
                  help="Run in verbose mode (y/n)")
    (_options, args) = op.parse_args()

    if _options.authFile is None:
        print "runTest.py: --authFile flag not set"
        print "Try `runTest.py --help` for more information."
        return -1

    authFile = _options.authFile
    stopAt = int(_options.stopAt)
    if _options.verboseMode == 'y' or \
       _options.verboseMode == 'Y' or \
       _options.verboseMode == '1':
        verboseMode = True
    else:
        verboseMode = False

    if not _options.mode:
        modePlainMySql = True
        modeQserv = True
    else:
        if _options.mode == 'm':
            modePlainMySql = True
        elif _options.mode == 'q':
            modeQserv = True
        elif _options.mode == 'b':
            modePlainMySql = True
            modeQserv = True
        else:
            print "Invalid value for option 'mode'"
            return -1

    ## read auth file
    f = open(authFile)
    for line in f:
        line = line.rstrip()
        (key, value) = line.split(':')
        if key == 'user':
            mysqlUser = value
        elif key == 'pass':
            mysqlPass = value
        elif key == 'mysqlSock':
            mysqlSock = value
        elif key == 'qservHost':
            qservHost = value
        elif key == 'qservPort':
            qservPort = int(value)
    f.close()

    outDir = "%s/qservTest_case%s" % (_options.outDir, _options.caseNo)
    os.makedirs(outDir)

    if modePlainMySql:
        print "\n***** running plain mysql test *****\n"
        runIt(mysqlUser, mysqlPass, mysqlSock, qservHost, qservPort, 
             _options.caseNo, outDir, stopAt, "m", verboseMode)
    if modeQserv:
        print "\n***** running qserv test *****\n"
        runIt(mysqlUser, mysqlPass, mysqlSock, qservHost, qservPort,
              _options.caseNo, outDir, stopAt, "q", verboseMode)

if __name__ == '__main__':
    main()
