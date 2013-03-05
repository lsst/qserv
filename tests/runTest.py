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

import ConfigParser
import MySQLdb as sql
import optparse
import os
import re
import stat
import tempfile


class RunTests():

    def init(self, config, caseNo, outDir, mode, verboseMode):

        self._user = config.get('mysql','user')
        self._password = config.get('mysql','pass')
        self._socket = config.get('mysql','sock')
        self._qservHost = config.get('qserv','host')
        self._qservPort = config.getint('qserv','port')
        self._qservUser = config.get('qserv','user')
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
        if not os.access(myOutDir, os.F_OK):
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
                    outFile = "%s/%s" % (myOutDir, qFN.replace('.sql', '.txt'))
                    #qText += " INTO OUTFILE '%s'" % outFile
                    print "Running %s: %s\n" % (qFN, qText)
                    self.runQueryInShell(qText, outFile)

    def disconnect(self):
        print "Disconnecting"
        self._cursor.close()
        self._conn.close()

    def runQueryInShell(self, qText, outFile):
        if self._mode == 'q':
            cmd = "mysql --port %i --host %s --batch -u%s -p%s %s -e \"%s\" > %s" % \
                (self._qservPort, self._qservHost, self._user, self._password, self._dbName, qText, outFile)
        else:
            cmd = "mysql --socket=%s --batch -u%s -p%s %s -e \"%s\" > %s" % \
                (self._socket, self._user, self._password, self._dbName, qText, outFile)
        print cmd
        os.system(cmd)

    # creates database and load all tables caseXX/data/
    # schema should be in <table>.schema
    # data should be in <table>.tsv.gz
    def loadData(self):
        print "Creating database %s" % self._dbName
        self.connectNoDb()
        self._cursor.execute("DROP DATABASE IF EXISTS %s" % self._dbName)
        self._cursor.execute("CREATE DATABASE %s" % self._dbName)
        # self._cursor.execute("GRANT ALL ON %s.* TO '%s'@'*'" % (self._dbName, self._qservUser, self._qservHost))
        self._cursor.execute("GRANT ALL ON %s.* TO '*'@'*'" % (self._dbName))
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
        cmd = "mysql --socket=%s -u%s -p%s %s < %s" % \
            (self._socket, self._user, self._password, self._dbName, schemaFile)
        print "  Loading schema:", cmd
        os.system(cmd)
        # load data
        q = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" % \
            (dataFile, tableName)
        print "  Loading data:  ", q
        self._cursor.execute(q)

    def loadPartitionedTable(self, tableName, schemaFile, dataFile):
        print '''
mkdir tmp1; cd tmp1; 

mkdir object; cd object
mysql -u<u> -p<p> qservTest_case01_m -e "select * INTO outfile '/tmp/Object.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM Object"
python ../../master/examples/partition.py -PObject -t 2  -p  4 /tmp/Object.csv -S 10 -s 2 
sudo rm /tmp/Object.csv
#use the loadPartitionedObjectTables.py script to generate loadO
mysql -u<u> -p<p> qservTest_case01_q < loadO
mysql -u<u> -p<p> qservTest_case01_q -e "create table Object_1234567890 like Object_100"

cd ../; mkdir source; cd source
mysql -u<u> -p<p> qservTest_case01_m -e "select * INTO outfile '/tmp/Source.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM Source"
python ../../master/examples/partition.py -PSource -t 33 -p 34 -o 0 /tmp/Source.csv -S 10 -s 2
#use the loadPartitionedSourceTables.py script to generate loadS
mysql -u<u> -p<p> qservTest_case01_q < loadS
mysql -u<u> -p<p> qservTest_case01_q -e "create table Source_1234567890 like Source_100"

# this creates the objectId index
mysql -u<u> -p<p> -e "create database qservMeta"
mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "create table LSST__Object (objectId BIGINT NOT NULL PRIMARY KEY, x_chunkId INT, x_subChunkId INT)"
mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "insert into LSST__Object SELECT objectId, chunkId, subChunkId from qservTest_case01_q.Object_100"
mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "insert into LSST__Object SELECT objectId, chunkId, subChunkId from qservTest_case01_q.Object_118"
mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "insert into LSST__Object SELECT objectId, chunkId, subChunkId from qservTest_case01_q.Object_80"
mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "insert into LSST__Object SELECT objectId, chunkId, subChunkId from qservTest_case01_q.Object_98"

../qserv-git-master/worker/tools/qsDbTool -a /u/sf/becla/.lsst/dbAuth.txt -i test register qservTest_case01_q Object Source
../qserv-git-master/worker/tools/qsDbTool -a /u/sf/becla/.lsst/dbAuth.txt -i test -b /u1/qserv/xrootd-run export qservTest_case01_q

/u/sf/danielw/ctools/bin/makeEmptyChunks.py -o /u1/qserv/qserv-run/emptyChunks_qservTest_case01_q.txt 0 7200 /u1/qserv/qserv-git-ticket1934/tmp1/object/stripe_*
        '''
        raw_input("Press Enter to continue...")


def runIt(config, caseNo, outDir, stopAt, mode, verboseMode):
    x = RunTests()
    x.init(config, caseNo, outDir, mode, verboseMode)
    x.loadData()
    x.runQueries(stopAt)


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
    config = ConfigParser.RawConfigParser()
    config.read(authFile)

    outDir = "%s/qservTest_case%s" % (_options.outDir, _options.caseNo)
    
    if not os.access(outDir, os.F_OK):
        os.makedirs(outDir)

    if modePlainMySql:
        print "\n***** running plain mysql test *****\n"
        runIt(config,_options.caseNo, outDir, stopAt, "m", verboseMode)
    if modeQserv:
        print "\n***** running qserv test *****\n"
        runIt(config,_options.caseNo, outDir, stopAt, "q", verboseMode)

if __name__ == '__main__':
    main()
