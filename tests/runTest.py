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
from  lsst.qserv.admin import commons
import logging
import optparse
import os
import re
import stat
import sys
import tempfile


class QservTestsRunner():

    def __init__(self, logging_level=logging.DEBUG ):
        self.logger = commons.console_logger(logging_level)

    def configure(self, config_dir, case_id, out_dirname, log_file_prefix='qserv-unit-tests' ):

        config_file_name=os.path.join(config_dir,"qserv-build.conf")
        default_config_file_name=os.path.join(config_dir,"qserv-build.default.conf")
        self.config = commons.read_config(config_file_name, default_config_file_name)

        self._user = self.config['mysqld']['user']
        self._password = self.config['mysqld']['pass']
        self._socket = self.config['mysqld']['sock']
        self._qservHost = self.config['qserv']['master']
        self._qservPort = self.config['mysql_proxy']['port']
        self._qservUser = self.config['qserv']['user']
        self._case_id = case_id

        if out_dirname == None :
            out_dirname = self.config['qserv']['tmp_dir']
        self._out_dirname = os.path.join(out_dirname, "qservTest_case%s" % case_id)

        qserv_tests_dirname = os.path.join(self.config['qserv']['base_dir'],'qserv','tests',"case%s" % self._case_id)

        self._input_dirname = os.path.join(qserv_tests_dirname,'data')
        self._queries_dirname = os.path.join(qserv_tests_dirname,"queries") 

        self.logger = commons.file_logger(
            log_file_prefix,
            log_path=self.config['qserv']['log_dir']
        )

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
        if self._mode == 'qserv':
            withQserv = True
            myOutDir = "%s/q" % self._out_dirname
        else:
            withQserv = False
            myOutDir = "%s/m" % self._out_dirname
        if not os.access(myOutDir, os.F_OK):
            os.makedirs(myOutDir)
            # because mysqld will write there
            os.chmod(myOutDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        qDir = self._queries_dirname
        print "Testing queries from %s" % qDir
        queries = sorted(os.listdir(qDir))
        noQservLine = re.compile('[\w\-\."%% ]*-- noQserv')
        for qFN in queries:
            if qFN.endswith(".sql"):
                if int(qFN[:4]) <= stopAt:
                    query_filename = os.path.join(qDir,qFN)
                    qF = open(query_filename, 'r')
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
        if self._mode == 'qserv':
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
        print "Loading data from %s" % self._input_dirname
        files = os.listdir(self._input_dirname)
        for f in files:
            if f.endswith('.schema'):
                tableName = f[:-7]
                schemaFile = os.path.join(self._input_dirname, f)
                dF = "%s/%s.tsv.gz" % (self._input_dirname, tableName)
                # check if the corresponding data file exists
                if not os.path.exists(dF):
                    raise Exception, "File: '%s' not found" % dF
                # uncompress data file into temp location
                # TODO : use a pipe instead of a tempfile
                tmp_data_file = tempfile.NamedTemporaryFile(suffix=(".%s.tsv" % tableName), dir=(self.config['qserv']['tmp_dir']),delete=False)
                
                cmd = "gunzip -c %s > %s" % (dF, tmp_data_file.name)
                self.logger.info("  Uncompressing: %s" % cmd)
                os.system(cmd)
                # load the table. Note, how we do it depends
                # whether we load to plain mysql or qserv
                # remove temporary file
                # treat Object and Source differently, they need to be partitioned
                if self._mode == 'qserv' and (tableName == 'Object' or tableName == 'Source'):
                    self.loadPartitionedTable(tableName, schemaFile, tmp_data_file.name)
                else:
                    self.loadRegularTable(tableName, schemaFile, tmp_data_file.name)
                os.unlink(tmp_data_file.name)
        self.disconnect()

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

    def loadPartitionedTable(self, table_name, schemaFile, data_filename):

        out_dirname = os.path.join(self.config['qserv']['tmp_dir'],table_name+"_partition")
        partition_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "partition.py")

        os.mkdir(out_dirname)
        os.system("python %s -PObject -t 2  -p 4 %s --delimiter '\t' -S 10 -s 2 --output-dir %s" % (partition_scriptname, data_filename, out_dirname))
        print '''
mkdir tmp1; cd tmp1; 

mkdir object; cd object
mysql -u<u> -p<p> qservTest_case01_m -e "select * INTO outfile '/tmp/Object.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM Object"
python ../../master/examples/partition.py -PObject -t 2  -p  4 /tmp/Object.csv -S 10 -s 2 
sudo rm /tmp/Object.csv

DONE ABOVE

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

    def parseOptions(self):
        script_name=sys.argv[0]
        op = optparse.OptionParser()
        op.add_option("-i", "--case-no", dest="case_no",
                  default="01",
                  help="test case number")
        mode_option_values = ['mysql','qserv']
        op.add_option("-m", "--mode",  action="append", dest="mode",
                  default=mode_option_values,
                  help= "Qserv test modes (direct mysql connection, or via qserv) : '" +
                  "', '".join(mode_option_values) +
                  "' [default: %default]")
        op.add_option("-c", "--config-dir", dest="config_dir",
                help= "Path to directory containing qserv-build.conf and"
                "qserv-build.default.conf")
        op.add_option("-s", "--stop-at", dest="stop_at",
                  default = 799,
                  help="Stop at query with given number")
        op.add_option("-o", "--out-dir", dest="out_dirname",
                  help="Full path to directory for storing temporary results. The results will be stored in <OUTDIR>/qservTest_case<CASENO>/")
        op.add_option("-v", "--verbose", dest="verboseMode",
                  default = 'n',
                  help="Run in verbose mode (y/n)")
        (options, args) = op.parse_args()


        if options.config_dir is None:
            print "%s: --config-dir flag not set" % script_name 
            print "Try `%s --help` for more information." % script_name
            exit(1)
        else:
            config_file_name=options.config_dir+os.sep+"qserv-build.conf"
            default_config_file_name=options.config_dir+os.sep+"qserv-build.default.conf"
            if not os.path.isfile(config_file_name):
                print ("%s: --config-dir must point to a directory containing " 
                        "qserv-build.conf" % script_name)
                exit(1)
            elif not os.path.isfile(config_file_name):
                print ("%s: --config-dir must point to a directory containing "
                       "qserv-build.default .conf" % script_name)
                exit(1)

        if not set(options.mode).issubset(set(mode_option_values)) :
            print "%s: --mode flag set with invalid value" % script_name
            print "Try `%s --help` for more information." % script_name
            exit(1)

        return options

    def run(self, options):

        if not os.access(self._out_dirname, os.F_OK):
            os.makedirs(self._out_dirname)

        for mode in options.mode:
            self._mode=mode
            self._dbName = "qservTest_case%s_%s" % (self._case_id, mode)
            self.loadData()     
            self.runQueries(options.stop_at)

def main():

    qserv_test_runner = QservTestsRunner()
    options = qserv_test_runner.parseOptions()  
    qserv_test_runner.configure(options.config_dir, options.case_no, options.out_dirname)
    qserv_test_runner.run(options)

if __name__ == '__main__':
    main()
