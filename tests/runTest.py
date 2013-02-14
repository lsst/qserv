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
import shutil
import stat
import sys
import tempfile


class QservTestsRunner():

    def __init__(self, logging_level=logging.DEBUG ):
        self.logger = commons.console_logger(logging_level)

    def configure(self, config_dir, case_id, out_dirname, log_file_prefix='qserv-tests' ):
        
        if config_dir is None:
            self.config = commons.read_user_config()
        else:
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

        self.partition_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "partition.py")
        self.load_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "loader.py")
        self.mysql_bin = os.path.join(self.config['qserv']['bin_dir'],'mysql')
        self.python_bin = os.path.join(self.config['qserv']['bin_dir'],'python')

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
        else:
            withQserv = False
        myOutDir = os.path.join(self._out_dirname, self._mode)
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

    def runQueryInShell(self, qText, out_file):
        if self._mode == 'qserv':
            #cmd = "mysql --port %i --host %s --batch -u%s -p%s %s -e \"%s\" > %s" % \
            #    (self._qservPort, self._qservHost, self._user, self._password, self._dbName, qText, outFile)

            execute_query_cmd = [
                self.mysql_bin, 
                '--port', str(self.config['mysql_proxy']['port']),
                '--host', self.config['qserv']['master'],
                '-u', self.config['qserv']['user'], 
                '--batch',
                self._dbName,
                '-e', qText
                ]
        else:
            #cmd = "mysql --socket=%s --batch -u%s -p%s %s -e \"%s\" > %s" % \
            #    (self._socket, self._user, self._password, self._dbName, qText, outFile)
            execute_query_cmd = [
                self.mysql_bin, 
                '--socket', self.config['mysqld']['sock'],
                '-u', self.config['mysqld']['user'], 
                '-p'+self.config['mysqld']['pass'],
                '--batch',
                self._dbName,
                '-e', qText
                ]

        commons.run_command(execute_query_cmd, stdout_file=out_file)
        self.logger.info("SQL query for mode %s launched"
                % self._mode)

    def initDatabases(self): 
        self.logger.info("Initializing databases %s, qservMeta" % self._dbName)
        self.connectNoDb()
        sql = "DROP DATABASE IF EXISTS %s" % self._dbName
        self.logger.info(sql)
        self._cursor.execute(sql)
        self._cursor.execute("CREATE DATABASE %s" % self._dbName)
        # self._cursor.execute("GRANT ALL ON %s.* TO '%s'@'*'" % (self._dbName, self._qservUser, self._qservHost))
        self._cursor.execute("GRANT ALL ON %s.* TO '*'@'*'" % (self._dbName))
        self._cursor.execute("USE {0};\n".format(self._dbName))

        self._cursor.execute("DROP DATABASE IF EXISTS qservMeta")
        self._cursor.execute("CREATE DATABASE qservMeta;\n")
        self.disconnect()

    # creates database and load all tables caseXX/data/
    # schema should be in <table>.schema
    # data should be in <table>.tsv.gz
    def loadData(self):

        self.initDatabases()

        self.connect2Db()
        print "Loading data from %s" % self._input_dirname
        files = os.listdir(self._input_dirname)
        for f in files:
            if f.endswith('.schema'):
                tableName = f[:-7]
                schemaFile = os.path.join(self._input_dirname, f)
                zipped_data_file = os.path.join(self._input_dirname, "%s.tsv.gz" % tableName)
                # check if the corresponding data file exists
                if not os.path.exists(zipped_data_file):
                    raise Exception, "File: '%s' not found" %  zipped_data_file
                # uncompress data file into temp location
                # TODO : use a pipe instead of a tempfile
                tmp_suffix = (".%s.tsv" % tableName)
                tmp_dir = self.config['qserv']['tmp_dir']
                tmp = tempfile.NamedTemporaryFile(suffix=tmp_suffix, dir=tmp_dir,delete=False)
                tmp_data_file = tmp.name
                
                #cmd = "gunzip -c %s > %s" % ( zipped_data_file, tmp_data_file)
                gunzip_cmd = [
                    "gunzip",
                    "-c", zipped_data_file
                    ]

                self.logger.info("  Uncompressing: %s" %  zipped_data_file)
                commons.run_command(gunzip_cmd, stdout_file=tmp_data_file)
                # load the table. Note, how we do it depends
                # whether we load to plain mysql or qserv
                # remove temporary file
                # treat Object and Source differently, they need to be partitioned
                if self._mode == 'qserv' and (tableName == 'Object' or tableName == 'Source'):
                    self.loadPartitionedTable(tableName, schemaFile, tmp_data_file)
                else:
                    self.loadRegularTable(tableName, schemaFile, tmp_data_file)
                os.unlink(tmp_data_file)
        self.disconnect()

    def loadRegularTable(self, tableName, schemaFile, dataFile):
        # load schema
        cmd = "mysql --socket=%s -u%s -p%s %s < %s" % \
            (self._socket, self._user, self._password, self._dbName, schemaFile)

        load_schema_cmd = [
            self.mysql_bin, 
            '--socket', self.config['mysqld']['sock'],
            '-u', self.config['mysqld']['user'], 
            '-p'+self.config['mysqld']['pass'],
            self._dbName,
            '-e', 'Source %s' %  schemaFile
        ]

        self.logger.info("  Loading schema %s" % schemaFile)
        commons.run_command(load_schema_cmd)
        # load data
        q = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" % \
            (dataFile, tableName)
        print "  Loading data:  ", q
        self._cursor.execute(q)

    def getNonEmptyChunkIds(self):
        # TODO
        #cmd =  "echo \"show tables in {0};\" | {1} | grep Object_ | sed \"s#Object_\(.*\)#touch {2}/\\1;#\" | sh --verbose".format(self._dbName, mysql_cmd, xrd_query_dir)
        return None

    def loadPartitionedTable(self, table, schemaFile, data_filename):

        stripes = self.config['qserv']['stripes']

        data_config = dict()
        data_config['Object']=dict()
        data_config['Object']['ra-column'] = 2
        data_config['Object']['decl-column'] = 4
        data_config['Object']['chunk-column-id'] = 227
        data_config['Source']=dict()
        # Source will be placed on the same chunk that its related Object
        data_config['Source']['ra-column'] = 33
        data_config['Source']['decl-column'] = 34
        # chunkId and subChunkId will be added
        data_config['Source']['chunk-column-id'] = None

        # load schema
        load_schema_cmd = [
            self.mysql_bin, 
            '--socket', self.config['mysqld']['sock'],
            '-u', self.config['mysqld']['user'], 
            '-p'+self.config['mysqld']['pass'],
            self._dbName,
            '-e', 'Source %s' %  schemaFile
        ]

        self.logger.info("  Loading schema %s" % schemaFile)
        commons.run_command(load_schema_cmd)

        # TODO : create index
        # "\nCREATE INDEX obj_objectid_idx on Object ( objectId );\n";

        # partition data       
        partition_dirname = os.path.join(self._out_dirname,table+"_partition")
        if os.path.exists(partition_dirname):
            shutil.rmtree(partition_dirname)
        os.makedirs(partition_dirname)

        # commons.run_command("python %s -PObject -t 2  -p 4 %s --delimiter '\t' -S 10 -s 2 --output-dir %s" % (self.partition_scriptname, data_filename, partition_dirname))
        partition_data_cmd = [
                self.python_bin,
                self.partition_scriptname,
                '--output-dir', partition_dirname,
                '--chunk-prefix', table,
                '--theta-column', str(data_config[table]['ra-column']),
                '--phi-column', str(data_config[table]['decl-column']),
                '--num-stripes', self.config['qserv']['stripes'],
                '--num-sub-stripes', self.config['qserv']['substripes'],
                '--delimiter', '\t'
            ]

        if data_config[table]['chunk-column-id'] != None :
             partition_data_cmd.extend(['--chunk-column', str(data_config[table]['chunk-column-id'])])

        partition_data_cmd.append(data_filename)

        out = commons.run_command(partition_data_cmd)
        self.logger.info("Test case%s LSST %s data partitioned : \n %s"
                % (self._case_id, table,out))

        # load partitionned data
        # TODO : remove hard-coded param : qservTest_caseXX_mysql
        load_partitionned_data_cmd = [
            self.python_bin, 
            self.load_scriptname,
            '-u', 'root', 
            '-p'+self.config['mysqld']['pass'],
            '--database', self._dbName,
            "%s:%s" %
            (self.config['qserv']['master'],self.config['mysqld']['port']),
            partition_dirname,
            "qservTest_case%s_mysql.%s" % (self._case_id, table)
        ]
        # python master/examples/loader.py --verbose -u root -p changeme --database qservTest_case01_qserv -D clrlsst-dbmaster.in2p3.fr:13306 /opt/qserv-dev/tmp/Object_partition/ qservTest_case01_mysql.Object
        out = commons.run_command(load_partitionned_data_cmd)
        self.logger.info("Partitioned %s data loaded : %s" % (table,out))

        # mysql -u<u> -p<p> qservTest_case01_qserv -e "create table Object_1234567890 like Object_100"
        
        # sql = "CREATE DATABASE IF NOT EXISTS LSST;"
        sql = "USE {0};\n".format(self._dbName)
        sql +=  "CREATE TABLE {0}_1234567890 LIKE {0}_100;\n".format(table)
        sql += "USE qservMeta;\n"
        sql += "CREATE TABLE LSST__{0} ({1}Id BIGINT NOT NULL PRIMARY KEY, x_chunkId INT, x_subChunkId INT);\n".format(table, table.lower())

        insert_sql =  "insert into LSST__{1} SELECT {2}Id, chunkId, subChunkId from {0}.{1}_%s;\n".format(self._dbName,table,table.lower())

        chunk_id_list=[80, 98, 100, 118]

        for chunkId in chunk_id_list :
            tmp =  insert_sql % chunkId
            sql += "\n" + tmp

        sql_cmd = [
            self.mysql_bin, 
            '-S', self.config['mysqld']['sock'],
            '-u', 'root', 
            '-p'+self.config['mysqld']['pass'],
            '-e', sql
        ]
        out = commons.run_command(sql_cmd)
        self.logger.info("%s table for empty chunk created, and meta Loaded : %s" % (table,out))

        # Create xrootd directories (inspired from fixExportDirs.sh)
        self.init_worker_xrd_dirs()

        empty_chunks_filename = os.path.join(self.config['qserv']['base_dir'],"etc","emptyChunks.txt")
        f=open(empty_chunks_filename,"w")
        empty_chunks_list=[i for i in range(7201) if i not in chunk_id_list]
        for i in empty_chunks_list:
            f.write("%s\n" %i)
        f.close()


#         print '''
# mkdir tmp1; cd tmp1; 

# mkdir object; cd object
# mysql -u<u> -p<p> qservTest_case01_m -e "select * INTO outfile '/tmp/Object.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM Object"
# python ../../master/examples/partition.py -PObject -t 2  -p  4 /tmp/Object.csv -S 10 -s 2 
# sudo rm /tmp/Object.csv


# #use the loadPartitionedObjectTables.py script to generate loadO
# mysql -u<u> -p<p> qservTest_case01_q < loadO
# mysql -u<u> -p<p> qservTest_case01_q -e "create table Object_1234567890 like Object_100"

# cd ../; mkdir source; cd source
# mysql -u<u> -p<p> qservTest_case01_m -e "select * INTO outfile '/tmp/Source.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM Source"
# python ../../master/examples/partition.py -PSource -t 33 -p 34 -o 0 /tmp/Source.csv -S 10 -s 2
# #use the loadPartitionedSourceTables.py script to generate loadS
# mysql -u<u> -p<p> qservTest_case01_q < loadS
# mysql -u<u> -p<p> qservTest_case01_q -e "create table Source_1234567890 like Source_100"

# # this creates the objectId index
# mysql -u<u> -p<p> -e "create database qservMeta"
# mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "create table LSST__Object (objectId BIGINT NOT NULL PRIMARY KEY, x_chunkId INT, x_subChunkId INT)"
# mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "insert into LSST__Object SELECT objectId, chunkId, subChunkId from qservTest_case01_q.Object_100"
# mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "insert into LSST__Object SELECT objectId, chunkId, subChunkId from qservTest_case01_q.Object_118"
# mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "insert into LSST__Object SELECT objectId, chunkId, subChunkId from qservTest_case01_q.Object_80"
# mysql -u<u> -p<p> qservTest_case01_q qservMeta -e "insert into LSST__Object SELECT objectId, chunkId, subChunkId from qservTest_case01_q.Object_98"

# DONE ABOVE

# ../qserv-git-master/worker/tools/qsDbTool -a /u/sf/becla/.lsst/dbAuth.txt -i test register qservTest_case01_q Object Source
# ../qserv-git-master/worker/tools/qsDbTool -a /u/sf/becla/.lsst/dbAuth.txt -i test -b /u1/qserv/xrootd-run export qservTest_case01_q

# /u/sf/danielw/ctools/bin/makeEmptyChunks.py -o /u1/qserv/qserv-run/emptyChunks_qservTest_case01_q.txt 0 7200 /u1/qserv/qserv-git-ticket1934/tmp1/object/stripe_*
#         '''
        raw_input("Press Enter to continue...")


    def init_worker_xrd_dirs(self):

        # match oss.localroot in etc/lsp.cf
        xrootd_run_dir = os.path.join(self.config['qserv']['base_dir'],'xrootd-run')

        xrd_query_dir = os.path.join(xrootd_run_dir, 'q', self._dbName) 
        xrd_result_dir = os.path.join(xrootd_run_dir, 'result') 

        if os.path.exists(xrd_query_dir):
            self.logger.info("Emptying existing xrootd query dir : %s" % xrd_query_dir)
            shutil.rmtree(xrd_query_dir)
        os.makedirs(xrd_query_dir)
        self.logger.info("Making placeholders")

        mysql_cmd = " ".join([self.mysql_bin, 
                               '--socket', self.config['mysqld']['sock'],
                               '-u', self.config['mysqld']['user'], 
                               '-p'+self.config['mysqld']['pass']
                               ])

        cmd =  "echo \"show tables in {0};\" | {1} | grep Object_ | sed \"s#Object_\(.*\)#touch {2}/\\1;#\" | sh --verbose".format(self._dbName, mysql_cmd, xrd_query_dir)

        self.logger.debug("Running : %s" % cmd)
        os.system(cmd)

        emptychunk_xrd_dir =  os.path.join(xrd_query_dir,"1234567890")
        cmd = "touch %s" % emptychunk_xrd_dir
        self.logger.debug("Running : %s" % cmd)
        os.system(cmd)

        #zero_dir =  os.path.join(xrd_query_dir,"0")
        #cmd = "touch %s" % zero_dir
        #self.logger.debug("Running : %s" % cmd)
        #os.system(cmd)

        # WARNING : no data in this chunk, is it usefull ??
        #emptychunk_xrd_dir =  os.path.join(xrd_query_dir,"0")
        #cmd = "touch %s" % emptychunk_xrd_dir
        #self.logger.debug("Running : %s" % cmd)
        #os.system(cmd)

        if os.path.exists(xrd_result_dir):
            self.logger.info("Emptying existing xrootd result dir : %s" % xrd_result_dir)
            shutil.rmtree(xrd_result_dir)
        os.makedirs(xrd_result_dir)

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

        if not set(options.mode).issubset(set(mode_option_values)) :
            print "%s: --mode flag set with invalid value" % script_name
            print "Try `%s --help` for more information." % script_name
            exit(1)

        return options

    def run(self, options):

        #if not os.access(self._out_dirname, os.F_OK):
        #    os.makedirs(self._out_dirname)

        for mode in options.mode:
            self._mode=mode

            if (self._mode=='mysql'):
                self._dbName = "qservTest_case%s_%s" % (self._case_id, self._mode)
            else:
                self._dbName = "LSST"
            self.loadData()     
            self.runQueries(options.stop_at)

def main():

    qserv_test_runner = QservTestsRunner()
    options = qserv_test_runner.parseOptions()  
    qserv_test_runner.configure(options.config_dir, options.case_no, options.out_dirname)
    qserv_test_runner.run(options)

if __name__ == '__main__':
    main()
