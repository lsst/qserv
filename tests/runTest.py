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
import QservDataLoader
import re
import shutil
import SQLCmd
import SQLConnection
import SQLMode
import SQLReader
import SQLSchema
import stat
import sys
import tempfile


def convertSchemaFile(tableName, schemaFile, newSchemaFile, schemaDict):
    
    mySchema = SQLSchema.SQLSchema(tableName)    
    mySchema.read(schemaFile)    
    mySchema.deleteField("`_chunkId`")
    mySchema.deleteField("`_subChunkId`")
    mySchema.addField("`chunkId`", "int(11)", ("DEFAULT", "NULL"))
    mySchema.addField("`subChunkId`", "int(11)", ("DEFAULT", "NULL"))
    
    if (tableName == "Object"):
        mySchema.deletePrimaryKey()
        mySchema.createIndex("`obj_objectid_idx`", "Object", "`objectId`")

    mySchema.write(newSchemaFile)
    schemaDict[tableName]=mySchema
    
    return mySchema


def findAllDataInStripes():
  """ Find all files like stripe_<stripeId>/<name>_<chunkId>.csv """

  result = []
  stripeRegexp = re.compile("^stripe_(.*)")
  chunkRegexp = re.compile("(.*)_(.*).csv")
  
  for (dirpath, dirnames, filenames) in os.walk(stripeDir):
    stripeMatching = stripeRegexp.match(os.path.basename(dirpath))
    if (stripeMatching is not None):
      stripeId = stripeMatching.groups()
      for filename in filenames:
        chunkMatching = chunkRegexp.match(filename)
        if (chunkMatching is not None):
           name, chunkId = chunkMatching.groups()
           result.append((stripeDir, dirpath, filename, name, chunkId))

  return result


def createSQLLoadFile(tableName, filename):
  overlapRegexp = re.compile("^Object\S+Overlap")
  
  with open(filename, 'w') as outfile:
    chunksList = []
    for (stripeDir, dirpath, filename, name, chunkId) in findAllDataInStripes():
      basedir = os.path.basename(dirpath)
      if (name == tableName):
        chunksList.append(chunkId)
        outfile.write("DROP TABLE IF EXISTS %s_%s;\n" % (name, chunkId))
        outfile.write("CREATE TABLE IF NOT EXISTS %s_%s LIKE %s;\n" % (name, chunkId, tableName))
        outfile.write("LOAD DATA INFILE '%s%s/%s' IGNORE INTO TABLE %s_%s FIELDS TERMINATED BY ',';\n" % (stripeDir, basedir, filename, name, chunkId))
      if (tableName == "Object") and (overlapRegexp.match(name) is not None):
        outfile.write("CREATE TABLE IF NOT EXISTS %s_%s LIKE %s;\n" % (name, chunkId, tableName))
        outfile.write("LOAD DATA INFILE '%s%s/%s' INTO TABLE %s_%s FIELDS TERMINATED BY ',';\n" % (stripeDir, basedir, filename, name, chunkId))

    # Seulement pour les tables Object et Source :
    outfile.write("CREATE TABLE IF NOT EXISTS %s_1234567890 LIKE %s;\n" % (tableName, tableName))

  return set(chunksList)
      
      
#def createSetupFile(filename):
#  MySQLProxyPort = self.config['mysql_proxy']['port']
#  password = self.config['mysqld']['pass']
#  socketFile = self.config['mysqld']['sock']
#  with open(filename, 'w') as outfile:
#    outfile.write("host:localhost\n")
#    outfile.write("port:%i\n" % MySQLProxyPort)
#    outfile.write("user:%s\n" % username)
#    outfile.write("pass:%s\n" % password)
#    outfile.write("sock:%s\n" % socketFile) 
    



# Douglas perl code in Python
# def loadData(tableName, schemaFile):
#   database = "LSST"
#   metaDatabase = "qservMeta"
#   sqlInteface = initSQL(database)
#   schemaFileBasename = os.path.basename(schemaFile)
#   installdir = self.config['qserv']['base_dir']
#   tmpdir = self.config['qserv']['tmp_dir']
#   stripes = self.config['qserv']['stripes']
#   newSchemaFile = os.path.join(tmpdir, schemaFileBasename)
#   SQLLoadFile = os.path.join(tmpdir, "load_" + schemaFileBasename)
#   emptyChunksFilename = os.path.join(installdir, "etc", "emptyChunks.txt")
#   setupFilename = os.path.join(installdir, "etc", "setup.cnf")
#   
#   createDatabaseIfNotExists(sqlInterface, database)
#   grantAllRightsOnDatabaseTo(database, "'*'@'*'")
#   dropDatabaseIfExists(sqlInterface, metaDatabase)
#   createDatabase(sqlInterface, metaDatabase)
#                              
#   convertSchemaFile(tableName, schemaFile, newSchemaFile)
#   chunks = createSQLLoadFile(tableName, SQLLoadFile)
#   sqlInteface.executeFromFile(newSchemaFile)
#   sqlInteface.executeFromFile(SQLLoadFile)
#   createEmptyChunksFile(stripes, chunks, emptyChunksFilename)
#   createSetupFile(setupFilename)
#   runfixExportDir()
#   # a remplacer avec self.init_worker_xrd_dirs(chunks)


  
# loadData("Object", "/data/lsst/pt11/Object.sql")

# ----------------------------------------

# TODO: do we have to drop old schema if it exists ?  
def loadSchema(sqlInterface, directory, table, schemaSuffix, schemaDict):
  partitionnedTables = ["Object", "Source"]
  schemaFile = directory + "/" + table + "." + schemaSuffix
  if table in partitionnedTables:      
    newSchemaFile = directory + "/" + table + "_converted" + "." + schemaSuffix
    convertSchemaFile(table, schemaFile, newSchemaFile, schemaDict)
    sqlInterface['cmd'].executeFromFileWithMySQLCLient(newSchemaFile)
    os.unlink(newSchemaFile)
  else:
    sqlInterface['sock'].executeFromFile(schemaFile)

      
# TODO: suffixes management (CSV, TSV, GZ, etc.)
# def loadData(sqlInteface, directory, table, dataSuffixList):
#   filename = directory + table + ".".join(dataSuffixList)
#   if (dataSuffixList != []):
#     suffix = dataSuffixList.pop()
#     if (suffix == "gz"):
#       newFilename = directory + table + ".".join(dataSuffixList)
#       gunzip(filename, newFilename)
#       # TODO: filename could be a fifo (from mkfifo) but then gunzip should run from background
#       loadData(directory, table, dataSuffixList)
#       os.unlink(newFilename)
#     elif (suffix == "csv"):
#       SQL_query = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',';\n"  % (filename, table)
#       sqlInteface.execute(SQL_query)
#     elif (suffix == "tsv"):
#       SQL_query = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s;\n"  % (filename, table)
#       sqlInteface.execute(SQL_query)
  
# ------------------------------------------------------------

def getSchemaFiles(dirname):
  files = os.listdir(dirname)
  result = []
  for file in files:
    if file.endswith('.schema'):
      result.append(file)
  return result


def gunzip(filename, output = None):
  if (output is None):
    commons.run_command(["gunzip", filename])
  else:
    commons.run_command(["gunzip", "-c", filename], stdout_file=output)


class QservTestsRunner():

    def __init__(self, logging_level=logging.DEBUG ):
        self.logger = commons.console_logger(logging_level)
        self._schemaDict = dict()
        self.qservDataLoader = None

        
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
        self._logFilePrefix = log_file_prefix
        self._sqlInterface = dict()

        self.partition_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "partition.py")
        self.load_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "loader.py")

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
        self.logger.info("Connecting via socket"+ self._socket+ "as"+ self._user)
        self._conn = sql.connect(user=self._user,
                                 passwd=self._password,
                                 unix_socket=self._socket)
        self._cursor = self._conn.cursor()

    def connect2Db(self):
        self.logger.info("Connecting via socket"+ self._socket+ "as"+ self._user+
            "to db"+ self._dbName)
        self._conn = sql.connect(user=self._user,
                                 passwd=self._password,
                                 unix_socket=self._socket,
                                 db=self._dbName)
        self._cursor = self._conn.cursor()

    def disconnect(self):
        self.logger.info("Disconnecting manually from DB")
        self._cursor.close()
        self._conn.close()

    def runQueries(self, stopAt):
        if self._mode == 'qserv':
            withQserv = True
        else:
            withQserv = False
        myOutDir = os.path.join(self._out_dirname, "outputs",self._mode)
        if not os.access(myOutDir, os.F_OK):
            os.makedirs(myOutDir)
            # because mysqld will write there
            os.chmod(myOutDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        qDir = self._queries_dirname
        self.logger.info("Testing queries from %s" % qDir)
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
                    self.logger.info("Running %s: %s\n" % (qFN, qText))
                    self.runQueryInShell(qText, outFile)
                    # self._sqlInterface.executeFromFileWithMySQLCLient(query_filename, outFile)

    def runQueryInShell(self, query, out_file = None):
        mode = self._mode
        self.logger.info("SQL query %s for mode %s launched" % (query, mode))
        self._sqlInterface['query'].executeWithMySQLCLient(query, out_file)
    
        
    # creates database and load all tables caseXX/data/
    # schema should be in <table>.schema
    # data should be in <table>.tsv.gz
    def loadData(self):
        self.connect2Db()

        self.logger.info("Loading data from %s" % self._input_dirname)
        files = os.listdir(self._input_dirname)

        schemaFiles = getSchemaFiles(self._input_dirname)
        
        for f in schemaFiles:
            tableName = f[:-7]
            schemaFile = os.path.join(self._input_dirname, f)
            zipped_data_file = os.path.join(self._input_dirname, "%s.tsv.gz" % tableName)
            # check if the corresponding data file exists
            if not os.path.exists(zipped_data_file):
                raise Exception, "File: '%s' not found" %  zipped_data_file
            # uncompress data file into temp location
            # TODO : use a pipe instead of a tempfile
            
            tmp_suffix = (".%s.tsv" % tableName)
            tmp = tempfile.NamedTemporaryFile(suffix=tmp_suffix, dir=self._out_dirname,delete=False)
            tmp_data_file = tmp.name

            if os.path.exists(tmp_data_file):
                os.unlink(tmp_data_file)
                
            # os.mkfifo(tmp_data_file)
            
            self.logger.info(" ./Uncompressing: %s into %s" %  (zipped_data_file, tmp_data_file))
            gunzip(zipped_data_file, tmp_data_file)

            self.logger.info("Loading schema of %s" % tableName)
            loadSchema(self._sqlInterface, self._input_dirname, tableName, "schema", self._schemaDict)

            # load the table. Note, how we do it depends
            # whether we load to plain mysql or qserv
            # remove temporary file
            # treat Object and Source differently, they need to be partitioned
            if self._mode == 'qserv' and (tableName == 'Object' or tableName == 'Source'):
                self.loadPartitionedTable(tableName, schemaFile, tmp_data_file)
            else:
                self.loadRegularTable(tableName, schemaFile, tmp_data_file)

            os.unlink(tmp_data_file)
#TODO
#self.disconnect()

    def loadRegularTable(self, tableName, schemaFile, dataFile):        
        self._sqlInterface['cmd'].executeFromFileWithMySQLCLient(schemaFile)
        query = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" % (dataFile, tableName)
        self.logger.info("Loading data:  %s" % dataFile)
        self._sqlInterface['cmd'].executeWithMySQLCLient(query)

    def loadPartitionedTable(self, table, schemaFile, data_filename):
        ''' Partition and load Qserv data like Source and Object
        '''

        stripes = self.config['qserv']['stripes']

        data_config = dict()
        data_config['Object']=dict()
        data_config['Object']['ra-column'] = self._schemaDict['Object'].indexOf("`ra_PS`")
        data_config['Object']['decl-column'] = self._schemaDict['Object'].indexOf("`decl_PS`")
        
        # zero-based index

        # FIXME : return 229 instead of 227
        #data_config['Object']['chunk-column-id'] = self._schemaDict['Object'].indexOf("`chunkId`") -2
        
        # for test case01
        #data_config['Object']['ra-column'] = 2
        #data_config['Object']['decl-column'] = 4
        data_config['Object']['chunk-column-id'] = 227

        data_config['Source']=dict()
        # Source will be placed on the same chunk that its related Object
        data_config['Source']['ra-column'] = self._schemaDict['Source'].indexOf("`raObject`")
        data_config['Source']['decl-column'] = self._schemaDict['Source'].indexOf("`declObject`")

        # for test case01
        #data_config['Source']['ra-column'] = 33
        #data_config['Source']['decl-column'] = 34

        # chunkId and subChunkId will be added
        data_config['Source']['chunk-column-id'] = None

        
        self.logger.debug("Data configuration : %s" % data_config)
        
        # load schema
        load_schema_cmd = [
            self.config['bin']['mysql'], 
            '--socket', self.config['mysqld']['sock'],
            '-u', self.config['mysqld']['user'], 
            '-p'+self.config['mysqld']['pass'],
            self._dbName,
            '-e', 'Source %s' %  schemaFile
        ]

        self.logger.info("  Loading schema %s" % schemaFile)
        commons.run_command(load_schema_cmd)

        # TODO : create index and alter table with chunkId and subChunkId
        # "\nCREATE INDEX obj_objectid_idx on Object ( objectId );\n";

        # partition data          
        partition_dirname = os.path.join(self._out_dirname,table+"_partition")
        if os.path.exists(partition_dirname):
            shutil.rmtree(partition_dirname)
        os.makedirs(partition_dirname)

        # python %s -PObject -t 2  -p 4 %s --delimiter '\t' -S 10 -s 2 --output-dir %s" % (self.partition_scriptname, data_filename, partition_dirname
        partition_data_cmd = [
            self.config['bin']['python'],
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
            self.config['bin']['python'], 
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

        insert_sql =  "INSERT INTO LSST__{1} SELECT {2}Id, chunkId, subChunkId FROM {0}.{1}_%s;\n".format(self._dbName,table,table.lower())

        chunk_id_list=self.qservDataLoader.getNonEmptyChunkIds()

        self.logger.info("Non empty data chunks list : %s " %  str(chunk_id_list))

        for chunkId in chunk_id_list :
            tmp =  insert_sql % chunkId
            sql += "\n" + tmp

        sql_cmd = [
            self.config['bin']['mysql'], 
            '-S', self.config['mysqld']['sock'],
            '-u', 'root', 
            '-p'+self.config['mysqld']['pass'],
            '-e', sql
        ]
        out = commons.run_command(sql_cmd)
        self.logger.info("%s table for empty chunk created, and meta loaded : %s" % (table,out))

        # Create xrootd query directories
        self.qservDataLoader.init_worker_xrd_dirs(chunk_id_list)

        # Create etc/emptychunk.txt
        empty_chunks_filename = os.path.join(self.config['qserv']['base_dir'],"etc","emptyChunks.txt")
        stripes=self.config['qserv']['stripes']
        self.qservDataLoader.createEmptyChunksFile(stripes, chunk_id_list,  empty_chunks_filename)

        raw_input("Qserv mono-node database filled with partitionned '%s' data.\nPress Enter to continue..." % table)

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
        (options, args) = op.parse_args()

        if not set(options.mode).issubset(set(mode_option_values)) :
            print "%s: --mode flag set with invalid value" % script_name
            print "Try `%s --help` for more information." % script_name
            exit(1)

        return options

    def run(self, options):

        #if not os.access(self._out_dirname, os.F_OK):
        #    os.makedirs(self._out_dirname)

        # cleanup of previous tests
        if os.path.exists(self._out_dirname):
            shutil.rmtree(self._out_dirname)
        os.makedirs(self._out_dirname)

        for mode in options.mode:
            self._mode=mode

            if (self._mode=='mysql'):
                self._dbName = "qservTest_case%s_%s" % (self._case_id, self._mode) 
                self.logger.info("Creation of a SQL Interface")
                self._sqlInterface['cmd'] = SQLCmd.SQLCmd(config = self.config,
                                                          mode = SQLMode.MYSQL_SOCK,
                                                          database = self._dbName
                                                          )
                self._sqlInterface['sock'] = SQLConnection.SQLConnection(
                                                          config = self.config,
                                                          mode = SQLMode.MYSQL_SOCK,
                                                          database = self._dbName
                                                          )
                self._sqlInterface['query'] = self._sqlInterface['cmd']
                
                
            elif (self._mode=='qserv'):
                self._dbName= 'LSST'                
                self.logger.info("Creation of a SQL Interface")
                self.qservDataLoader = QservDataLoader.QservDataLoader(self.config, self._dbName, self._logFilePrefix)
                self.qservDataLoader.initDatabases()
                self._sqlInterface['sock'] =  self.qservDataLoader._sqlInterface['sock']
                self._sqlInterface['cmd'] =  self.qservDataLoader._sqlInterface['cmd']
                self._sqlInterface['query'] = SQLCmd.SQLCmd(config = self.config,
                                                          mode = SQLMode.MYSQL_PROXY,
                                                          database = self._dbName
                                                          )
                
    
            self.loadData()     
            self.runQueries(options.stop_at)

    def __del__(self):
        self.logger.info("Calling  destructor for QservTestsRunner()")

def main():

    qserv_test_runner = QservTestsRunner()
    options = qserv_test_runner.parseOptions()  
    qserv_test_runner.configure(options.config_dir, options.case_no, options.out_dirname)
    qserv_test_runner.run(options)

if __name__ == '__main__':
    main()
