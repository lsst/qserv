from  lsst.qserv.admin import commons
from  lsst.qserv.sql import const, cmd, connection, schema
import logging
import os
import re
import shutil

class QservDataLoader():

    def __init__(self, config, data_config, db_name, out_dirname, log_file_prefix='qserv-loader', logging_level=logging.DEBUG):
        self.config = config
        self.dataConfig = data_config
        self._dbName = db_name

        self._out_dirname = out_dirname

        #self.logger = commons.console_logger(logging_level)
        #self.logger = commons.file_logger(
        #    log_file_prefix,
        #    log_path=self.config['qserv']['log_dir']
        #)
        self.logger = logging.getLogger()
        sock_connection_params = {
            'config' : self.config,
            'mode' : const.MYSQL_SOCK,
            'database' : self._dbName
            }

        self._sqlInterface = dict()
        self._sqlInterface['sock'] = connection.Connection(**sock_connection_params)
        self._sqlInterface['cmd'] = cmd.Cmd(**sock_connection_params)

    def loadPartitionedTable(self, table, schemaFile, data_filename):
        ''' Partition and load Qserv data like Source and Object
        '''

        # load schema with chunkId and subChunkId
        self.logger.info("  Loading schema %s" % schemaFile)
        self._sqlInterface['sock'].executeFromFile(schemaFile)
        # TODO : create index and alter table with chunkId and subChunkId
        # "\nCREATE INDEX obj_objectid_idx on Object ( objectId );\n";

        partition_dirname = self.partitionData(table,data_filename)
        
        self.loadPartitionedData(partition_dirname,table)

        self.workerCreateTable1234567890(table)

        chunk_id_list=self.workerGetNonEmptyChunkIds()
        self.masterCreateAndFeedMetaTable(table,chunk_id_list)

        # Create xrootd query directories
        self.workerCreateXrootdExportDirs(chunk_id_list)

        # Create etc/emptychunk.txt
        empty_chunks_filename = os.path.join(self.config['qserv']['base_dir'],"etc","emptyChunks.txt")
        stripes=self.config['qserv']['stripes']
        self.masterCreateEmptyChunksFile(stripes, chunk_id_list,  empty_chunks_filename)

        self.logger.info("-----\nQserv mono-node database filled with partitionned '%s' data.-----\n" % table)

    def initDatabases(self): 
        self.logger.info("Initializing databases %s, qservMeta" % self._dbName)
        sql_instructions= [
            "DROP DATABASE IF EXISTS %s" % self._dbName,
            "CREATE DATABASE %s" % self._dbName,
            # TODO : "GRANT ALL ON %s.* TO '%s'@'*'" % (self._dbName, self._qservUser, self._qservHost)
            "GRANT ALL ON %s.* TO '*'@'*'" % (self._dbName),
            "DROP DATABASE IF EXISTS qservMeta",
            "CREATE DATABASE qservMeta",
            "USE %s" %  self._dbName
            ]
        
        for sql in sql_instructions:
            self._sqlInterface['sock'].execute(sql)

    def workerGetNonEmptyChunkIds(self):
        non_empty_chunk_list=[]

        sql = "SHOW TABLES IN %s LIKE \"Object\_%%\";" % self._dbName 
        rows = self._sqlInterface['sock'].execute(sql)

        for row in rows:
            self.logger.debug("Chunk table found : %s" % row)
            pattern = re.compile(r"^Object_([0-9]+)$")
            m = pattern.match(row[0])
            if m:
                chunk_id = m.group(1)
                non_empty_chunk_list.append(int(chunk_id))
                self.logger.debug("Chunk number : %s" % chunk_id)
        chunk_list = sorted(non_empty_chunk_list)
        self.logger.info("Non empty data chunks list : %s " %  chunk_list)
        return chunk_list

    def masterCreateEmptyChunksFile(self, stripes, chunk_id_list, empty_chunks_filename):
        f=open(empty_chunks_filename,"w")
        # TODO : replace 7201 by an operation with stripes
        empty_chunks_list=[i for i in range(0,7201) if i not in chunk_id_list]
        for i in empty_chunks_list:
            f.write("%s\n" %i)
        f.close()

    def workerCreateXrootdExportDirs(self, non_empty_chunk_id_list):

        # match oss.localroot in etc/lsp.cf
        xrootd_run_dir = os.path.join(self.config['qserv']['base_dir'],'xrootd-run')

        # TODO : read 'q' and 'result' in etc/lsp.cf
        xrd_query_dir = os.path.join(xrootd_run_dir, 'q', self._dbName) 
        xrd_result_dir = os.path.join(xrootd_run_dir, 'result') 

        if os.path.exists(xrd_query_dir):
            self.logger.info("Emptying existing xrootd query dir : %s" % xrd_query_dir)
            shutil.rmtree(xrd_query_dir)
        os.makedirs(xrd_query_dir)
        self.logger.info("Making placeholders")

        for chunk_id in non_empty_chunk_id_list:
            xrd_file = os.path.join(xrd_query_dir,str(chunk_id))
            open(xrd_file, 'w').close() 

        if os.path.exists(xrd_result_dir):
            self.logger.info("Emptying existing xrootd result dir : %s" % xrd_result_dir)
            shutil.rmtree(xrd_result_dir)
        os.makedirs(xrd_result_dir)

    def partitionData(self,table, data_filename):
        # partition data          
        
        partition_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "partition.py")
        partition_dirname = os.path.join(self._out_dirname,table+"_partition")
        if os.path.exists(partition_dirname):
            shutil.rmtree(partition_dirname)
        os.makedirs(partition_dirname)
            
            # python %s -PObject -t 2  -p 4 %s --delimiter '\t' -S 10 -s 2 --output-dir %s" % (self.partition_scriptname, data_filename, partition_dirname
        partition_data_cmd = [
            self.config['bin']['python'],
            partition_scriptname,
            '--output-dir', partition_dirname,
            '--chunk-prefix', table,
            '--theta-column', str(self.dataConfig[table]['ra-column']),
            '--phi-column', str(self.dataConfig[table]['decl-column']),
            '--num-stripes', self.config['qserv']['stripes'],
            '--num-sub-stripes', self.config['qserv']['substripes'],
            '--delimiter', '\t'
            ]
        
        if self.dataConfig[table]['chunk-column-id'] != None :
            partition_data_cmd.extend(['--chunk-column', str(self.dataConfig[table]['chunk-column-id'])])
            
        partition_data_cmd.append(data_filename)
            
        out = commons.run_command(partition_data_cmd)
        
        self.logger.info("Working in DB : %s.  LSST %s data partitioned : \n %s"
                % (self._dbName, table,out))

        return partition_dirname


    def loadPartitionedData(self,partition_dirname,table):

        load_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "loader.py")

    # TODO : remove hard-coded param : qservTest_caseXX_mysql => check why model table already exists in self._dbName
        load_partitionned_data_cmd = [
            self.config['bin']['python'], 
            load_scriptname,
            '--user=%s' % self.config['mysqld']['user'], 
            '--password=%s' % self.config['mysqld']['pass'],
            '--database=%s' % self._dbName,
            "%s:%s" %
            (self.config['qserv']['master'],self.config['mysqld']['port']),
            partition_dirname,
            "%s.%s" % (self._dbName, table)
            ]
        # python master/examples/loader.py --verbose -u root -p changeme --database qservTest_case01_qserv -D clrlsst-dbmaster.in2p3.fr:13306 /opt/qserv-dev/tmp/Object_partition/ qservTest_case01_mysql.Object
        out = commons.run_command(load_partitionned_data_cmd)
        self.logger.info("Partitioned %s data loaded : %s" % (table,out))

    def workerCreateTable1234567890(self,table):
        sql =  "CREATE TABLE {0}.{1}_1234567890 LIKE {1};\n".format(self._dbName,table)
        self._sqlInterface['sock'].execute(sql)

        self.logger.info("%s table for empty chunk created" % table)

    def masterCreateAndFeedMetaTable(self,table,chunk_id_list):

        sql = "USE qservMeta;"
        sql += "CREATE TABLE LSST__{0} ({1}Id BIGINT NOT NULL PRIMARY KEY, x_chunkId INT, x_subChunkId INT);\n".format(table, table.lower())

        # TODO : scan data on all workers here, with recovery on error
        insert_sql =  "INSERT INTO LSST__{1} SELECT {2}Id, chunkId, subChunkId FROM {0}.{1}_%s;".format(self._dbName,table,table.lower())
        for chunkId in chunk_id_list :
            tmp =  insert_sql % chunkId
            sql += "\n" + tmp

        self._sqlInterface['sock'].execute(sql)
        self.logger.info("meta table created and loaded for %s" % table)

    def convertSchemaFile(self, tableName, schemaFile, newSchemaFile, schemaDict):
    
        self.logger.debug("Converting schema file for table : %s" % tableName)
        mySchema = schema.SQLSchema(tableName, schemaFile)    
        mySchema.read()
        mySchema.replaceField("`_chunkId`", "`chunkId`")
        mySchema.replaceField("`_subchunkId`", "`subchunkId`")

        if (tableName == "Object"):
            mySchema.deletePrimaryKey()
            mySchema.createIndex("`obj_objectid_idx`", "Object", "`objectId`")

        mySchema.write(newSchemaFile)
        schemaDict[tableName]=mySchema
    
        return mySchema

    # TODO: do we have to drop old schema if it exists ?  
    def loadPartitionedSchema(self, directory, table, schemaSuffix, schemaDict):
        # TODO: read meta data to know which table must be partitionned.
        partitionnedTables = ["Object", "Source"]
        schemaFile = os.path.join(directory, table + "." + schemaSuffix)
        if table in partitionnedTables:      
            newSchemaFile = os.path.join(self._out_dirname, table + "_converted" + "." + schemaSuffix)
            self.convertSchemaFile(table, schemaFile, newSchemaFile, schemaDict)
            self._sqlInterface['cmd'].executeFromFile(newSchemaFile)
            os.unlink(newSchemaFile)


    # TODO : not used now
    def findAllDataInStripes(self):
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
