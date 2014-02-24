# Loads partition and load data set and then configure Qserv
# used by test procedure

# WARNING : this procedure is redundant with :
# - admin/bin/qserv-chunkload.py (which was used for data-loading on CC-IN2P3 cluster)
# - admin/custom/bin/qserv-admin.pl also have a data loading procedure for
# PT1.1 data set
# it should be unified in a global data loading procedure :
# https://dev.lsstcorp.org/trac/wiki/db/Qserv/DataLoading


from  lsst.qserv.admin import commons
from  lsst.qserv.sql import const, cmd, connection, schema
import logging
import os
import re
import shutil
from os.path import expanduser

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
        self.sock_connection_params = {
            'config' : self.config,
            'mode' : const.MYSQL_SOCK
            }

        self._sqlInterface = dict()

    def createQmsDatabase(self):

        qms_script = os.path.join(self.config['qserv']['base_dir'],"qserv", "admin", "bin", "qms_setup.sh")
        qms_setup_cmd = [
            qms_script,
            self.config['qserv']['base_dir'],
            self._dbName,
            self.dataConfig['data-name']
            ]
        out = commons.run_command(qms_setup_cmd)
        self.logger.info("QMS meta successfully loaded for db : %s" % self._dbName)

    def createAndLoadTable(self, table_name, schema_filename, input_filename):
        self.logger.debug("QservDataLoader.createAndLoadTable(%s, %s, %s)" % (table_name, schema_filename, input_filename))

        if table_name in self.dataConfig['partitioned-tables']:
            self.logger.info("Loading schema of partitioned table %s" % table_name)
            self.createPartitionedTable(table_name, schema_filename)
            self.loadPartitionedTable(table_name, input_filename)
        elif table_name in self.dataConfig['sql-views']:
            self.logger.info("Creating schema for table %s as a view" % table_name)
            self._sqlInterface['cmd'].executeFromFile(schema_filename)
        else:
            self.logger.info("Creating and loading non-partitioned table %s" % table_name)
            self._sqlInterface['cmd'].createAndLoadTable(table_name, schema_filename, input_filename, self.dataConfig['delimiter'])

    def loadPartitionedTable(self, table, data_filename):
        ''' Duplicate, partition and load Qserv data like Source and Object
        '''

        # TODO : create index and alter table with chunkId and subChunkId
        # "\nCREATE INDEX obj_objectid_idx on Object ( objectId );\n";

        self.logger.info("Partitioning and loading data for table  '%s' in Qserv mono-node database" % table)

        if ('Duplication' in self.dataConfig) and self.dataConfig['Duplication']:
            self.logger.info("-----\nQserv Duplicating data for table  '%s' -----\n" % table)
            partition_dirname = self.duplicateAndPartitionData(table, data_filename)
        else:
            partition_dirname = self.partitionData(table, data_filename)

        self.loadPartitionedData(partition_dirname,table)


    def configureQservMetaEmptyChunk(self):
        
        self.logger.info("Configuring Qserv mono-node database")

        
        chunk_id_list=self.workerGetNonEmptyChunkIds()
        self.masterCreateMetaDatabase()
        for table in self.dataConfig['partitioned-tables']:
            self.workerCreateTable1234567890(table)
            self.masterCreateAndFeedMetaTable(table,chunk_id_list)
            
        for view in self.dataConfig['partitioned-sql-views']:
            self.workerCreateView1234567890(view)
            self.masterCreateAndFeedMetaTable(view,chunk_id_list)
        
        # Create etc/emptychunk.txt
        empty_chunks_filename = os.path.join(self.config['qserv']['base_dir'],"etc","emptyChunks.txt")
        self.masterCreateEmptyChunksFile(chunk_id_list,  empty_chunks_filename)


    def connectAndInitDatabase(self):

        self._sqlInterface['sock'] = connection.Connection(**self.sock_connection_params)

        self.logger.info("Initializing Qserv databases : %s, qservMeta" % self._dbName)
        sql_instructions= [
            "DROP DATABASE IF EXISTS %s" % self._dbName,
            "CREATE DATABASE %s" % self._dbName,
            # TODO : "GRANT ALL ON %s.* TO '%s'@'*'" % (self._dbName, self._qservUser, self._qservHost)
            "GRANT ALL ON %s.* TO 'qsmaster'@'localhost'" % (self._dbName),
            "USE %s" %  self._dbName
            ]

        for sql in sql_instructions:
            self._sqlInterface['sock'].execute(sql)

        cmd_connection_params =   self.sock_connection_params
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)

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

    def masterCreateEmptyChunksFile(self, chunk_id_list, empty_chunks_filename):
        f=open(empty_chunks_filename,"w")
        # TODO : replace 7201 by an operation with stripes
        stripes=self.dataConfig['num-stripes']
        top_chunk = 2 * stripes * stripes
        empty_chunks_list=[i for i in range(0,top_chunk) if i not in chunk_id_list]
        for i in empty_chunks_list:
            f.write("%s\n" %i)
        f.close()

    def duplicateAndPartitionData(self, table, data_filename):
        self.logger.info("Duplicating and partitioning table  '%s' from file '%s'\n" % (table, data_filename))

        partition_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "partition.py")
        partition_dirname = os.path.join(self._out_dirname,table+"_partition")

        if os.path.exists(partition_dirname):
            shutil.rmtree(partition_dirname)
        os.makedirs(partition_dirname)

        schema_filename = os.path.join(self.dataConfig['dataDirName'], table + self.dataConfig['schema-extension'])
        data_filename = os.path.join(self.dataConfig['dataDirName'], table + self.dataConfig['data-extension'])

        data_filename_cleanup = False
        data_filename_zipped = data_filename + self.dataConfig['zip-extension']
        if (not os.path.exists(data_filename)) and os.path.exists(data_filename_zipped):
            commons.run_command(["gunzip", data_filename_zipped])
            data_filename_cleanup = True
        if not os.path.exists(data_filename):
            raise Exception, "File: %s not found" % data_filename

        chunker_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "makeChunk.py")

        chunker_cmd = [
            self.config['bin']['python'],
            chunker_scriptname,
            '--output-dir', partition_dirname,
            '--delimiter', self.dataConfig['delimiter'],
            '-S', str(self.dataConfig['num-stripes']),
            '-s', str(self.dataConfig['num-substripes']),
            '--dupe',
            '--node-count', str(self.dataConfig['nbNodes']),
            '--node=' + str(self.dataConfig['currentNodeID']),
            '--chunk-prefix=' + table,
            '--theta-name=' + self.dataConfig[table]['ra-fieldname'],
            '--phi-name=' + self.dataConfig[table]['decl-fieldname'],
            '--schema', schema_filename,
            data_filename
            ]

        out = commons.run_command(chunker_cmd)

        self.logger.debug("Data for {0}.{1} duplicated and partitioned  (output :%s)" % (self._dbName, table,out))

        if data_filename_cleanup:
            commons.run_command(["gzip", data_filename])

        return partition_dirname


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
            '--num-stripes=%s' % self.dataConfig['num-stripes'],
            '--num-sub-stripes=%s' % self.dataConfig['num-substripes'],
            '--delimiter', self.dataConfig['delimiter']
            ]

        if self.dataConfig[table]['chunk-column-id'] != None :
            partition_data_cmd.extend(['--chunk-column', str(self.dataConfig[table]['chunk-column-id'])])

        partition_data_cmd.append(data_filename)

        out = commons.run_command(partition_data_cmd)

        self.logger.debug("Data for {0}.{1} partitioned  (output :{2})".format(self._dbName, table,out))

        return partition_dirname


    def loadPartitionedData(self,partition_dirname,table):

        load_scriptname = os.path.join(self.config['qserv']['base_dir'],"qserv", "master", "examples", "loader.py")

    # TODO : remove hard-coded param : qservTest_caseXX_mysql => check why model table already exists in self._dbName
        load_partitioned_data_cmd = [
            self.config['bin']['python'],
            load_scriptname,
            '--user=%s' % self.config['mysqld']['user'],
            '--password=%s' % self.config['mysqld']['pass'],
            '--database=%s' % self._dbName
            ]

        # if (table in self.dataConfig['partitioned-tables']):
        #     load_partitioned_data_cmd.extend(['--drop-primary-key', 'Overlap'])

        load_partitioned_data_cmd.extend( [
            "%s:%s" %
            ("127.0.0.1",self.config['mysqld']['port']),
            partition_dirname,
            "%s.%s" % (self._dbName, table)
            ])

        # python master/examples/loader.py --verbose -u root -p changeme --database qservTest_case01_qserv -D clrlsst-dbmaster.in2p3.fr:13306 /opt/qserv-dev/tmp/Object_partition/ qservTest_case01_mysql.Object
        out = commons.run_command(load_partitioned_data_cmd)
        self.logger.info("Partitioned {0} data loaded (stdout : {1})".format(table,out))

    def workerCreateTable1234567890(self,table):
        sql =  "CREATE TABLE {0}.{1}_1234567890 LIKE {0}.{1};\n".format(self._dbName,table)
        self._sqlInterface['sock'].execute(sql)

        self.logger.info("%s table for empty chunk created" % table)
        
    def workerCreateView1234567890(self,table):
        
        create_view_sql="SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}';".format(self._dbName,table)
        rename_view_sql =  "RENAME TABLE {0}.{1} TO {0}.{1}_1234567890;".format(self._dbName,table)
        self._sqlInterface['sock'].execute(rename_view_sql)
        self._sqlInterface['sock'].execute(create_view_sql)

        self.logger.info("%s view for empty chunk created" % table)

    def masterCreateMetaDatabase(self):
        sql_instructions= [
            "DROP DATABASE IF EXISTS qservMeta",
            "CREATE DATABASE qservMeta"
            ]
        for sql in sql_instructions:
            self._sqlInterface['sock'].execute(sql)

    def masterCreateAndFeedMetaTable(self,table,chunk_id_list):

	meta_table_prefix = "LSST__"
	#meta_table_prefix = "%s__" % self._dbName

	meta_table_name = meta_table_prefix + table

        sql = "USE qservMeta;"
        sql += "CREATE TABLE {0} ({1}Id BIGINT NOT NULL PRIMARY KEY, chunkId INT, subChunkId INT);\n".format(meta_table_name, table.lower())

        # TODO : scan data on all workers here, with recovery on error
        insert_sql =  "INSERT INTO {0} SELECT {1}Id, chunkId, subChunkId FROM {2}.{3}_%s;".format(meta_table_name, table.lower(), self._dbName, table)
        for chunkId in chunk_id_list :
            tmp =  insert_sql % chunkId
            sql += "\n" + tmp

        self._sqlInterface['sock'].execute(sql)
        self.logger.info("meta table created and loaded for %s" % table)

    def convertSchemaFile(self, tableName, schemaFile, newSchemaFile):

        self.logger.debug("Converting schema file for table : %s" % tableName)
        mySchema = schema.SQLSchema(tableName, schemaFile)
        mySchema.read()

        # TODO schema should be correct before
        for field_name in ["chunkId","subChunkId"]:
            previous_field_name = "`_%s`" % field_name
            if not mySchema.hasField(field_name):
                if mySchema.hasField(previous_field_name):
                    self.logger.debug("Replacing field %s in schema %s" % (field_name, tableName))
                    mySchema.replaceField(previous_field_name, "`%s`" %field_name)
                else:
                    self.logger.debug("Adding field %s in schema %s" % (field_name, tableName))
                    mySchema.addField("`%s`" %field_name, "int(11)", ("default", "NULL"))

        if (tableName == "Object"):
            mySchema.deletePrimaryKey()
            mySchema.createIndex("`obj_objectid_idx`", "Object", "`objectId`")

        mySchema.write(newSchemaFile)

        return mySchema


    def alterTable(self, table):
        sql_statement = "SHOW COLUMNS FROM `%s` LIKE '%s'"
        for field_name in ["chunkId","subChunkId"]:
            sql =  sql_statement % (table,field_name)
            col = self._sqlInterface['sock']. execute(sql)
            if col:
                self.logger.debug("Table %s already contain column %s" % (table,field_name))
                continue
            else:
                sql =  sql_statement % (table,"_%s" % field_name)
                col = self._sqlInterface['sock']. execute(sql)
                if col:
                    self.logger.debug("Removing column _%s in table %s" % (field_name, table))
                    sql = 'ALTER TABLE %s DROP COLUMN _%s' % (table,field_name)
                    self._sqlInterface['sock']. execute(sql)
                else:
                    self.logger.debug("Adding column %s in table %s" % (field_name, table))
                sql = 'ALTER TABLE %s ADD %s int(11) NOT NULL' % (table,field_name)
                self._sqlInterface['sock']. execute(sql)
        # TODO add index creation w.r.t. dataConfig

    def createPartitionedTable(self, table, schemaFile):
        self.logger.info("Creating partitioned table %s with schema %s" % (table, schemaFile))
        if table in self.dataConfig['partitioned-tables']:
            self._sqlInterface['cmd'].executeFromFile(schemaFile)
            self.alterTable(table)

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
