from  lsst.qserv.admin import commons
from  lsst.qserv.sql import const, cmd, connection, schema
import logging
import os
import re
import shutil

class MysqlDataLoader():

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
        self.chunk_id_list = None

    def createAndLoadTable(self, table_name, schema_filename, input_filename):
        self.logger.debug("MysqlDataLoader.createAndLoadTable(%s, %s, %s)" % (table_name, schema_filename, input_filename))

        if (table_name in self.dataConfig['partitionned-tables']) and ('Duplication' in self.dataConfig) and self.dataConfig['Duplication']:
            self.logger.info("Loading schema of duplicated table %s" % table_name)
            self.createPartitionedTable(table_name, schema_filename)
            self.loadPartitionedTable(table_name, input_filename)
        elif table_name in self.dataConfig['sql-views']:
            self.logger.info("Creating schema for table %s as a view" % table_name)
            self._sqlInterface['cmd'].executeFromFile(schema_filename)
        else:
            self.logger.info("Creating and loading non-partitionned table %s" % table_name)
            self._sqlInterface['cmd'].createAndLoadTable(table_name, schema_filename, input_filename, self.dataConfig['delimiter'])


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


    def loadPartitionedTable(self, table, data_filename):
        ''' Duplicate, partition and load MySQL data like Source and Object
        '''
        self.logger.info("-----\nMySQL Duplicating data for table  '%s' -----\n" % table)
        partition_dirname = self.duplicateAndPartitionData(table, data_filename)
        self.loadPartitionedData(partition_dirname,table)
        self.logger.info("-----\nMySQL database filled with partitionned '%s' data. -----\n" % table)


    def createPartitionedTable(self, table, schemaFile):
        self.logger.info("Creating partitionned table %s with schema %s" % (table, schemaFile))
        if table in self.dataConfig['partitionned-tables']:
            self._sqlInterface['cmd'].executeFromFile(schemaFile)
            self.alterTable(table)


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

        self.logger.info("Working in DB : %s.  LSST %s data duplicated and partitioned : \n %s"
                % (self._dbName, table,out))

        if data_filename_cleanup:
            commons.run_command(["gzip", data_filename])

        return partition_dirname

    def connectAndInitDatabase(self):

        self._sqlInterface['sock'] = connection.Connection(**self.sock_connection_params)

        self._sqlInterface['sock'].dropAndCreateDb(self._dbName)
        self._sqlInterface['sock'].setDb(self._dbName)

        cmd_connection_params =   self.sock_connection_params
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)


    def loadPartitionedData(self,partition_dirname,table):
        self.logger.info("MysqlDataLoader.loadPartitionedData(%s, %s)" % (partition_dirname,table))

        prefix = table + "_"
        for root, dirnames, filenames in os.walk(partition_dirname):
            for filename in filenames:
                if filename.startswith(prefix):
                    datafile = os.path.join(root, filename)
                    self.logger.info("Loading partitionned data from %s" % datafile)
                    self._sqlInterface['cmd'].loadData(datafile, table, self.dataConfig['delimiter'])
