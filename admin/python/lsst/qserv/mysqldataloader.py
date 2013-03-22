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

        if table_name in self.dataConfig['sql-views']:
            self._sqlInterface['cmd'].executeFromFile(schema_filename)
        else:
            self._sqlInterface['cmd'].createAndLoadTable(table_name, schema_filename, input_filename, self.dataConfig['delimiter'])

    def connectAndInitDatabases(self):

        self._sqlInterface['sock'] = connection.Connection(**self.sock_connection_params)

        self._sqlInterface['sock'].dropAndCreateDb(self._dbName)
        self._sqlInterface['sock'].setDb(self._dbName)

        cmd_connection_params =   self.sock_connection_params  
        cmd_connection_params['database'] = self._dbName
        self._sqlInterface['cmd'] = cmd.Cmd(**cmd_connection_params)

