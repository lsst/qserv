from  lsst.qserv.admin import commons
import logging
import os
import re
import shutil
import SQLCmd
import SQLConnection
import SQLInterface
import SQLMode

class QservDataLoader():

    def __init__(self, config, db_name, log_file_prefix='qserv-loader', logging_level=logging.DEBUG):
        self.config = config

        self._dbName = db_name

        self.logger = commons.console_logger(logging_level)
        self.logger = commons.file_logger(
            log_file_prefix,
            log_path=self.config['qserv']['log_dir']
        )

        sock_connection_params = {
            'config' : self.config,
            'mode' : SQLMode.MYSQL_SOCK,
            'database' : self._dbName
            }

        self._sqlInterface = dict()
        self._sqlInterface['sock'] = SQLConnection.SQLConnection(**sock_connection_params)
        self._sqlInterface['cmd'] = SQLCmd.SQLCmd(**sock_connection_params)

    def getNonEmptyChunkIds(self):
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

        return sorted(non_empty_chunk_list)
        
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

    def createEmptyChunksFile(self, stripes, chunk_id_list, empty_chunks_filename):
        f=open(empty_chunks_filename,"w")
        empty_chunks_list=[i for i in range(0,7201) if i not in chunk_id_list]
        for i in empty_chunks_list:
            f.write("%s\n" %i)
        f.close()

    def init_worker_xrd_dirs(self, non_empty_chunk_id_list):

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
