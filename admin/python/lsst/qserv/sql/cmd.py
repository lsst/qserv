import logging

from  lsst.qserv.admin import commons

import const


# TODO: replace all SQL by SQLConnection    
class Cmd():
    """ SQLCmd is a class for managing SQL cmd via a shell client"""
    def __init__(self,
                 config,
                 mode,                 
                 database):
      
        """

      @param config:
      @param mode:
      @param database:
      """
        self.config = config
      
        self.logger = logging.getLogger()
        self.logger.debug("SQL cmd creation")
        
        self.buildMysqlCmd(mode,database)
        
    def buildMysqlCmd(self,mode,database):
        self._mysql_cmd = []
        
        self._mysql_cmd.append(self.config['bin']['mysql']) 
        
        if mode==const.MYSQL_PROXY :
            self._addQservCmdParams()
	elif mode==const.QSERV_LOAD :
	    self._addQservSockCmdParams()
        elif mode==const.MYSQL_SOCK :
            self._addMySQLSockCmdParams()
        elif mode==const.MYSQL_NET :
            self._addMySQLNetCmdParams()
        
        self._mysql_cmd.append("--batch")

        if (database is not None):
             self._mysql_cmd.append( "%s" % database )
        
        self.logger.debug("SQLCmd._mysql_cmd %s" % self._mysql_cmd )
        
        self._mysql_cmd.append("-e")
        
    def _addQservCmdParams(self):
        self._mysql_cmd.append( "--host=%s" % self.config['qserv']['master'])
        self._mysql_cmd.append( "--port=%s" % self.config['mysql_proxy']['port'])
        self._mysql_cmd.append("--user=%s" % self.config['qserv']['user'])

    def _addQservSockCmdParams(self):
	""" User root should not be used for data loading, only for database creation and to give rights. Use qsmaster user instead. """
        self._mysql_cmd.append("--sock=%s" % self.config['mysqld']['sock'])
	self._mysql_cmd.append("--user=%s" % self.config['qserv']['user'])

    def _addMySQLSockCmdParams(self):
        self._mysql_cmd.append("--sock=%s" % self.config['mysqld']['sock'])
        self._mysql_cmd.append("--user=%s" % self.config['mysqld']['user'])
        self._mysql_cmd.append("--password=%s" % self.config['mysqld']['pass'])
        
    def _addMySQLNetCmdParams(self):
        self._mysql_cmd.append("--host=%s" % self.config['qserv']['master'])
        self._mysql_cmd.append("--port=%s" % self.config['mysqld']['port'])
        self._mysql_cmd.append("--user=%s" % self.config['mysqld']['user'])
        self._mysql_cmd.append("--password=%s" % self.config['mysqld']['pass'])

    def execute(self, query, stdout = None):
      """ Some queries cannot run correctly through MySQLdb, so we must use MySQL client instead """
      self.logger.debug("SQLCmd.execute:  %s" % query)
      commandLine = self._mysql_cmd + [query]
      commons.run_command(commandLine, stdout_file=stdout)
      
    def executeFromFile(self, filename, stdout = None):
      """ Some queries cannot run correctly through MySQLdb, so we must use MySQL client instead """
      self.logger.debug("SQLCmd.executeFromFile:  %s" % filename)
      commandLine = self._mysql_cmd + ["SOURCE %s" % filename]
      commons.run_command(commandLine, stdout_file=stdout)
        
    def createAndLoadTable(self, tableName, schemaFile, dataFile, delimiter):
        self.logger.debug("CMD.createAndLoadTable(%s, %s, %s, %s)" % (tableName, schemaFile, dataFile, delimiter))
        self.executeFromFile(schemaFile)
        self.loadData(dataFile, tableName, delimiter)

    def loadData(self, dataFile, tableName,delimiter):
        query = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s';" % (dataFile, tableName,delimiter)
        self.logger.debug("CMD.createAndLoadTable: Loading data  %s" % dataFile)
        self.execute(query)
        

# ----------------------------------------
#    
#  def createDatabase(sqlInterface, database):
#    SQL_query = "CREATE DATABASE %s;" % database
#    sqlInterface.execute(SQL_query)
#  
#  def createDatabaseIfNotExists(sqlInterface, database):
#    SQL_query = "CREATE DATABASE IF NOT EXISTS %s;" % database
#    sqlInterface.execute(SQL_query)
#  
#  def dropDatabaseIfExists(sqlInterface, database):
#    SQL_query = "DROP DATABASE IF EXISTS %s;" % database
#    sqlInterface.execute(SQL_query)
#  
#  def grantAllRightsOnDatabaseTo(sqlInterface, database, user):
#    SQL_query = "GRANT ALL ON %s TO %s" % (database, user)
#    sqlInterface.execute(SQL_query)
#  
#  def createTable(sqlInterface, tablename, description):
#    SQL_query = "CREATE TABLE %s (%s);\n" % (tablename, description)
#    # tablename = "LSST__%s" % tablename
#    # description = "%sId BIGINT NOT NULL PRIMARY KEY, x_chunkId INT, x_subChunkId INT" % tablename.lower()
#    sqlInterface.execute(SQL_query)
#  
#  def insertIntoTableFrom(sqlInterface, tablename, query):
#    SQL_query = "INSERT INTO %s %s;\n" % (tablename, query)
#    # tablename = "LSST__%s_%s" % (tablename, chunkId)
#    # query = "SELECT %sId, chunkId, subChunkId FROM %s.%s" % ( tablename.lower(), database, tablename )
#    sqlInterface.execute(SQL_query)

  
      
