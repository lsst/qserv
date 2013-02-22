import MySQLdb as sql
import os.path
import sys
from  lsst.qserv.admin import commons

# TODO: replace all SQL by SQLInterface    
class SQLInterface():
    """ SQLInterface is a class for managing SQL connection and executing queries"""
    
    def __init__(self,
                 mysql_client,
                 user,                 
                 database,
                 password = None, 
                 socket = None,
                 host = None,
                 port = None,
                 logger = None):
      
        self.logger = logger
        self.logger.info("SQLInterface creation ")


        socket_connection_params = { 
          'user': user,
          'db' : database,
          'unix_socket' : socket
          }

        if (password is not None):
           socket_connection_params['passwd']=password

        #if (socket is not None):
        #  socket_connection_params['unix_socket']=socket
           
        try :
          self._connection = sql.connect(**socket_connection_params)
        except:
          self.logger.fatal("SQL connection error")
          sys.exit(1)

        self._cursor = None

        mysql_cmd = [ mysql_client ]
        
        if (host is not None):
          mysql_cmd.append('--host=%s' % host)
          
        if (port is not None):
          mysql_cmd.append('--port=%s'% port)

        if (user is not None):
          mysql_cmd.append('--user=%s' % user)

        if (socket is not None):
          mysql_cmd.append('--socket=%s' % socket)

        if (password is not None):
          mysql_cmd.append('--password=%s'% password)
          
        if (database is not None):
          mysql_cmd.append(database)

        mysql_cmd.append("--batch")
        mysql_cmd.append("-e")
        
        self._mysql_cmd = mysql_cmd
        
    def __del__(self):
        self.logger.info("SQLInterface: Calling destructor %s" % self._mysql_cmd)
        self._connection.close()

    def disconnect(self):
        self.logger.info("SQLInterface: disconnecting")
        self._connection.close()
        
    def execute(self, query):
        self._cursor = self._connection.cursor()
        self.logger.info("Executing query: %s" % query)
        self._cursor.execute(query)
        result = self._cursor.fetchall()
        self._cursor.close()
        return result

    def executeFromFile(self, filename):
      if os.path.exists(filename):
        # What a pity: SOURCE doesn't work with MySQLdb !
        self.logger.info("SQLInterface.executeFromFile:  %s" % filename)
        sql = open(filename).read()
        return self.execute(sql)
      else:
        raise Exception, "File: '%s' not found" % filename

    def executeWithMySQLCLient(self, query, stdout = None):
      """ Some queries cannot run correctly through MySQLdb, so we must use MySQL client instead """
      self.logger.info("SQLInterface.executeWithMySQLCLient:  %s" % query)
      commandLine = self._mysql_cmd + [query]
      commons.run_command(commandLine, stdout_file=stdout)
      
    def executeFromFileWithMySQLCLient(self, filename, stdout = None):
      """ Some queries cannot run correctly through MySQLdb, so we must use MySQL client instead """
      self.logger.info("SQLInterface.executeFromFile:  %s" % filename)
      commandLine = self._mysql_cmd + ["SOURCE %s" % filename]
      commons.run_command(commandLine, stdout_file=stdout)
        

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

  
      
