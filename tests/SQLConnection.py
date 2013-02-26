import logging
import MySQLdb as sql
import os.path
import sys
from  lsst.qserv.admin import commons

# TODO: replace all SQL by SQLConnection    
class SQLConnection():
    """ SQLConnection is a class for managing SQL connection and executing queries"""
    def __init__(self,
                 config, 
                 mode,
                 database):
      
        self.logger = logging.getLogger()
        self.logger.info("SQLConnection creation ")


        socket_connection_params = { 
          'user': config['mysqld']['user'],
          'db' : database,
          'unix_socket' : config['mysqld']['sock'],
          }

        if (config['mysqld']['pass'] is not None):
           socket_connection_params['passwd']=config['mysqld']['pass']

        #if (socket is not None):
        #  socket_connection_params['unix_socket']=socket
           
        try :
          self._connection = sql.connect(**socket_connection_params)
        except:
          self.logger.fatal("SQL connection error %s" % socket_connection_params )
          sys.exit(1)

        self._cursor = None
       
    # TODO the mysqldb destructor should close the connection when object is deleted
    def __del__(self):
        self.logger.info("SQLConnection: Calling destructor, and closing connection")
        self._connection.close()

    def disconnect(self):
        self.logger.info("SQLConnection: disconnecting")
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
        self.logger.info("SQLConnection.executeFromFile:  %s" % filename)
        sql = open(filename).read()
        return self.execute(sql)
      else:
        raise Exception, "File: '%s' not found" % filename
        

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

  
      
