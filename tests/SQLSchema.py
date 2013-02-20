
# Test:
# import SQLSchema
# s = SQLSchema.SQLSchema("foo")
# s.read("/tmp/a.sql")


import SQLReader
import operator

def backquote(str):
  return "`" + str + "`"

class SQLSchema():
    """Schema class for management of SQL schema"""
    
    def __init__(self, tableName):
      # each field must have a positional parameter !
      self._tableName = tableName
      self._arity = 0
      self._primaryKey = None
      self._index = None
      self._keys = []
      self._fields = dict()
      self._engine = None
      self._prologue = []
      self._epilogue = []

    def indexOf(self,fieldName):
      (index, fieldSpecification) = self._fields[fieldName]
      return index
        
    def primaryKey(self, fieldName):
      self._primaryKey = fieldName

    def deletePrimaryKey(self):
      self._primaryKey = None

    def addKey(self, keyName, fieldName):
      self._keys.append((keyName, fieldName))
      
    def createIndex(self, indexName, tableName, fieldName):
      self._index = (indexName, tableName, fieldName)

    def deleteField(self, fieldName):
      del self._fields[fieldName]
    
    def replaceField(self, oldFieldName, newFieldName):
      if (oldFieldName in self._fields):
        self._fields[newFieldName] = self._fields[oldFieldName]
        del self.deleteField[oldFieldName]

    def _createFieldString(self, fieldName, dataType, constraints):
      if (constraints is None):
        return dataType
      else:
        return "%s %s" % ( dataType, " ".join(constraints) )
    
    def addField(self, fieldName, dataType, constraints = None):
      self._arity = self._arity + 1
      fieldSpecification = self._createFieldString(fieldName,
                                                   dataType,
                                                   constraints)
      self._fields[fieldName] = ( self._arity, fieldSpecification )

    def convertSQLToSchema(self, SQLSchema):
      for line in SQLSchema:
        if ((line[0] == "PRIMARY") and (line[1] == "KEY")):
          self.primaryKey(line[2])
        elif (line[0] == "KEY"):
          self.addKey(line[1], line[2])
        elif (len(line) == 2):
          (fieldName, dataType) = line
          self.addField(fieldName, dataType)
        elif (len(line) == 4):
          (fieldName, dataType, defaultStr, defaultValue) = line
          self.addField(fieldName, dataType, (defaultStr, defaultValue))
        else:
          print "convertSQLToSchema: unknown line : %s\n" % line
      return None
                
    def read(self, filename):
      """ Read a MySQL dump schema file """
      parsing = SQLReader.reader(filename)
      self._prologue = parsing["prologue"]
      self.convertSQLToSchema(parsing["schema"])
      self._engine = parsing["engine"]
      self._epilogue = parsing["epilogue"]
        
    def write(self, filename):
      with open(filename, 'w') as outfile:
      	for line in self._prologue:
      	  outfile.write(" ".join(line))
          outfile.write("\n")
      	  
      	outfile.write("CREATE TABLE `%s` (\n" % self._tableName)
      	
      	sortedFields = sorted(self._fields.iteritems(),
      	                      key=operator.itemgetter(1))
      	
      	for (fieldName, (index, specification)) in sortedFields:
      	  outfile.write("  %s %s\n" % (fieldName, specification))
      	  
      	if (self._primaryKey is not None):
      	  outfile.write("  PRIMARY KEY  %s\n" % self._primaryKey)

        for key in self._keys:
          outfile.write("  KEY %s %s\n" % key)
          
      	outfile.write(" ".join(self._engine))
      	outfile.write("\n")
        
      	for line in self._epilogue:
      	  outfile.write(" ".join(line))
          outfile.write("\n")
        
      	if (self._index is not None):
      	  outfile.write("CREATE INDEX %s ON %s ( %s );\n" % self._index)
      	
      	outfile.close()


