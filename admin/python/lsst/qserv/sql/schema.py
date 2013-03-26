
# Test:
# import SQLSchema
# s = SQLSchema.SQLSchema("foo")
# s.read("/tmp/a.sql")

import logging
import reader
import operator


class SQLSchema():
    """Schema class for management of SQL schema"""
    
    def __init__(self, tableName, schemaFile):
        self.log = logging.getLogger()
        self.log.debug("Calling SQLSchema(%s)",  tableName)
        # each field must have a positional parameter !
        self._tableName = tableName
        self._schemaFile = schemaFile
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
        if (fieldName in  self._fields):
            del self._fields[fieldName]
        else:
            error_msg = "%s field not in %s for table %s" % (fieldName,self._fields,self._tableName)
            self.log.error(error_msg)
            raise Exception(error_msg)

    def hasField(self, fieldName):
        if (fieldName in  self._fields):
            return True
        else:
            return False
    
    def replaceField(self, oldFieldName, newFieldName):
        if (oldFieldName in self._fields):
            self._fields[newFieldName] = self._fields[oldFieldName]
            self.deleteField(oldFieldName)
        else:
            error_msg = "%s field not in %s for table %s" % (oldFieldName,self._fields,self._tableName)
            self.log.error(error_msg)
            raise Exception(error_msg)

    def _createFieldString(self, dataType, constraints):
        if (constraints is None):
            return dataType
        else:
            return "%s %s" % ( dataType, " ".join(constraints) )
    
    def addField(self, fieldName, dataType, constraints = None):
        fieldSpecification = self._createFieldString(dataType,
                                                     constraints)
        self._fields[fieldName] = ( self._arity, fieldSpecification )
        self._arity = self._arity + 1

    def convertSQLToSchema(self, SQLSchemaStr):
        for line in SQLSchemaStr:
            if ((line[0] == "PRIMARY KEY")):
                self.primaryKey(line[1])
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
                
    def read(self):
        """ Read a MySQL dump schema file """
        parsing = reader.reader(self._schemaFile)
        self._prologue = parsing["prologue"]
        self.convertSQLToSchema(parsing["schema"])
        self._engine = parsing["engine"]
        self._epilogue = parsing["epilogue"]
        
    def write(self, filename):
        with open(filename, 'w') as outfile:
            for line in self._prologue:
                outfile.write(" ".join(line))
                outfile.write("\n")

            self.log.debug("Writing schema table for table : %s",  self._tableName)
            outfile.write("CREATE TABLE `%s` (\n" % self._tableName)

            sortedFields = sorted(self._fields.iteritems(),
                                  key=operator.itemgetter(1))

            fieldsStrList = []
            for (fieldName, (index, specification)) in sortedFields:
              fieldsStrList.append("  %s %s" % (fieldName, specification))

            if (self._primaryKey is not None):
                fieldsStrList.append("  PRIMARY KEY  (%s)" % self._primaryKey)

            for key in self._keys:
                fieldsStrList.append("  KEY %s %s" % key)

            fieldsStr = " ,\n".join(fieldsStrList)
            outfile.write(fieldsStr)
            outfile.write("\n")

            outfile.write(" ".join(self._engine))
            outfile.write("\n")

            for line in self._epilogue:
                outfile.write(" ".join(line))
                outfile.write("\n")

            if (self._index is not None):
                outfile.write("CREATE INDEX %s ON %s ( %s );\n" % self._index)

            outfile.close()


