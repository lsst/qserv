from lsst.qserv.admin import commons
import logging
import os

class DataReader():

    def __init__(self, data_dir_name):
        self.log = logging.getLogger()
        self.dataDirName = data_dir_name
        self.dataConfig = dict()

        self.tables = []

    def analyze(self):

        self.dataConfig['partitionned-tables'] = ["Object", "Source"]
        self.dataConfig['schema-extension']='.schema'
        self.dataConfig['data-extension']='.tsv'
        self.dataConfig['zip-extension']='.gz'
        self.dataConfig['Object']=dict()
    #dataConfig['Object']['ra-column'] = self._schemaDict['Object'].indexOf("`ra_PS`")
    #dataConfig['Object']['decl-column'] = self._schemaDict['Object'].indexOf("`decl_PS`")
    
    # zero-based index
    
    # FIXME : return 229 instead of 227
        #dataConfig['Object']['chunk-column-id'] = self._schemaDict['Object'].indexOf("`chunkId`") -2
    
    # for test case01
        self.dataConfig['Object']['ra-column'] = 2
        self.dataConfig['Object']['decl-column'] = 4
        self.dataConfig['Object']['chunk-column-id'] = 227
    
        self.dataConfig['Source']=dict()
        # Source will be placed on the same chunk that its related Object
    #dataConfig['Source']['ra-column'] = self._schemaDict['Source'].indexOf("`raObject`")
    #dataConfig['Source']['decl-column'] = self._schemaDict['Source'].indexOf("`declObject`")
    
    # for test case01
        self.dataConfig['Source']['ra-column'] = 33
        self.dataConfig['Source']['decl-column'] = 34
    
        # chunkId and subChunkId will be added
        self.dataConfig['Source']['chunk-column-id'] = None

        self.log.debug("Data configuration : %s" % self.dataConfig)


    def readTableList(self):
        files = os.listdir(self.dataDirName)
        if self.tables==[]:
            for f in files:
                filename, fileext = os.path.splitext(f)
                if fileext == self.dataConfig['schema-extension']:
                    self.tables.append(filename)

    def getDataFiles(self, table_name):
        if table_name in self.tables:
            prefix = os.path.join(self.dataDirName, table_name)
            schema_filename = prefix + self.dataConfig['schema-extension']
            data_filename = prefix + self.dataConfig['data-extension']
            zipped_data_filename = data_filename + self.dataConfig['zip-extension']
            return (schema_filename, data_filename,zipped_data_filename)
        else:
            return None
