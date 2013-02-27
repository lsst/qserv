from lsst.qserv.admin import commons
import logging
import os

class DataReader():

    def __init__(self, data_dir_name, logging_level=logging.DEBUG ):
        self.log = logging.getLogger()
        self.dataDirName = data_dir_name

    def analyze(self):

        self.dataConfig = dict()

        self.dataConfig['partitionned-tables'] = ["Object", "Source"]
        self.dataConfig['schema-extension']='.schema'
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


    def getSchemaFiles(self):
        files = os.listdir(self.dataDirName)
        result = []
        for file in files:
            if file.endswith(self.dataConfig['schema-extension']):
                result.append(file)
        return result
