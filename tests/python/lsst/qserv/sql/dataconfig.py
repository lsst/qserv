from lsst.qserv.admin import commons
import logging
import os
import tempfile

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
        self.dataConfig['delimiter']='\t'

        """ Fill column position (zero-based index) """
        self.dataConfig['Object']=dict()
        self.dataConfig['Source']=dict()
        #self.dataConfig['Object']['ra-column'] = schemaDict['Object'].indexOf("`ra_PS`")
        #self.dataConfig['Object']['decl-column'] = schemaDict['Object'].indexOf("`decl_PS`")
        #self.dataConfig['Object']['chunk-column-id'] = schemaDict['Object'].indexOf("`chunkId`")
        self.dataConfig['Object']['ra-column'] = 2
        self.dataConfig['Object']['decl-column'] = 4

        # for case01
        self.dataConfig['Object']['chunk-column-id'] = 227


        # Source will be placed on the same chunk that its related Object
        #self.dataConfig['Source']['ra-column'] = schemaDict['Source'].indexOf("`raObject`")
        #self.dataConfig['Source']['decl-column'] = schemaDict['Source'].indexOf("`declObject`")

        # for case01
        self.dataConfig['Source']['ra-column'] = 33
        self.dataConfig['Source']['decl-column'] = 34
        
        # chunkId and subChunkId will be added
        self.dataConfig['Source']['chunk-column-id'] = None

        self.log.debug("Data configuration : %s" % self.dataConfig)

        # for PT1.1
        self.dataConfig['schema-extension']='.sql'
        self.dataConfig['data-extension']='.txt'
        self.dataConfig['zip-extension']=None
        self.dataConfig['delimiter']=','

        # for PT1.1
        self.dataConfig['Object']['chunk-column-id'] = 225
        self.dataConfig['Source']['ra-column'] = 32
        self.dataConfig['Source']['decl-column'] = 33

    def readTableList(self):
        files = os.listdir(self.dataDirName)
        if self.tables==[]:
            for f in files:
                filename, fileext = os.path.splitext(f)
                if fileext == self.dataConfig['schema-extension']:
                    self.tables.append(filename)
        self.log.debug("%s.readTableList() found : %s" %  (self.__class__.__name__, self.tables))

    def getSchemaAndDataFiles(self, table_name):
        zipped_data_filename = None
        data_filename = None
        if table_name in self.tables:
            prefix = os.path.join(self.dataDirName, table_name)
            schema_filename = prefix + self.dataConfig['schema-extension']
            data_filename = prefix + self.dataConfig['data-extension']
            if self.dataConfig['zip-extension'] is not None:
                zipped_data_filename = data_filename + self.dataConfig['zip-extension']
            else:
                data_filename = prefix + self.dataConfig['data-extension']
            return (schema_filename, data_filename, zipped_data_filename)
        else:
            raise Exception, "%s.getDataFiles(): '%s' table isn't described in input data" %  (self.__class__.__name__, table_name)

    def getTextDataFile(self, out_dirname, table_name):
        (schema_filename, data_filename, zipped_data_filename) = self.getSchemaAndDataFiles(table_name)

        tmp_suffix = (".%s.%s" % (table_name,self.dataConfig['data-extension']))
        tmp = tempfile.NamedTemporaryFile(suffix=tmp_suffix, dir=out_dirname,delete=False)
        tmp_data_file = tmp.name

        if data_filename is not None:
            # TODO make a link
            return data_filename
        elif zipped_data_filename is not None:
            if os.path.exists(tmp_data_file):
                os.unlink(tmp_data_file)
            self.log.info(" ./Uncompressing: %s into %s" %  (zipped_data_filename, tmp_data_file))
            # TODO gunzip(zipped_data_filename, tmp_data_file)
  
        return  tmp_data_file
