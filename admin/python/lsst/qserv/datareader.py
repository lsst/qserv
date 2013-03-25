from lsst.qserv.admin import commons
import logging
import os
import tempfile

class DataReader():

    def __init__(self, data_dir_name, data_name=None):
        self.log = logging.getLogger()
        self.dataDirName = data_dir_name
        self.dataName = data_name
        self.dataConfig = dict()

        self.tables = []

    def analyze(self):

        #self.dataConfig['Object']['ra-column'] = schemaDict['Object'].indexOf("`ra_PS`")
        #self.dataConfig['Object']['decl-column'] = schemaDict['Object'].indexOf("`decl_PS`")
        #self.dataConfig['Object']['chunk-column-id'] = schemaDict['Object'].indexOf("`chunkId`")

        # TODO : use meta service instead of hard-coded parameters
        self.log.debug("DataReader.analyze() : Data name is : %s" %self.dataName )

        self.dataConfig['sql-views'] = []

        if self.dataName=="case01":
            
            self.dataConfig['partitionned-tables'] = ["Object", "Source"]
       
            """ Fill column position (zero-based index) """
            self.dataConfig['Object']=dict()
            self.dataConfig['Source']=dict()
            
            self.dataConfig['schema-extension']='.schema'
            self.dataConfig['data-extension']='.tsv'
            self.dataConfig['zip-extension']='.gz'
            self.dataConfig['delimiter']='\t'
            
            self.dataConfig['Object']['ra-column'] = 2
            self.dataConfig['Object']['decl-column'] = 4
            self.dataConfig['Object']['chunk-column-id'] = 227
            
            self.dataConfig['Source']['ra-column'] = 33
            self.dataConfig['Source']['decl-column'] = 34
            
             # chunkId and subChunkId will be added
            self.dataConfig['Source']['chunk-column-id'] = None
            
            self.log.debug("Data configuration : %s" % self.dataConfig)
            
        # for PT1.1
        elif self.dataName=="case02":
            
            self.dataConfig['partitionned-tables'] = ["Object", "Source"]
       
            """ Fill column position (zero-based index) """
            self.dataConfig['Object']=dict()
            self.dataConfig['Source']=dict()
            
            self.dataConfig['schema-extension']='.sql'
            self.dataConfig['data-extension']='.txt'
            self.dataConfig['zip-extension']=None
            self.dataConfig['delimiter']=','

            self.dataConfig['Object']['ra-column'] = 2
            self.dataConfig['Object']['decl-column'] = 4
            self.dataConfig['Object']['chunk-column-id'] = 225

            self.dataConfig['Source']['ra-column'] = 32
            self.dataConfig['Source']['decl-column'] = 33
             # chunkId and subChunkId will be added
            self.dataConfig['Source']['chunk-column-id'] = None

        # for W13
        elif self.dataName=="case03":

            self.dataConfig['partitionned-tables'] = ["AvgForcedPhot",
                                                "AvgForcedPhotYearly",
                                                "RefObject", 
                                                "RunDeepSource",
                                                "RunDeepForcedSource"]
 
            for table in self.dataConfig['partitionned-tables']:
                self.dataConfig[table]=dict()
                # chunkId and subChunkId will be added
                self.dataConfig[table]['chunk-column-id'] = None

            self.dataConfig['schema-extension']='.sql'
            self.dataConfig['data-extension']='.txt'
            self.dataConfig['zip-extension']=None
            self.dataConfig['delimiter']=','

            self.dataConfig['AvgForcedPhot']['ra-column'] = 1
            self.dataConfig['AvgForcedPhot']['decl-column'] = 2

            self.dataConfig['AvgForcedPhotYearly']['ra-column'] = 2
            self.dataConfig['AvgForcedPhotYearly']['decl-column'] = 3
            
            self.dataConfig['RefObject']['ra-column'] = 12
            self.dataConfig['RefObject']['decl-column'] = 13

            self.dataConfig['RunDeepSource']['ra-column'] = 1
            self.dataConfig['RunDeepSource']['decl-column'] = 2

            self.dataConfig['RunDeepForcedSource']['ra-column'] = 1
            self.dataConfig['RunDeepForcedSource']['decl-column'] = 2

            self.tables=['Science_Ccd_Exposure_Metadata_coadd_r', 'AvgForcedPhotYearly', 'Science_Ccd_Exposure_Metadata', 'RunDeepSource',  'RunDeepForcedSource', 'DeepForcedSource', 'ZZZ_Db_Description', 'RefObject', 'RefDeepSrcMatch', 'Science_Ccd_Exposure_coadd_r', 'Science_Ccd_Exposure', 'AvgForcedPhot', 'DeepCoadd_To_Htm10', 'Science_Ccd_Exposure_To_Htm10_coadd_r', 'LeapSeconds', 'DeepCoadd', 'DeepCoadd_Metadata', 'DeepSource', 'Filter']

            self.dataConfig['sql-views'] = ['DeepForcedSource','DeepSource']

        
    def readTableList(self):
        files = os.listdir(self.dataDirName)
        if self.tables==[]:
            for f in files:
                filename, fileext = os.path.splitext(f)
                if fileext == self.dataConfig['schema-extension']:
                    self.tables.append(filename)
        self.log.debug("%s.readTableList() found : %s" %  (self.__class__.__name__, self.tables))

    def getSchemaAndDataFilenames(self, table_name):
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

