import logging

MYSQL_PROXY = 1
MYSQL_SOCK = 2
MYSQL_NET = 3


def readDataConfig(data_dir_name):

    log = logging.getLogger()

    data_config = dict()
    data_config['Object']=dict()
    #data_config['Object']['ra-column'] = self._schemaDict['Object'].indexOf("`ra_PS`")
    #data_config['Object']['decl-column'] = self._schemaDict['Object'].indexOf("`decl_PS`")
    
# zero-based index
    
    # FIXME : return 229 instead of 227
        #data_config['Object']['chunk-column-id'] = self._schemaDict['Object'].indexOf("`chunkId`") -2
    
    # for test case01
    data_config['Object']['ra-column'] = 2
    data_config['Object']['decl-column'] = 4
    data_config['Object']['chunk-column-id'] = 227
    
    data_config['Source']=dict()
        # Source will be placed on the same chunk that its related Object
    #data_config['Source']['ra-column'] = self._schemaDict['Source'].indexOf("`raObject`")
    #data_config['Source']['decl-column'] = self._schemaDict['Source'].indexOf("`declObject`")
    
    # for test case01
    data_config['Source']['ra-column'] = 33
    data_config['Source']['decl-column'] = 34
    
        # chunkId and subChunkId will be added
    data_config['Source']['chunk-column-id'] = None

    log.debug("Data configuration : %s" % data_config)
    return data_config
