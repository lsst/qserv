#!/usr/bin/env python

from lsst.qserv.admin.datamanager import QservDataManager

def main():
    qserv_data_manager = QservDataManager()
    options = qserv_data_manager.parseOptions()   
    qserv_data_manager.configure(options.config_dir)
    qserv_data_manager.run(options) 
 
    
if __name__ == '__main__':
    main()
