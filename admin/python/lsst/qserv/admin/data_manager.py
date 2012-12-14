#!/usr/bin/env python

import inspect 
import optparse
import os
import sys 

import commons

#current_file_name=os.path.abspath(inspect.getfile(inspect.currentframe()))
#current_dir = os.path.dirname(current_file_name)
#src_dir=os.path.abspath(os.path.join(current_dir,os.path.pardir))

class QservDataManager:

    def __init__(self, config_dir,logger_name='qserv-data-manager'):

        config_file_name=config_dir+os.sep+"qserv-build.conf"
        default_config_file_name=config_dir+os.sep+"qserv-build.default.conf"
        config = commons.read_config(config_file_name, default_config_file_name)
    
        self.logger_name = logger_name

        self.logger = commons.init_default_logger(
            logger_name,
            log_path=config['log_dir']
        )
        
        qserv_admin_cmd=config['bin_dir']+os.sep+'qserv-admin'

        self.partition_data_cmd = [
            'PYTHONPATH=/usr/lib64/python2.6/site-packages/',   
            qserv_admin_cmd,
            '--partition',
            '--source', config['lsst_data_dir']+os.sep+'pt11',
            '--table', 'Object',
            '--output', config['lsst_data_dir']+os.sep+'pt11_partition'
        ] 

        self.delete_data_cmd = [
            qserv_admin_cmd, 
            '--delete-data', 
            '--dbpass', config['mysqld_pass']
        ]
        
        self.delete_data_cmd_2 = [
            'mysql', 
            '-S', '/opt/qserv-dev/var/lib/mysql/mysql.sock'
            '-u', 'root' 
            '-p', config['mysqld_pass'],
            '-e', '\'Drop database if exists LSST;\'' 
        ]

        self.load_data_cmd = [
            qserv_admin_cmd,
            '--load', 
            '--dbpass', config['mysqld_pass'],
            '--source', config['lsst_data_dir']+os.sep+'pt11',
            '--table', 'Object',
            '--output', config['lsst_data_dir']+os.sep+'pt11_partition'
        ]

    def partitionPt11Data(self):
        out = commons.run_command(
            ['mkdir', '-p', self.config['lsst_data_dir']+os.sep+'pt11_partition'], 
            self.logger_name) 
        out += commons.run_command(self.partition_data_cmd, self.logger_name)
        self.logger.info("Partitionning PT1.1 LSST data : \n %s" % out)

    def deleteAllData(self):
        out = commons.run_command(self.delete_data_cmd, self.logger_name)
        self.logger.info("Deleting previous LSST data : \n %s" % out)

    def loadPt11Data(self):
        out = commons.run_command(self.load_data_cmd, self.logger_name)
        self.logger.info("Loading LSST PT1.1 Object data : \n %s" % out)

        # if it fails : launch next commands to reset to initial state.
        # mysql> drop database LSST;
        # mysql> drop database qserv_worker_meta_1
        # bash> rm -rf /opt/qserv/xrootd-run//q/LSST/


def main():
    op = optparse.OptionParser()
    mode_option_values = ['partition','delete-db','load-db','delete-then-load-db']
    op.add_option("-m", "--mode", dest="mode",
                  default="delete-then-load-db",
                  help= "LSST data management mode:" +
                        ", ".join(mode_option_values) +
                        " [default: %default]")
    op.add_option("-c", "--config-dir", dest="config_dir",
                  help= "Path to directory containing qserv-build.conf and"
                        "qserv-build.default.conf")
    (options, args) = op.parse_args()
   
    script_name=sys.argv[0]
 
    if options.config_dir is None:
        print "%s: --config-dir flag not set" % script_name 
        print "Try `%s --help` for more information." % script_name
        exit(1)
    else:
        config_file_name=options.config_dir+os.sep+"qserv-build.conf"
        default_config_file_name=options.config_dir+os.sep+"qserv-build.default.conf"
        if not os.path.isfile(config_file_name):
            print ("%s: --config-dir must point to a directory containing " 
                   "qserv-build.conf" % script_name)
            exit(1)
        elif not os.path.isfile(config_file_name):
            print ("%s: --config-dir must point to a directory containing "
                   "qserv-build.default .conf" % script_name)
            exit(1)

    if options.mode not in mode_option_values :
        print "%s: --mode flag set with invalid value" % script_name
        print "Try `%s --help` for more information." % script_name
        exit(1)

    qservDataManager = QservDataManager(options.config_dir)

    if options.mode == 'partition':
        qservDataManager.partitionPt11Data()
    elif options.mode == 'delete-db':
        qservDataManager.deleteAllData()
    elif options.mode == 'load-db':
        qservDataManager.loadPt11Data()
    elif options.mode == 'delete-then-load-db':
        qservDataManager.deleteAllData()
        qservDataManager.loadPt11Data()
    
if __name__ == '__main__':
    main()
