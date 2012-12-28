#!/usr/bin/env python

import inspect 
import optparse
import os
import sys 

import commons

import csv2object

#current_file_name=os.path.abspath(inspect.getfile(inspect.currentframe()))
#current_dir = os.path.dirname(current_file_name)
#src_dir=os.path.abspath(os.path.join(current_dir,os.path.pardir))

class QservDataManager:

    #def __init__(self, config_dir,logger_name='qserv-data-manager', temp='/tmp/output'):

    def configure(self, config_dir,logger_name='qserv-data-manager'):

        config_file_name=os.path.join(config_dir,"qserv-build.conf")
        default_config_file_name=os.path.join(config_dir,"qserv-build.default.conf")
        self.config = commons.read_config(config_file_name, default_config_file_name)
    
        self.logger_name = logger_name

        self.logger = commons.init_default_logger(
            logger_name,
            log_path=self.config['qserv']['log_dir']
        )
        
        qserv_admin_cmd=os.path.join(self.config['qserv']['bin_dir'],'qserv-admin')

        self.partition_data_cmd = [
            'PYTHONPATH=/usr/lib64/python2.6/site-packages/',   
            qserv_admin_cmd,
            '--partition',
            '--source', os.path.join(self.config['lsst']['data_dir'],'pt11'),
            '--table', 'Object',
            '--output', os.path.join(self.config['lsst']['data_dir'],'pt11_partition')
        ] 

        self.delete_data_cmd = [
            qserv_admin_cmd, 
            '--delete-data', 
            '--dbpass', self.config['mysqld']['pass']
        ]
        
        self.delete_data_cmd_2 = [
            'mysql', 
            '-S', '/opt/qserv-dev/var/lib/mysql/mysql.sock'
            '-u', 'root' 
            '-p', self.config['mysqld']['pass'],
            '-e', '\'Drop database if exists LSST;\'' 
        ]

        self.meta_outfilename = os.path.join(self.config['qserv']['base_dir'],'tmp',"meta-pt11.csv")

        # Delete and load data from file
        self.load_data = [
            'mysql', 
            '-S', '/opt/qserv-dev/var/lib/mysql/mysql.sock'
            '-u', 'root' 
            '-p', self.config['mysqld']['pass'],
            '-e', '\' use qservMeta;\n delete from LSST__Object;\n LOAD DATA INFILE ' + self.meta_outfilename + 'IGNORE INTO TABLE LSST__Object FIELDS TERMINATED BY ','; \'' 
        ]

        self.load_data_cmd = [
            qserv_admin_cmd,
            '--load', 
            '--dbpass', self.config['mysqld']['pass'],
            '--source', os.path.join(self.config['lsst']['data_dir'],'pt11'),
            '--table', 'Object',
            '--output', os.path.join(self.config['lsst']['data_dir'],'pt11_partition')
        ]

    def partitionPt11Data(self):
        out = commons.run_command(
            ['mkdir', '-p', os.path.join(self.config['lsst']['data_dir'],'pt11_partition')], 
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
        
    def fillTableMeta(self, nbworkers, outfilename):
        # TODO: use parameters or comand line options ?
        data_dirs = [os.path.join(self.config['lsst']['data_dir'],'pt11_partition')]
        self.logger.info("Filling meta database from PT1.1 LSST data : %s \n" % data_dirs[0])
        csv2object.CSV2Object(nbworkers, data_dirs, outfilename)
    
    def parseOptions(self):    
        script_name=sys.argv[0]
        op = optparse.OptionParser()
        mode_option_values = ['partition','delete-db','load-db','delete-then-load-db','fill-table-meta']
        op.add_option("-m", "--mode", dest="mode",
                default="delete-then-load-db",
                help= "LSST data management mode:" +
                ", ".join(mode_option_values) +
                " [default: %default]")
        op.add_option("-c", "--config-dir", dest="config_dir",
                help= "Path to directory containing qserv-build.conf and"
                "qserv-build.default.conf")
        op.add_option("-n", "--number-of-threads", dest="number_of_threads",
                default="1",
                help= "Number of threads used when loading data into qservMeta" + 
                " [default: %default]")
        # TODO : add to config file with nbthreads ?
        op.add_option("-t", "--meta-temporary-file", dest="meta-temporary_file",
                help= "Name of the temporary file used to write and then load qservMeta")
        (options, args) = op.parse_args()
        
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

        return options

    def run(self, options):
        mode = options.mode
        if mode == 'partition':
            self.partitionPt11Data()
        elif mode == 'delete-db':
            self.deleteAllData()
        elif mode == 'load-db':
            self.loadPt11Data()
        elif mode == 'delete-then-load-db':
            self.deleteAllData()
            self.loadPt11Data()
        elif mode == 'fill-table-meta':
            nbworkers = int(options.number_of_threads)        
            self.fillTableMeta(nbworkers,self.meta_outfilename)

def main():
    qserv_data_manager = QservDataManager()
    options = qserv_data_manager.parseOptions()   
    qserv_data_manager.configure(options.config_dir)
    qserv_data_manager.run(options) 
 
    
if __name__ == '__main__':
    main()
