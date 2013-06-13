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
        self.config = commons.read_config(config_file_name)
    
        self.logger_name = logger_name

        self.logger = commons.init_default_logger(
            logger_name,
            log_path=self.config['qserv']['log_dir']
        )
        
        self.qserv_admin_bin=os.path.join(self.config['qserv']['bin_dir'],'qserv-admin')

        self.delete_data_cmd = [
            self.qserv_admin_bin, 
            '--delete-data', 
            '--dbpass', self.config['mysqld']['pass']
        ]
        
        # TODO : replace delete_data_cmd
        self.delete_data_cmd_TODO = [
            'mysql', 
            '-S', self.config['mysqld']['sock'],
            '-u', 'root', 
            '-p', self.config['mysqld']['pass'],
            '-e', '\'Drop database if exists LSST;\'' 
        ]

        self.meta_outfilename = os.path.join(self.config['qserv']['tmp_dir'],"meta-pt11.csv")

        # Create meta table
        self.create_meta_cmd = [
            'mysql', 
            '-S', self.config['qserv']['base_dir']+'/var/lib/mysql/mysql.sock',
            '-u', 'root', 
            '-p'+self.config['mysqld']['pass'],
            '-e', "source %s" % os.path.join(self.config['qserv']['tmp_dir'],'qservmeta.sql')   
        ]

        # Delete and load data from file
        self.load_meta_cmd = [
            'mysql', 
            '-S', self.config['qserv']['base_dir']+'/var/lib/mysql/mysql.sock',
            '-u', 'root', 
            '-p'+self.config['mysqld']['pass'],
            '-e', "use qservMeta; delete from LSST__Object; LOAD DATA INFILE" 
                    + " '" + self.meta_outfilename + "' "
                  "IGNORE INTO TABLE LSST__Object FIELDS TERMINATED BY ',';"  
        ]

        self.load_data_cmd = [
            self.qserv_admin_bin,
            '--load', 
            '--dbpass', self.config['mysqld']['pass'],
            '--source', os.path.join(self.config['lsst']['data_dir'],'pt11'),
            '--table', 'Object',
            '--stripedir', os.path.join(self.config['lsst']['data_dir'],'pt11_partition')
        ]

    def partitionPt11Data(self,tables,append=False):

        partition_dirname = os.path.join(self.config['lsst']['data_dir'],'pt11_partition')
        if not os.path.exists(partition_dirname):
            self.logger.debug("Creating directory : %s" % partition_dirname)
            os.mkdir(partition_dirname)
        elif not append:
            self.logger.debug("Deleting previous partitioned data for %s"
                ", in dir %s" % (tables, partition_dirname))
            for elem in tables :
                for root, dirs, files in os.walk(partition_dirname, topdown=False):
                    for name in files:
                        if name.startswith(elem) :
                            filename = os.path.join(root, name)
                            self.logger.debug("Deleting  : %s" % filename) 
                            os.remove(filename)
       
        pt11_config = dict()
        pt11_config['Object']=dict() 
        pt11_config['Object']['ra-column'] = 2
        pt11_config['Object']['decl-column'] = 4
        pt11_config['Source']=dict() 
        pt11_config['Source']['ra-column'] = 6
        pt11_config['Source']['decl-column'] = 9

        for table in tables :
            partition_data_cmd = [
                #'PYTHONPATH=/usr/lib64/python2.6/site-packages/',  
                os.path.join(self.config['qserv']['bin_dir'],'python'), 
                os.path.join(self.config['qserv']['base_dir'],'qserv', 'master', 'examples', 'partition.py'),
                '--output-dir', os.path.join(self.config['lsst']['data_dir'],'pt11_partition'),
                '--chunk-prefix', table,
                '--theta-column', str(pt11_config[table]['ra-column']),
                '--phi-column', str(pt11_config[table]['decl-column']),
                os.path.join(self.config['lsst']['data_dir'],'pt11',table+'.txt'),
                '--num-stripes', self.config['qserv']['stripes'],
                '--num-sub-stripes', self.config['qserv']['substripes'] 
            ]
            out = commons.run_command(partition_data_cmd)
            self.logger.info("Partitionning PT1.1 LSST %s data : \n %s" 
                % (table,out))

    def deleteAllData(self):
        out = commons.run_command(self.delete_data_cmd)
        self.logger.info("Deleting previous LSST data : \n %s" % out)

    def loadPt11Data(self):
        out = commons.run_command(self.load_data_cmd)
        self.logger.info("Loading LSST PT1.1 Object data : \n %s" % out)

        # if it fails : launch next commands to reset to initial state.
        # mysql> drop database LSST;
        # mysql> drop database qserv_worker_meta_1
        # bash> rm -rf /opt/qserv/xrootd-run//q/LSST/
        
        
    def fillTableMeta(self, nbworkers, outfilename):
        # TODO: use parameters or comand line options ?
        data_dirs = [os.path.join(self.config['lsst']['data_dir'],'pt11_partition')]
        self.logger.info("Erasing if needed and creating meta database \n")
        self.logger.debug(" ".join(self.create_meta_cmd))
        out = commons.run_command(self.create_meta_cmd)
        self.logger.info("Meta database creation report: \n %s" % out)
        self.logger.info("Filling meta database from PT1.1 LSST data : %s \n" % data_dirs[0])
        csv2object.CSV2Object(nbworkers, data_dirs, outfilename)
        out = commons.run_command(self.load_meta_cmd)
        self.logger.info("Loading LSST PT1.1 Meta data : \n %s" % out)
    
    def parseOptions(self):    
        script_name=sys.argv[0]
        op = optparse.OptionParser()
        mode_option_values = ['partition','delete-db','load-db','delete-then-load-db','fill-table-meta']
        op.add_option("-m", "--mode", dest="mode",
                default="delete-then-load-db",
                help= "LSST data management mode  : '" +
                "', '".join(mode_option_values) +
                "' [default: %default]")
        op.add_option("-c", "--config-dir", dest="config_dir",
                help= "Path to directory containing qserv-build.conf and"
                "qserv-build.default.conf")
        op.add_option("-n", "--number-of-threads", dest="number_of_threads",
                default="1",
                help= "Number of threads used when loading data into qservMeta" + 
                " [default: %default]")
        tables_option_values = ['Object','Source']
        op.add_option("-t", "--tables", action="append", dest="tables",
                default=tables_option_values,
                help= "Names of data tables to manage" + 
                " [default: %default]")
        # TODO : add to config file with nbthreads ?
        op.add_option("-f", "--meta-temporary-file", dest="meta-temporary_file",
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

        if not set(options.tables).issubset(set(tables_option_values)) :
            print "%s: --tables flag set with invalid value" % script_name
            print "Try `%s --help` for more information." % script_name
            exit(1)

        return options

    def run(self, options):
        mode = options.mode
        if mode == 'partition':
            self.partitionPt11Data(options.tables)
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
