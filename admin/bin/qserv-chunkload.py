#!/usr/bin/env python

# Usage: Loads chunk into Qserv
# 
# qserv-dataload.py -m [master | worker]
#   -c <Directory of qserv configuration file> 
#   --chunks-file <file containing chunks to be loaded in worker or all chunks id for master>
#   <Schema to be loaded> ...
#   <Data to be loaded> ...

from lsst.qserv.admin import commons
from lsst.qserv.sql import cmd, const
from lsst.qserv import qservdataloader
from optparse import OptionParser

import os
import shutil

def parseOptions():
    print "Parsing options."
    commandline_parser = OptionParser(usage="usage: %prog -c <config dir> -m <mode> --chunks-file <chunks filename> <schema or data file> ...")
    commandline_parser.add_option("-c", "--config-dir",
                                  dest="config_dir", default=None,
                                  help= "Path to directory containing qserv-build.conf and"
                                  "qserv-build.default.conf")
    mode_option_values = ['master','worker']
    commandline_parser.add_option("-m", "--mode", type='choice',
                                  dest="mode", choices=mode_option_values,
                                  default='master',
                                  help= "Qserv mode : '" +
                                  "', '".join(mode_option_values) +
                                  "' [default: %default]")
    commandline_parser.add_option("--chunks-file",
                                  dest="chunks_file", default=None,
                                  help= "Path to a file containing all chunks of all workers.")
    commandline_parser.add_option("--database",
                                  dest="database", default="LSST",
                                  help= "Database to be used for data loading. [default: %default]")

    (options, args) = commandline_parser.parse_args()

    if options.chunks_file is None:
        commandline_parser.error("Chunks file is missing")
        
    if len(args) < 1:
        commandline_parser.error("Schema(s) or data file(s) are missing")

    return (options, args)


def CreateEmptyChunksFile(nbstripes, chunk_id_list, empty_chunks_filename):
    with open(empty_chunks_filename,"w") as f:
        maxstripe = 2 * nbstripes * nbstripes
        empty_chunks_list=[i for i in range(0,maxstripe + 1) if i not in chunk_id_list]
        for i in empty_chunks_list:
            f.write("%s\n" %i)


def LoadSql(logger, config, dbname, sql_list):
    sqlCommand = cmd.Cmd(config, mode=const.QSERV_LOAD, database=dbname)
    for sqlFile in sql_list:
        logger.info("Loading SQL %s." % sqlFile)
        sqlCommand.executeFromFile(sqlFile)
    

def read_chunks(logger, chunkfile):
    with open(chunkfile) as f:
        chunk_ids = f.read().splitlines()        
    logger.info("Chunks = " + ', '.join(chunk_ids))
    chunk_id_list = map(int, chunk_ids)
    return chunk_id_list


def configure_master(logger, config, chunkfile, dbname, sql_list):
    filename =  os.path.join(config['qserv']['base_dir'],"etc","emptyChunks.txt")
    nbstripes = config['qserv']['stripes']

    chunk_id_list = read_chunks(logger, chunkfile)   

    logger.info("Creating empty chunk file : %s with %s stripes." % (filename, nbstripes))
    CreateEmptyChunksFile(int(nbstripes), chunk_id_list, filename)

    logger.info("Loading SQL into database %s." % dbname)
    LoadSql(logger, config, dbname, sql_list)

    
def configure_worker(logger, config, chunkfile, dbname, sql_list):
    logger.info("Loading chunks data into MySQL database %s." % dbname)
    LoadSql(logger, config, dbname, sql_list)    

    LSST_dir = os.path.join(config['qserv']['base_dir'],"xrootd-run","q",dbname)
    if os.path.isdir(LSST_dir):
        logger.info("Removing existing directory %s" % LSST_dir)
        shutil.rmtree(LSST_dir)
    
    logger.info("Creation of %s Xrootd query directory (%s)." % (dbname, LSST_dir))
    os.makedirs(LSST_dir)
    
    logger.info("Chunk directory creation for Xrootd.")
    chunk_id_list = read_chunks(logger, chunkfile)
    for chunk_id in chunk_id_list:
        chunk_name = str(chunk_id)
        chunk_file = os.path.join(LSST_dir, chunk_name)
        with open(chunk_file, "w") as f:
             logger.info("Chunk directory %s creation." % chunk_name) 

    
def main():
    logger = commons.init_default_logger("dataload", log_path="/tmp")
    (options, args) = parseOptions()
    chunksfile = options.chunks_file
    sql_list = args

    if options.config_dir is None:
        config = commons.read_user_config()
    else:
        config_file_name=os.path.join(options.config_dir,"qserv-build.conf")
        default_config_file_name=os.path.join(options.config_dir,"qserv-build.default.conf")
        config = commons.read_config(config_file_name, default_config_file_name)

    dbname = options.database
    
    configure = {'master': configure_master,
                 'worker': configure_worker}
    configure[options.mode](logger, config, chunksfile, dbname, sql_list)

    
if __name__ == '__main__':
    main()
