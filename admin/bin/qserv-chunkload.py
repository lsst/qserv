#!/usr/bin/env python

# Loads chunk into Qserv

# Example usage: 
# admin/bin/qserv-chunkload.py --database LSST --number-of-nodes 300 -c $PWD -m worker --chunks-file /tmp/chunks --table-description-file /tmp/descfile 
# qserv-chunkload.py -m [master | worker]
#   -c <Directory of qserv configuration file>
#   --database <database>
#   --number-of-nodes <#workers>
#   --chunks-file <file containing chunks to be loaded in worker or all chunks id for master>
#   --table-description-file <table description file>
#   <Schema to be loaded on master> ...
#   <Non partitionned data to be loaded on master> ...

# Description file format : (TODO: to be filled in metadata server instead)
# tablename  Object
# schema     /home/qserv/qserv/tests/testdata/case01/data/Object.schema
# data       /home/qserv/qserv/tests/testdata/case01/data/Object.tsv 
# delimiter  \t 
# rafieldname    ra_PS
# declfieldname  decl_PS

# WARNING : this procedure is redundant with :
# - admin/python/lsst/qserv/qservdataloader.py (which load data for test cases)
# - admin/custom/bin/qserv-admin.pl also have a data loading procedure for PT1.1 data set
# it should be unified in a global data loading procedure : https://dev.lsstcorp.org/trac/wiki/db/Qserv/DataLoading

from lsst.qserv.admin import commons
from lsst.qserv.sql import cmd, const
from lsst.qserv import qservdataloader
from optparse import OptionParser

import os
import shutil

def splitlist_callback(option, opt, value, parser):
    setattr(parser.values, option.dest, value.split(','))

def parseOptions():
    print "Parsing options."
    commandline_parser = OptionParser()
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
    commandline_parser.add_option("--number-of-nodes",
                                  dest="nbNodes", default=None,
                                  help= "Number of nodes.")
    commandline_parser.add_option("--delimiter",
                                  dest="delimiter", default="\t",
                                  help= "Delimiter for data files. [default: %default]")
    commandline_parser.add_option("--table-description-file",
                                  type='string',
                                  dest="tables_list", default=None,
                                  action='callback', callback=splitlist_callback,
                                  help= "Comma separated list of table description files.")
    commandline_parser.add_option("--generate-chunks-only",
                                  dest='generate_chunks_only', 
                                  default=False,
                                  action='store_true',
                                  help="Generates only chunks file without loading.")
    (options, args) = commandline_parser.parse_args()

    if options.chunks_file is None:
        commandline_parser.error("Chunks file is missing")

    if options.nbNodes is None:
        commandline_parser.error("Number of nodes is missing")

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


def read_chunks(logger, chunksfile):
    with open(chunksfile) as f:
        chunk_ids = f.read().splitlines()
    logger.info("Chunks = " + ', '.join(chunk_ids))
    chunk_id_list = map(int, chunk_ids)
    return (chunk_id_list, chunk_ids)


def read_description(filename):
    description = dict()
    with open(filename) as f:
        for line in f:
            (key, val) = line.split()
            description[key] = val
    return description

def partition(logger, options, config, chunk_str_list):
    dbname = options.database
    base_dir = config['qserv']['base_dir']
    tmp_dir = config['qserv']['tmp_dir']
    
    partition_dirname = os.path.join(tmp_dir, "partition")
    if os.path.isdir(partition_dirname):
        logger.info("Removing existing directory %s" % partition_dirname)
        shutil.rmtree(partition_dirname)

    logger.info("Creation of partition directory : %s." % partition_dirname)
    os.makedirs(partition_dirname)

    chunkfile = options.chunks_file
    chunker_scriptname = os.path.join(base_dir,"qserv", "master", "examples", "makeChunk.py")

    python = config['bin']['python']
    username = config['qserv']['user']
    nbstripes = config['qserv']['stripes']
    nbsubstripes = config['qserv']['substripes']
    master = config['qserv']['master']
    port = config['mysqld']['port']

    for filename in options.tables_list:
        description = read_description(filename) 

        tablename = description["tablename"]
        schema_filename = description["schema"]
        data_filename = description["data"]
        delimiter = description["delimiter"]
        delimiter = delimiter.replace("\\t","\t")
        rafieldname = description["rafieldname"]
        declfieldname = description["declfieldname"]

        # Loading schemas 
        LoadSql(logger, config, dbname, [schema_filename])
        
        chunker_cmd = [ python,
                        chunker_scriptname,
                        '--output-dir', partition_dirname,
                        '-S', str(nbstripes),
                        '-s', str(nbsubstripes),
                        '--dupe',
                        '--chunk-prefix=' + tablename,
                        # "--chunk-list=" + ",".join(chunk_str_list),
                        "--chunks-file", chunkfile,
                        "--node-count=" + options.nbNodes,
                        "--delimiter=" + delimiter,
                        "--theta-name=" + rafieldname,
                        "--phi-name=" + declfieldname,
                        "--schema=" + schema_filename,
                        data_filename
                        ]

        logger.info("Partitioning data into chunks.")
        out = commons.run_command(chunker_cmd)

        if options.generate_chunks_only:
            return None
        
        loader_scriptname = os.path.join(base_dir,"qserv", "master", "examples", "loader.py")
        socketname = config['mysqld']['sock']

        loader_cmd = [ python,
                       loader_scriptname,
                       '--user=' + username,
                       '--socket=' + socketname,
                       '--database=' + options.database,
                       master + ":" + str(port),
                       partition_dirname,
                       options.database + "." + tablename
                       ]

        logger.info("Loading chunks data into MySQL database %s." % options.database)
        out =  commons.run_command(loader_cmd)



def configure_master(logger, options, config, sql_list):
    chunkfile = options.chunks_file
    dbname = options.database

    filename =  os.path.join(config['qserv']['base_dir'],"etc","emptyChunks.txt")
    nbstripes = config['qserv']['stripes']

    (chunk_id_list, chunk_str_list) = read_chunks(logger, chunkfile)

    logger.info("Creating empty chunk file : %s with %s stripes." % (filename, nbstripes))
    CreateEmptyChunksFile(int(nbstripes), chunk_id_list, filename)

    logger.info("Loading SQL into database %s." % dbname)
    LoadSql(logger, config, dbname, sql_list)


def configure_worker(logger, options, config, sql_list):
    chunkfile = options.chunks_file
    dbname = options.database

    (chunk_id_list, chunk_str_list) = read_chunks(logger, chunkfile)

    LoadSql(logger, config, dbname, sql_list)
    if options.tables_list is not None:
        partition(logger, options, config, chunk_str_list)
    else:
        logger.info("WARNING: empty tables list ! (--table-description-file)")
        exit(1)
        
    LSST_dir = os.path.join(config['qserv']['base_dir'],"xrootd-run","q",dbname)
    if os.path.isdir(LSST_dir):
        logger.info("Removing existing directory %s" % LSST_dir)
        shutil.rmtree(LSST_dir)

    logger.info("Creation of %s Xrootd query directory (%s)." % (dbname, LSST_dir))
    os.makedirs(LSST_dir)

    logger.info("Chunk directory creation for Xrootd.")
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

    configure = {'master': configure_master,
                 'worker': configure_worker}
    configure[options.mode](logger, options, config, sql_list)


if __name__ == '__main__':
    main()
