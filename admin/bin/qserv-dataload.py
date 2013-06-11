#!/usr/bin/env python

# admin/bin/qserv-dataload.py
# -c ~/src/release/qserv/
# -m master
# --chunks-file /tmp/toto
# /home/qserv/src/release/qserv/tests/testdata/case01/data/Object.schema /home/qserv/src/release/qserv/tests/testdata/case01/data/Source.schema 

from lsst.qserv.admin import commons
from lsst.qserv.sql import cmd, const
from optparse import OptionParser

import os


def parseOptions():
    print "Parsing options."
    commandline_parser = OptionParser(usage="usage: %prog -c <config dir> -m <mode> --chunks-file <chunks filename> <schema> ...")
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

    (options, args) = commandline_parser.parse_args()

    if options.chunks_file is None:
        commandline_parser.error("Chunks file is missing")
        
    if len(args) < 1:
        commandline_parser.error("Schema(s) are missing")

    return (options, args)


def CreateEmptyChunksFile(nbstripes, chunk_id_list, empty_chunks_filename):
    f=open(empty_chunks_filename,"w")
    maxstripe = 2 * nbstripes * nbstripes
    empty_chunks_list=[i for i in range(0,maxstripe + 1) if i not in chunk_id_list]
    for i in empty_chunks_list:
        f.write("%s\n" %i)
    f.close()


def LoadSchemas(config, schemas):
    sqlCommand = cmd.Cmd(config, mode=const.QSERV_LOAD, database="LSST")
    for schemaFile in schemas:
        sqlCommand.executeFromFile(schemaFile)
    

def configure_master(logger, config, chunkfile, schemas):
    filename =  os.path.join(config['qserv']['base_dir'],"etc","emptyChunks.txt")
    nbstripes = config['qserv']['stripes']

    with open(chunkfile) as f:
        chunk_ids = f.read().splitlines()        
    logger.info("Chunks = " + ', '.join(chunk_ids))
    chunk_id_list = map(int, chunk_ids)

    logger.info("Creating empty chunk file : %s with %s stripes." % (filename, nbstripes))
    CreateEmptyChunksFile(int(nbstripes), chunk_id_list, filename)

    logger.info("Loading schemas into database LSST.")
    LoadSchemas(config, schemas)

def configure_worker(config):
    print "empty"

def main():
    logger = commons.init_default_logger("dataload", log_path="/tmp")
    (options, args) = parseOptions()
    chunksfile = options.chunks_file
    schemas = args

    if options.config_dir is None:
        config = commons.read_user_config()
    else:
        config_file_name=os.path.join(options.config_dir,"qserv-build.conf")
        default_config_file_name=os.path.join(options.config_dir,"qserv-build.default.conf")
        config = commons.read_config(config_file_name, default_config_file_name)

    configure = {'master': configure_master,
                 'worker': configure_worker}
    configure[options.mode](logger, config, chunksfile, schemas)
    
if __name__ == '__main__':
    main()
