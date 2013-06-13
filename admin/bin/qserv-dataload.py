#!/usr/bin/env python

# - Faire un merge avec ticket 2834
# - Voir comment loader attribue ses numero de chunks
# - Charger les donnees avec le qservdataloader des tests

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


def LoadSchemas(logger, config, schemas):
    sqlCommand = cmd.Cmd(config, mode=const.QSERV_LOAD, database="LSST")
    for schemaFile in schemas:
        logger.info("Loading schema %s." % schemaFile)
        sqlCommand.executeFromFile(schemaFile)
    

def read_chunks(logger, chunkfile):
    with open(chunkfile) as f:
        chunk_ids = f.read().splitlines()        
    logger.info("Chunks = " + ', '.join(chunk_ids))
    chunk_id_list = map(int, chunk_ids)
    return chunk_id_list


def configure_master(logger, config, chunkfile, schemas):
    filename =  os.path.join(config['qserv']['base_dir'],"etc","emptyChunks.txt")
    nbstripes = config['qserv']['stripes']

    chunk_id_list = read_chunks(logger, chunkfile)   

    logger.info("Creating empty chunk file : %s with %s stripes." % (filename, nbstripes))
    CreateEmptyChunksFile(int(nbstripes), chunk_id_list, filename)

    logger.info("Loading schemas into database LSST.")
    LoadSchemas(logger, config, schemas)

    
def configure_worker(logger, config, chunkfile, schemas):
    logger.info("Loading schemas into database LSST.")
    LoadSchemas(logger, config, schemas)

    # Here we need to do duplication/replication/partitioning on the fly
    
    # Partitioning
    # $ mkdir /tmp/partitions
    # $ /opt/qserv-dev/qserv-release-0.5.1rc2/bin/python /opt/qserv-dev/qserv-release-0.5.1rc2/qserv/master/examples/partition.py --output-dir /tmp/partitions/ --chunk-prefix Object --theta-column 2 --phi-column 4 --num-stripes 10 --num-sub-stripes 10 --delimiter '\t' --chunk-column 227 /home/qserv/src/release/data/case01/data/Object.tsv
    # $ find /tmp/partitions/ -type f
    # $ /opt/qserv-dev/qserv-release-0.5.1rc2/bin/python /home/qserv/src/release/qserv/master/examples/loader.py --user=root --password=2T7KR0 --database=LSST clrlsstwn02-vm.in2p3.fr:3306 /tmp/partitions LSST.Object

    # TODO: Comment retrouver les chunks id a partir des worker id dans le load.py ?
    
    LSST_dir = os.path.join(config['qserv']['base_dir'],"xrootd-run","q","LSST")
    logger.info("Creation of LSST Xrootd query directory (%s)." % LSST_dir)
    if not os.path.exists(LSST_dir):
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
