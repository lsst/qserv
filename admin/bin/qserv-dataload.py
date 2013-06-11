#!/usr/bin/env python

from lsst.qserv.admin import commons
from optparse import OptionParser

import os


def parseOptions():
    print "Parsing options."
    commandline_parser = OptionParser(usage="usage: %prog -c <config> -m <mode>")
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
    (options, args) = commandline_parser.parse_args()
    return options


def CreateEmptyChunksFile(nbstripes, chunk_id_list, empty_chunks_filename):
    f=open(empty_chunks_filename,"w")
    maxstripe = 2 * nbstripes * nbstripes
    empty_chunks_list=[i for i in range(0,maxstripe + 1) if i not in chunk_id_list]
    for i in empty_chunks_list:
        f.write("%s\n" %i)
    f.close()


def configure_master(config):
    filename =  os.path.join(self.config['qserv']['base_dir'],"etc","emptyChunks.txt")
    nbstripes = self.config['qserv']['stripes']
    # TODO chunk_id_list = ???
    
    CreateEmptyChunksFile(nbstripes, chunk_id_list, filename)


def main():
    logger = commons.init_default_logger("dataload", log_path="/tmp")
    options = parseOptions()
    print options
    
    if options.config_dir is None:
        config = commons.read_user_config()
    else:
        config_file_name=os.path.join(options.config_dir,"qserv-build.conf")
        default_config_file_name=os.path.join(options.config_dir,"qserv-build.default.conf")
        config = commons.read_config(config_file_name, default_config_file_name)

    logger.info(config)
    
if __name__ == '__main__':
    main()
