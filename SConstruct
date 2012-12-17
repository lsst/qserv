# -*- python -*-

import os
import sys
import io
import re
import errno
import logging
import logger
from SCons.Node import FS
from SCons.Script import Mkdir,Chmod,Copy,WhereIs
import ConfigParser

import actions 
import commons 
import utils


logger = commons.init_default_logger('scons-qserv')


# this file must be placed in main scons directory
src_dir=Dir('.').srcnode().abspath+"/"
config_file_name=src_dir+"qserv-build.conf"
default_config_file_name=src_dir+"qserv-build.default.conf"

env = Environment(tools=['textfile'])

if not os.path.exists(config_file_name):
    logging.fatal("Your configuration file is missing: %s" % config_file_name)
    sys.exit(1)

try: 
    config = commons.read_config(config_file_name, default_config_file_name)
except ConfigParser.NoOptionError, exc:
    logging.fatal("An option is missing in your configuration file: %s" % exc)
    sys.exit(1)

config['src_dir'] = src_dir

def init_action(target, source, env):

    logger = logging.getLogger('scons-qserv')

    check_success=True

    for param in ('base_dir','log_dir','mysqld_data_dir'):
        dir = config[param]
        if not utils.exists_and_is_writable(dir):
       	    logging.fatal("%s is not writable check/update permissions or update config['%s']" % (dir, param))
            check_success=False

    for suffix in ('/build', '/var', '/var/lib'):
        dir = config['base_dir']+suffix
        if not utils.exists_and_is_writable(dir):
       	    logging.fatal("%s is not writable check/update permissions" % dir)
            check_success=False

    if not commons.is_readable(config['lsst_data_dir']):
    	logging.fatal("LSST data directory (lsst_data_dir) is not readable : %s" % config['lsst_data_dir'])
        check_success=False    

    if check_success :
        logger.info("Qserv directory structure creation succeeded")
    else:
        sys.exit(1)

env.Requires(env.Alias('download'), env.Alias('init')) 
env.Requires(env.Alias('install'), env.Alias('download'))

env.Default(env.Alias('install'))
        
######################        
#
# Defining Init Alias
#
######################        
init_cmd = env.Command('init-dummy-target', [], init_action)
env.Alias('init', init_cmd)

###########################        
#
# Defining Download Alias
#
###########################        
source_urls = []
target_files = []

download_cmd_lst = []
output_dir = config['base_dir'] + os.sep +"build" + os.sep
# Add a command for each file to download
for app in config['dependencies']:
    if re.match(".*_url",app) and not re.match("base_url",app):
        app_url=config['dependencies'][app]
        base_file_name = os.path.basename(app_url)
        output_file = output_dir + base_file_name
        # Command to use in order to download source tarball 
        env.Command(output_file, Value(app_url), actions.download)
	download_cmd_lst.append(output_file)
env.Alias('download', download_cmd_lst)

#########################        
#
# Defining Install Alias
#
######################### 

for target in ('install', 'init-mysql-db', 'qserv-only', 'clean-all'): 
    env.Alias(target, env.Command(target+'-dummy-target', [], actions.build_cmd_with_opts(config,target)))


script_dict = {'@foo@': 'foo', '@bar@': 'bar', '<TEST_DIR>': 'supertoto'}

env.Substfile("common.c", [Value('#include "@foo@.h"'), Value('#include "@bar@.h"'),
                "common.h"
                ], SUBST_DICT=script_dict)

env.Alias('subst', "common.c")
