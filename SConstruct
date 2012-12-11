# -*- python -*-

import os, sys, io, re
import errno
import logging
from SCons.Node import FS
from SCons.Script import Mkdir,Chmod,Copy,WhereIs

import utils
import ConfigParser

logger = logging.getLogger('scons-qserv')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# this level can be reduce for each handler
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('scons.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler) 

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler) 

# this file must be placed in main scons directory
src_dir=Dir('.').srcnode().abspath+"/"
config_file_name=src_dir+"qserv-build.conf"

default_config = """
# WARNING : these variables mustn't be changed once the install process is started
[DEFAULT]
version = qserv-dev
basedir = /opt/%(version)s
logdir = %(basedir)s/var/log

[qserv]
# Qserv rpc service port is 7080 but is hard-coded

# Tree possibles values :
# mono
# master
# worker
node_type=mono

# Qserv master DNS name
master=qserv-master.in2p3.fr

# Geometry file will be downloaded by default in git master branch
# but a source directory may be specified 
# it could be retrieved for exemple with : git clone git://dev.lsstcorp.org/LSST/DMS/geom
# geom=/home/user/geom

[xrootd]
cmsd_manager_port=4040
xrootd_port=1094

[mysql-proxy]

port=4040

[mysqld]

port=13306

pass='changeme'
#datadir=/data/$(version)/mysql
datadir=%(basedir)s/var/lib/mysql

[lsst]
      
# Where to download LSST data
# Example: PT1.1 data should be in $(datadir)/pt11/
datadir=/data/lsst 
"""

env = Environment()

if not os.path.exists(config_file_name):
    logging.fatal("Your configuration file is missing: %s" % config_file_name)
    sys.exit(1)

try: 
    config = utils.read_config(config_file_name, default_config)
except ConfigParser.NoOptionError, exc:
    logging.fatal("An option is missing in your configuration file: %s" % exc)
    sys.exit(1)

config['src_dir'] = src_dir

def init_action(target, source, env):

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

    if not utils.is_readable(config['lsst_data_dir']):
    	logging.fatal("LSST data directory (lsst_data_dir) is not writable : %s" % config['lsst_data_dir'])
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
        env.Command(output_file, Value(app_url), utils.download_action)
	download_cmd_lst.append(output_file)
env.Alias('download', download_cmd_lst)

#########################        
#
# Defining Install Alias
#
######################### 

for target in ('install', 'init-mysql-db', 'qserv-only', 'clean-all'): 
    env.Alias(target, env.Command(target+'-dummy-target', [], utils.build_cmd_with_opts_action(config,target)))

