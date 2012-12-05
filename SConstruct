import os, sys, io, re
import errno
import logging
from SCons.Node import FS
from SCons.Script import Mkdir,Chmod,Copy,WhereIs

import utils

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
config_file_name=Dir('.').srcnode().abspath+"/"+"qserv-build.conf"

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

config = utils.read_config(config_file_name, default_config)


def init_target(target, source, env):

    check_success=True

    ret =  utils.is_writeable_dir(config['base_dir']) 
    if (not ret[0]):
    	logging.fatal("Checking Qserv base directory : %s" % ret[1])
        check_success=False
    else:
        Execute(Mkdir(config['base_dir']+"/build"))   
 
    ret =  utils.is_writeable_dir(config['log_dir']) 
    if (not ret[0]):
    	logging.fatal("Checking Qserv log directory : %s" % ret[1])
        check_success=False    

    ret =  utils.is_writeable_dir(config['mysqld_data_dir']) 
    if (not ret[0]):
    	logging.fatal("Checking MySQL data directory : %s" % ret[1])
        check_success=False    

    ret =  utils.is_readable_dir(config['lsst_data_dir']) 
    if (not ret[0]):
    	logging.fatal("Checking LSST data directory : %s" % ret[1])
        check_success=False    
    
    if not check_success :
        sys.exit(1)
    else:
        logger.info("Qserv initial directory structure analysis succeeded")    

def download_target(target, source, env):
    for url in source:
	logger.debug("URL : %s" % url)
	
        utils.download(str(url),config['base_dir']+"/build")

init_cmd = env.Command(['init'], [], init_target)
env.Alias('Init', init_cmd)

source_urls = []
target_files = []
for app in config['dependencies']:
    if re.match(".*_url",app) and not re.match("base_url",app):
        app_url=config['dependencies'][app]
        file_name = config['base_dir']+"/build/"+app_url.split(os.sep)[-1]
        source_urls.append(Value(app_url))
        target_files.append(file_name)

logger.debug("Download target %s :" %target_files)
#env.Command(Split(target_files), Split(source_urls), download_target)
download_cmd = env.Command(target_files, source_urls, download_target)
env.Depends( download_cmd, env.Alias('Init'))

env.Default(download_cmd)
