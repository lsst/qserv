import os, sys, io
import logging
import ConfigParser
from SCons.Script import Mkdir,Chmod,Copy,WhereIs

logger = logging.getLogger('scons-qserv')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# this level can be reduce for each handler
logger.setLevel(logging.DEBUG)

#file_handler = logging.FileHandler('scons.log')
#file_handler.setFormatter(formatter)
#file_handler.setLevel(logging.DEBUG)
#logger.addHandler(file_handler) 

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler) 

# this file must be placed in main scons directory
# TODO : see quattor to read a default file and overload it
config_file_name="qserv-build.conf"

sample_config = """
# WARNING : these variables mustn't be changed once the install process is started
[default]
version=qserv-dev
basedir=/opt/$(version)

[qserv]
# Qserv rpc service port is 7080 but is hard-coded

logdir=$(basedir)/var/log

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

port=3306

pass='changeme'
#datadir=/data/$(version)/mysql
datadir=$(basedir)/var/lib/mysql

[lsst]

# Where to download LSST data
# Example: PT1.1 data should be in $(datadir)/pt11/
datadir=/data/lsst
"""

env = Environment()

def read_config():
    buildConfigFile=Dir('.').srcnode().abspath+"/"+config_file_name
    logger.debug("Reading build config file : %s" % buildConfigFile)
    config = ConfigParser.SafeConfigParser()
    config.readfp(io.BytesIO(sample_config))
    config.read(buildConfigFile)
    logger.debug("MySQL port : "+config.get("mysqld", "port"))

    

def init_target(target, source, env):
    env.Execute(Mkdir("testdir"))
    print (os.path.abspath(str(target[0])))
    print (os.path.abspath(str(target[1])))
    # os.mkdir(os.path.abspath(str(source[0])))
    # os.symlink(os.path.abspath(str(source[1])), os.path.abspath(str(target[0])))

def symlink(target, source, env):
    os.symlink(os.path.abspath(str(source[0])), os.path.abspath(str(target[0])))

read_config()

#symlink_builder = Builder(action = "ln -s ${SOURCE.file} ${TARGET.file}", chdir = True)
symlink_builder = Builder(action = symlink, chdir = True)

env.Append(BUILDERS = {"Symlink" : symlink_builder})

mylib_link = env.Symlink("toto", "qserv-env.sh")

env.Alias('symlink', mylib_link)

#if Execute(action=init_action):
#        # A problem occurred while making the temp directory.
#        Exit(1)

init_bld = env.Builder(action=init_target)

env.Append(BUILDERS = {'Init' : init_bld})

init = env.Init(["test-test","test-env.sh"], [])

env.Alias('init', init)

#Execute(Mkdir('tutu'))

#qserv_init_alias = env.Alias('qserv_inYit', env.Qserv_init())
#env.Alias('install', [qserv_init_alias])
