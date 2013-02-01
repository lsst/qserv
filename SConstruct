# -*- python -*-

import os
import sys
import io
import re
import errno
import logging
import logger
import SCons.Node.FS
from SCons.Script import Mkdir,Chmod,Copy,WhereIs
import ConfigParser

import actions 
import commons 
import utils

logger = commons.init_default_logger(log_file_prefix="scons-qserv", level=logging.DEBUG)

env = Environment(tools=['textfile', 'clean', 'pymod'])

#########################
#
# Reading config file
#
#########################

# this file must be placed in main scons directory
src_dir=Dir('.').srcnode().abspath+"/"
config_file_name=src_dir+"qserv-build.conf"
default_config_file_name=src_dir+"qserv-build.default.conf"

if not os.path.exists(config_file_name):
    logging.fatal("Your configuration file is missing: %s" % config_file_name)
    sys.exit(1)

try: 
    config = commons.read_config(config_file_name, default_config_file_name)
except ConfigParser.NoOptionError, exc:
    logging.fatal("An option is missing in your configuration file: %s" % exc)
    sys.exit(1)

config['src_dir'] = src_dir

env['config']=config

#####################################
#
# Defining main directory structure
#
#####################################

init_cmd = env.Command('init-dummy-target', [], actions.check_root_dirs)
env.Alias('init', init_cmd)

#########################
#
# Defining dependencies 
#
#########################

env.Requires(env.Alias('download'), env.Alias('init')) 
env.Requires(env.Alias('install'), env.Alias('download'))
# templates must be applied before installation in order to 
# initialize mysql db
env.Requires(env.Alias('install'), env.Alias('templates'))
env.Requires(env.Alias('init-mysql-db'), env.Alias('templates'))
env.Requires(env.Alias('admin-bin'), env.Alias('python-admin'))
env.Requires(env.Alias('install'), env.Alias('admin-bin'))

env.Default(env.Alias('install'))
        
###########################        
#
# Defining Download Alias
#
###########################        

source_urls = []
target_files = []

download_cmd_lst = []
output_dir = config['qserv']['base_dir'] + os.sep +"build" + os.sep
# Add a command for each file to download
for app in config['dependencies']:
    if re.match(".*_url",app):
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

#########################        
#
# Using templates files 
#
######################### 

def get_template_targets():

    template_dir_path="admin/custom"
    target_lst = []

    script_dict = {
                '%\(QSERV_BASE_DIR\)s': config['qserv']['base_dir'], 
                '%\(QSERV_LOG_DIR\)s': config['qserv']['log_dir'], 
                '%\(QSERV_STRIPES\)s': config['qserv']['stripes'], 
                '%\(QSERV_SUBSTRIPES\)s': config['qserv']['substripes'], 
                '%\(MYSQLD_DATA_DIR\)s': config['mysqld']['data_dir'], 
                '%\(MYSQLD_PORT\)s': config['mysqld']['port'], 
                # used for mysql-proxy in mono-node
                # '%(MYSQLD_HOST)': config['qserv']['master'], 
                '%\(MYSQLD_HOST\)s': '127.0.0.1', 
                '%\(MYSQLD_PASS\)s': config['mysqld']['pass'], 
                '%\(MYSQL_PROXY_PORT\)s': config['mysql_proxy']['port'], 
                '%\(XROOTD_MANAGER_HOST\)s': config['qserv']['master'], 
                '%\(XROOTD_PORT\)s': config['xrootd']['xrootd_port'], 
                '%\(XROOTD_RUN_DIR\)s': os.path.join(config['qserv']['base_dir'],'xrootd-run'), 
                '%\(XROOTD_ADMIN_DIR\)s': os.path.join(config['qserv']['base_dir'],'tmp'), 
                '%\(XROOTD_PID_DIR\)s': os.path.join(config['qserv']['base_dir'],'var/run'), 
                '%\(CMSD_MANAGER_PORT\)s': config['xrootd']['cmsd_manager_port'] 
    }
    if config['qserv']['node_type']=='mono':
        script_dict['%\(COMMENT_MONO_NODE\)s']='#MONO-NODE# '
    else:
        script_dict['%\(COMMENT_MONO_NODE\)s']='' 

    logger.info("Applying configuration information via templates files ")
    for node in utils.recursive_glob(template_dir_path,"*",env):
        # strip template_dir_path out to have the filepath relative to templates dir
        template_node_name=str(node)
        logger.debug("Source : %s" % template_node_name)
        template_dir_path = os.path.normpath(template_dir_path)
        index = (template_node_name.find(template_dir_path) +
             len(template_dir_path+os.sep) 
        )

        node_name_path = template_node_name[index:]
        source = template_node_name 
        target = os.path.join(config['qserv']['base_dir'], node_name_path)

        logger.debug("Target : %s " % target)
        target_lst.append(target)
 
        if isinstance(node, SCons.Node.FS.File) :
            env.Substfile(target, source, SUBST_DICT=script_dict)

            # qserv-admin has no extension, Substfile can't manage it easily
            # TODO : qserv-admin could be modified in order to be removed to
            # template files, so that next test could be removed
            f="qserv-admin.pl"
            logger.debug("%s %s " % (target, os.path.basename(target)))
            if os.path.basename(target)	== f :
                symlink_name = target[:-3] 
                logger.debug("Creating symlink from %s to %s " % (symlink_name,target))
                env.Command(symlink_name, target, actions.symlink)       
                target_lst.append(symlink_name)

            path = os.path.dirname(target)
            if os.path.basename(path) == "bin" : 
                env.AddPostAction(target, Chmod("$TARGET", 0760))
            # all other files are configuration files
            else:
                env.AddPostAction(target, Chmod("$TARGET", 0660))
    
    return target_lst

env.Alias("templates", get_template_targets())

#########################        
#
# Install python modules 
#
#########################

python_path_prefix=config['qserv']['base_dir']
 
python_admin = env.InstallPythonModule(target=python_path_prefix, source='admin/python')

#python_targets=utils.build_python_module(source='admin/python',target='/opt/qserv-dev',env=env)
env.Alias("python-admin", python_admin)


#########################        
#
# Install admin commands 
#
#########################
file_base_name="qserv-datamanager.py"
source = os.path.join("admin","bin",file_base_name)
target = os.path.join(config['qserv']['base_dir'],"bin",file_base_name)
Command(target, source, Copy("$TARGET", "$SOURCE"))

env.Alias("admin-bin", target)

# List all aliases

try:
    from SCons.Node.Alias import default_ans
except ImportError:
    pass
else:
    aliases = default_ans.keys()
    aliases.sort()
    env.Help('\n')
    env.Help('Recognized targets:\n')
    for alias in aliases:
        env.Help('    %s\n' % alias)

