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

env = Environment(tools=['textfile'])

#########################
#
# Reading config file
#
#########################

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

#####################################
#
# Defining main directory structure
#
#####################################

env['config']=config

init_cmd = env.Command('init-dummy-target', Value(config), actions.check_root_dirs)
env.Alias('init', init_cmd)


#########################
#
# Defining dependencies 
#
#########################

env.Requires(env.Alias('download'), env.Alias('init')) 
env.Requires(env.Alias('install'), env.Alias('download'))
env.Requires(env.Alias('install'), env.Alias('templates'))

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

#########################        
#
# Using templates files 
#
######################### 

def recursive_glob(dir_path,pattern):
        files = Glob(dir_path+os.sep+pattern)
        if files:
            files += recursive_glob(dir_path+os.sep+"*",pattern)
        return files

def symlink(target, source, env):
    os.symlink(os.path.abspath(str(source[0])), os.path.abspath(str(target[0])))

def get_template_targets():

    template_dir_path="admin/custom"
    target_lst = []

    script_dict = {
                '<QSERV_BASE_DIR>': config['qserv']['base_dir'], 
                '<QSERV_LOG_DIR>': config['qserv']['log_dir'], 
                '<MYSQLD_DATA_DIR>': config['mysqld']['data_dir'], 
                '<MYSQLD_PORT>': config['mysqld']['port'], 
                # used for mysql-proxy in mono-node
                # '<MYSQLD_HOST>': config['qserv']['master'], 
                '<MYSQLD_HOST>': '127.0.0.1', 
                '<MYSQLD_PASS>': config['mysqld']['pass'], 
                '<MYSQL_PROXY_PORT>': config['mysql_proxy']['port'], 
                '<XROOTD_MANAGER_HOST>': config['qserv']['master'], 
                '<XROOTD_PORT>': config['xrootd']['xrootd_port'], 
                '<XROOTD_ADMIN_DIR>': os.path.join(config['qserv']['base_dir'],'tmp'), 
                '<XROOTD_PID_DIR>': os.path.join(config['qserv']['base_dir'],'var/run'), 
                '<CMSD_MANAGER_PORT>': config['xrootd']['cmsd_manager_port'] 
    }
    if config['qserv']['node_type']=='mono':
        script_dict['<COMMENT_MONO_NODE>']='#MONO-NODE# '
    else:
        script_dict['<COMMENT_MONO_NODE>']='' 

    logger.info("Applying configuration information via templates files ")
    for node in recursive_glob(template_dir_path,"/*"):
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
        logger.debug("%s %i " % (target, target.rfind(f)))
        if target.rfind(f) == len(target) - len(f) :
            symlink_name = target[:-3] 
            logger.debug("Creating symlink from %s to %s " % (symlink_name,target))
            env.Command(symlink_name, target, [
                                              Chmod("$SOURCE", 0744),
                                              symlink
                                              ]
            )
            target_lst.append(symlink_name)

    return target_lst

env.Alias("templates", get_template_targets())


# template_files = [f for f in template_nodes if isinstance(f, SCons.Node.FS.File)]


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

