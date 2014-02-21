# -*- python -*-
import actions
import commons
import ConfigParser
import errno
import io
import logging
import os
import sys
import re
import SCons.Node.FS
from SCons.Script import Mkdir,Chmod,Copy,WhereIs
import shutil
import utils

logger = commons.init_default_logger(log_file_prefix="scons-qserv", level=logging.DEBUG)

env = Environment(tools=['textfile', 'pymod'])

#########################
#
# Reading config file
#
#########################

# this file must be placed in main scons directory
src_dir=Dir('.').srcnode().abspath+"/"
config_file_name=os.path.join(src_dir, "qserv-build.conf")

if not os.path.exists(config_file_name):
    logging.fatal("Your configuration file is missing: %s" % config_file_name)
    sys.exit(1)

try:
    config = commons.read_config(config_file_name)
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

init_target_lst = []
make_root_dirs_cmd = env.Command('make-root-dirs-dummy-target', [], actions.check_root_dirs)
init_target_lst.append(make_root_dirs_cmd)

make_root_symlinks_cmd = env.Command('make-root-symlinks-dummy-target', [], actions.check_root_symlinks)
init_target_lst.append(make_root_symlinks_cmd)

env.Alias('init', init_target_lst)

#########################
#
# Defining dependencies
#
#########################

env.Requires(env.Alias('perl-install'), env.Alias('init'))
# templates must be applied before installation in order to
# initialize mysqld
env.Requires(env.Alias('perl-install'), env.Alias('config'))
env.Requires(env.Alias('perl-init-mysql-db'), env.Alias('templates'))
env.Requires(env.Alias('python-tests'), env.Alias('python-admin'))
env.Requires(env.Alias('python-tests'), env.Alias('python-qms'))
env.Requires(env.Alias('admin-bin'), env.Alias('python-tests'))
# next target install all python files, usefull for development purposes
env.Requires(env.Alias('admin-bin'), env.Alias('python'))
env.Alias("python",
    [
        env.Alias("python-admin"),
        env.Alias("python-qms"),
        env.Alias("python-tests"),
    ]
)
env.Requires(env.Alias('perl-install'), env.Alias('admin-bin'))

env.Alias('install',env.Alias('perl-install'))

env.Default(env.Alias('install'))

#########################
#
# Defining Install Alias
#
#########################

for perl_option in ('install', 'init-mysql-db', 'clean-all'):
    scons_target = "perl-%s" % perl_option
    env.Alias(scons_target, env.Command(scons_target+'-dummy-target', [], actions.build_cmd_with_opts(config,perl_option)))

######################################################
#
# Templating system
# fill Qserv config files with qserv-build.conf values
#
######################################################

def get_template_targets():

    template_dir_path= os.path.normpath("admin/custom")
    target_lst = []

    script_dict = {
        '%\(QMS_HOST\)s': config['qserv']['master'],
        '%\(QMS_DB\)s': config['qms']['db'],
        '%\(QMS_USER\)s': config['qms']['user'],
        '%\(QMS_PASS\)s': config['qms']['pass'],
        '%\(QMS_PORT\)s': config['qms']['port'],
        '%\(QSERV_BASE_DIR\)s': config['qserv']['base_dir'],
        '%\(QSERV_UNIX_USER\)s': os.getlogin(),
        '%\(QSERV_SRC_DIR\)s': config['src_dir'],
        '%\(QSERV_LOG_DIR\)s': config['qserv']['log_dir'],
        '%\(QSERV_PID_DIR\)s': os.path.join(config['qserv']['base_dir'],'var/run'),
        '%\(QSERV_RPC_PORT\)s': config['qserv']['rpc_port'],
        '%\(QSERV_LUA_SHARE\)s': os.path.join(config['qserv']['base_dir'],"share","lua","5.1"),
        '%\(QSERV_LUA_LIB\)s': os.path.join(config['qserv']['base_dir'],"lib","lua","5.1"),
        '%\(QSERV_SCRATCH_DIR\)s': config['qserv']['scratch_dir'],
        '%\(MYSQLD_DATA_DIR\)s': config['mysqld']['data_dir'],
        '%\(MYSQLD_PORT\)s': config['mysqld']['port'],
        # used for mysql-proxy in mono-node
        '%\(MYSQLD_HOST\)s': '127.0.0.1',
        '%\(MYSQLD_SOCK\)s': config['mysqld']['sock'],
        '%\(MYSQLD_USER\)s': config['mysqld']['user'],
        '%\(MYSQLD_PASS\)s': config['mysqld']['pass'],
        '%\(MYSQL_PROXY_PORT\)s': config['mysql_proxy']['port'],
        '%\(XROOTD_MANAGER_HOST\)s': config['qserv']['master'],
        '%\(XROOTD_PORT\)s': config['xrootd']['xrootd_port'],
        '%\(XROOTD_RUN_DIR\)s': os.path.join(config['qserv']['base_dir'],'xrootd-run'),
        '%\(XROOTD_ADMIN_DIR\)s': os.path.join(config['qserv']['base_dir'],'tmp'),
        '%\(CMSD_MANAGER_PORT\)s': config['xrootd']['cmsd_manager_port'],
        '%\(HOME\)s': os.path.expanduser("~") 
        }

    if config['qserv']['node_type']=='mono':
        script_dict['%\(COMMENT_MONO_NODE\)s']='#MONO-NODE# '
    else:
        script_dict['%\(COMMENT_MONO_NODE\)s']=''

    logger.info("Applying configuration information via templates files ")

    for src_node in utils.recursive_glob(template_dir_path,"*",env):

        target_node = utils.replace_base_path(template_dir_path,config['qserv']['base_dir'],src_node,env)

        if isinstance(src_node, SCons.Node.FS.File) :

            logger.debug("Template SOURCE : %s, TARGET : %s" % (src_node, target_node))
            env.Substfile(target_node, src_node, SUBST_DICT=script_dict)
            target_lst.append(target_node)
            # qserv-admin has no extension, Substfile can't manage it easily
            # TODO : qserv-admin could be modified in order to be removed to
            # template files, so that next test could be removed
            target_name = str(target_node)
            f="qserv-admin.pl"
            if os.path.basename(target_name)	== f :
                symlink_name, file_ext = os.path.splitext(target_name)
                env.Command(symlink_name, target_node, actions.symlink)
                target_lst.append(symlink_name)

            path = os.path.dirname(target_name)
            target_basename = os.path.basename(target_name)
            logger.debug("TARGET BASENAME : %s" % target_basename)
            if (os.path.basename(path) == "bin" or 
                os.path.basename(path) == "init.d" or
                target_basename in [
                "start_xrootd",
                "start_qserv",
                "start_mysqlproxy",
                "start_qms",
                "qserv-master.sh",
                "scisql.sh",
                "protobuf.sh",
                "qms.sh"
                ]
                ):
                env.AddPostAction(target_node, Chmod("$TARGET", 0760))
            # all other files are configuration files
            else:
                env.AddPostAction(target_node, Chmod("$TARGET", 0660))

    return target_lst

env.Alias("templates", get_template_targets())

############################
#
# Fill configuration files
#
############################

homedir=os.path.expanduser("~")
user_config_dir=os.path.join(homedir,".lsst") 
user_config_file_name=os.path.join(user_config_dir, "qserv.conf")
make_user_config_cmd = env.Command(user_config_file_name, config_file_name, Copy("$TARGET", "$SOURCE"))

env.Alias("config", [env.Alias("templates"),make_user_config_cmd])

#########################
#
# Install python modules
#
#########################
python_path_prefix=config['qserv']['base_dir']

env['python_path_prefix']=python_path_prefix

python_admin = env.InstallPythonModule(target=python_path_prefix, source='admin/python')
#python_targets=utils.build_python_module(source='admin/python',target='/opt/qserv-dev',env=env)
env.Alias("python-admin", python_admin)

python_tests = env.InstallPythonModule(target=python_path_prefix, source='tests/python')
env.Alias("python-tests", python_tests)

SConscript('meta/SConscript', exports = 'env')

#########################
#
# Install admin commands
#
#########################
bin_basename_lst=[
    "qserv-benchmark.py","qserv-testdata.py",
    "qserv-testunit.py","qms_setup.sh"
]
bin_target_lst = []
for f in bin_basename_lst:
    source = os.path.join("admin","bin",f)
    target = os.path.join(config['qserv']['base_dir'],"bin",f)
    Command(target, source, Copy("$TARGET", "$SOURCE"))
    bin_target_lst.append(target)

env.Alias("admin-bin", bin_target_lst)

#########################
#
# Install qms 
#
#########################
if config['qserv']['node_type'] in ['mono','master']:

    # MySQL is required 
    env.Requires(env.Alias('qms'), env.Alias('perl-install'))
    env.Requires(env.Alias('install'), env.Alias('qms'))

    qms_cmd=[] 
    qms_install_sh = os.path.join(config['qserv']['base_dir'],'tmp','install', "qms.sh")
    cmd = env.Command('qms-only', [], qms_install_sh)
    qms_cmd.append(cmd)

    env.Alias("qms", qms_cmd)

#########################
#
# Install scisql
#
#########################
if config['qserv']['node_type'] in ['mono','worker']:

    # MySQL is required 
    env.Requires(env.Alias('scisql'), env.Alias('perl-install'))
    env.Requires(env.Alias('install'), env.Alias('scisql'))

    scisql_cmd=[] 
    scisql_install_sh = os.path.join(config['qserv']['base_dir'],'tmp','install', "scisql.sh")
    cmd = env.Command('scisql-only', [], scisql_install_sh)
    scisql_cmd.append(cmd)

    env.Alias("scisql", scisql_cmd)

#############################
#
# Install master and worker
#
#############################

# MySQL is required 
env.Requires(env.Alias('qserv'), env.Alias('perl-install'))
env.Requires(env.Alias('install'), env.Alias('qserv'))

qserv_cmd=[] 
qserv_install_sh = os.path.join(config['qserv']['base_dir'],'tmp','install', "qserv-master.sh")
cmd = env.Command('qserv-only', [], qserv_install_sh)
qserv_cmd.append(cmd)

env.Alias("qserv", qserv_cmd)

#########################
#
# Uninstall everything:
#
#########################
if 'uninstall' in COMMAND_LINE_TARGETS:
    paths = [
            os.path.join(config['qserv']['log_dir']),
            os.path.join(config['mysqld']['data_dir']),
            os.path.join(config['qserv']['base_dir'],'qserv','master','dist'),
            os.path.join(config['qserv']['base_dir'],'qserv','worker','dist'),
            os.path.join(config['qserv']['base_dir']),
            os.path.join(config['qserv']['scratch_dir']),
            user_config_dir
            ]

    env['uninstallpaths'] = paths
    uninstall_cmd = env.Command('uninstall-dummy-target', [], actions.uninstall)
    env.Alias("uninstall", uninstall_cmd)

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

