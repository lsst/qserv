# -*- python -*-
import logger 
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

logger = logger.init_default_logger(log_file_prefix="scons-qserv", level=logging.DEBUG)

env = Environment(tools=['default', 'textfile', 'pymod', 'protoc', 'antlr', 'swig', 'recinstall'])

# TODO : manage custom.py, with ARGUMENTS
opts = Variables('custom.py')


#####################
#
# Option management 
#
#####################
opts.AddVariables(
        (PathVariable('build_dir','Qserv build dir','build',PathVariable.PathIsDirCreate)),
        (PathVariable('XROOTD_DIR','xrootd install dir',os.getenv("XROOTD_DIR"),PathVariable.PathIsDir)),
        (PathVariable('MYSQL_DIR','mysql install dir',os.getenv("MYSQL_DIR"),PathVariable.PathIsDir)),
        (PathVariable('MYSQLPROXY_DIR','mysqlproxy install dir',os.getenv("MYSQLPROXY_DIR"),PathVariable.PathIsDir)),
        (PathVariable('PROTOBUF_DIR', 'protobuf install dir',os.getenv("PROTOBUF_DIR"),PathVariable.PathIsDir)),
        (PathVariable('LUA_DIR', 'lua install dir',os.getenv("LUA_DIR"),PathVariable.PathIsDir)),
        (PathVariable('PROTOC', 'protoc install dir',os.path.join("$PROTOBUF_DIR","bin","protoc"),PathVariable.PathIsFile)),
        (PathVariable('prefix', 'qserv install dir',os.path.join("$build_dir","dist"),PathVariable.PathIsDirCreate)),
        (PathVariable('python_prefix', 'qserv install dir for python modules',os.path.join("$prefix","lib","python"),PathVariable.PathIsDirCreate))
)

opts.Update(env)

Help(opts.GenerateHelpText(env))

#########################
#
# Defining dependencies
#
#########################

env.Default(env.Alias("build"))
env.Depends(env.Alias("install"), env.Alias("build"))

env.Depends(env.Alias('python-tests'), env.Alias('python-admin'))
env.Depends(env.Alias('python-tests'), env.Alias('python-qms'))
env.Depends(env.Alias('admin'), env.Alias('python'))

env.Alias("python",
    [
        env.Alias("python-admin"),
        env.Alias("python-qms"),
        env.Alias("python-tests"),
    ]
)

env.Alias("install",
    [
        env.Alias("dist"),
        env.Alias("admin"),
        env.Alias("userconfig")
    ]
)

#########################
#
# Install python modules
#
#########################
python_admin = env.InstallPythonModule(target=env['python_prefix'], source='admin/python')
env.Alias("python-admin", python_admin)

python_tests = env.InstallPythonModule(target=env['python_prefix'], source='tests/python')
env.Alias("python-tests", python_tests)

# Install qms
#########################
python_qms = env.InstallPythonModule(target=env['python_prefix'], source='meta/python')
env.Alias("qms", python_qms)

#########################
#
# Install admin commands
#
#########################
template_target = os.path.join(env['prefix'],"admin","templates")
env.RecursiveInstall(template_target, os.path.join("admin","templates"))

sitescons_target = os.path.join(env['prefix'],"admin","site_scons")
env.RecursiveInstall(sitescons_target, os.path.join("admin","site_scons"))

env.Alias("admin", 
        [
        template_target,
        sitescons_target,
        env.Install(os.path.join(env['prefix'],"admin"), os.path.join("admin","SConstruct"))
        ]
)

#############################
#
# Install Qserv code
#
#############################

# Trigger the modules build
############################
# Setup the #include paths
env.Append(CPPPATH="modules")

filesToInstall = SConscript('core/modules/SConscript', variant_dir=env['build_dir'], duplicate=1,
        exports=['env', 'ARGUMENTS'])

# computing install target paths
#################################
def get_install_targets(prefix, filesToInstall) :
    targetDirSet = set()
    for (path, f) in filesToInstall :
        targetDir = os.path.join(prefix, path)
        env.Install(targetDir, f)
        targetDirSet.add(targetDir)
    return list(targetDirSet)

env.Alias("dist", get_install_targets(env['prefix'],filesToInstall))

############################
#
# Fill configuration files
#
############################

src_dir=Dir('.').srcnode().abspath
config_file_name=os.path.join(src_dir, "admin", "templates", "client", "qserv.conf")
homedir=os.path.expanduser("~")
user_config_dir=os.path.join(homedir,".lsst")
user_config_file_name=os.path.join(user_config_dir, "qserv.conf")

script_dict = {
    '%\(QSERV_DIR\)s': os.path.join(src_dir,env['prefix']),
    '%\(XROOTD_DIR\)s': env['XROOTD_DIR'],
    '%\(LUA_DIR\)s': env['LUA_DIR'],
    '%\(MYSQL_DIR\)s': env['MYSQL_DIR'],
    '%\(MYSQLPROXY_DIR\)s': env['MYSQLPROXY_DIR']
}

make_user_config_cmd = env.Substfile(user_config_file_name, config_file_name, SUBST_DICT=script_dict)

env.Alias("userconfig", [make_user_config_cmd])

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

