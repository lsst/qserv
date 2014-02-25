# -*- python -*-
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

env = Environment(tools=['default', 'textfile', 'pymod', 'protoc', 'antlr', 'swig'])

#####################
#
# Build dir
#
#####################
AddOption('--build',
        dest='buildDir',
        type='string',
        nargs=1,
        action='store',
        metavar='DIR',
        default= 'build',
        help='Qserv build dir')

buildDir = GetOption('buildDir')
print "Building into", buildDir

#####################
#
# Install prefix
#
#####################
AddOption('--prefix',
        dest='prefix',
        type='string',
        nargs=1,
        action='store',
        metavar='DIR',
        default= os.path.join(buildDir,'dist'),
        help='Qserv installation prefix')

PREFIX = GetOption('prefix')

PYTHON_PREFIX = os.path.join(PREFIX, "lib", "python")

#########################
#
# Reading config file
#
#########################

# config['qserv']['base_dir']=
# env['config']=config

#########################
#
# Defining dependencies
#
#########################

env.Default(env.Alias("build"))
env.Depends(env.Alias("install"), env.Alias("build"))

env.Depends(env.Alias('python-tests'), env.Alias('python-admin'))
env.Depends(env.Alias('python-tests'), env.Alias('python-qms'))
env.Depends(env.Alias('admin-bin'), env.Alias('python'))

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
        env.Alias("config")
    ]
)

#########################
#
# Install python modules
#
#########################
python_admin = env.InstallPythonModule(target=PYTHON_PREFIX, source='admin/python')
env.Alias("python-admin", python_admin)

python_tests = env.InstallPythonModule(target=PYTHON_PREFIX, source='tests/python')
env.Alias("python-tests", python_tests)

# Install qms
#########################
python_qms = env.InstallPythonModule(target=PYTHON_PREFIX, source='meta/python')
env.Alias("qms", python_qms)

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
    target = os.path.join(PREFIX,"bin",f)
    Command(target, source, Copy("$TARGET", "$SOURCE"))
    bin_target_lst.append(target)

env.Alias("admin-bin", bin_target_lst)

#############################
#
# Install Qserv code
#
#############################

# Trigger the modules build
############################
# Setup the #include paths
env.Append(CPPPATH="modules")

filesToInstall = SConscript('core/modules/SConscript', variant_dir=buildDir, duplicate=1,
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

env.Alias("dist", get_install_targets(PREFIX,filesToInstall))

############################
#
# Fill configuration files
#
############################

src_dir=Dir('.').srcnode().abspath
config_file_name=os.path.join(src_dir, "qserv-build.conf")
homedir=os.path.expanduser("~")
user_config_dir=os.path.join(homedir,".lsst")
user_config_file_name=os.path.join(user_config_dir, "qserv.conf")

script_dict = {
    '%\(QSERV_DIR\)s': PREFIX,
    '%\(XROOTD_DIR\)s': env['XROOTD_DIR'],
    '%\(MYSQL_DIR\)s': env['MYSQL_DIR'],
    '%\(MYSQLPROXY_DIR\)s': env['MYSQLPROXY_DIR']
}

make_user_config_cmd = env.Substfile(user_config_file_name, config_file_name, SUBST_DICT=script_dict)

env.Alias("config", [make_user_config_cmd])

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

