# -*- python -*-
import logger
import ConfigParser
import errno
import io
import logging
import os
import sys
import re
import utils
import SCons.Node.FS
import SCons.Util
from SCons.Script import Mkdir, Chmod, Copy, WhereIs
import shutil

logger = logger.init_default_logger(log_file_prefix="scons-qserv", level=logging.DEBUG)

env = Environment(tools=['default', 'textfile', 'pymod', 'protoc', 'antlr', 'swig', 'recinstall'])

# TODO : manage custom.py, with ARGUMENTS
opts = Variables('custom.py')

def findPrefix(product, binName=None):
    """ returns install prefix for  a dependency named 'product'
    - if the dependency binary is PREFIX/bin/binName then PREFIX is used
    - else env var PRODUCT_DIR is used
    - if no prefix is detected, exit with an error message
    """
    prefix = None
 
    if binName:
        (binpath, binname) = os.path.split(binName)
        (basepath, bin) = os.path.split(binpath)
        if bin.lower() == "bin":
           prefix = basepath

    if not prefix:
        prefix = os.getenv("%s_DIR" % product.upper())

    if not prefix:
        print >> sys.stderr, "Could not locate %s install prefix" % product 
        sys.exit(1)

    return prefix 


#####################
#
# Option management
#
#####################
opts.AddVariables(
        (PathVariable('build_dir', 'Qserv build dir', 'build', PathVariable.PathIsDirCreate)),
        (EnumVariable('debug', 'debug output and symbols', 'no', allowed_values=('yes', 'no', 'full'))),
        (PathVariable('XROOTD_DIR', 'xrootd install dir', findPrefix("XROOTD", "xrootd"), PathVariable.PathIsDir)),
        (PathVariable('MYSQL_DIR', 'mysql install dir', findPrefix("MYSQL", "mysql"), PathVariable.PathIsDir)),
        (PathVariable('MYSQLPROXY_DIR', 'mysqlproxy install dir', findPrefix("MYSQLPROXY", "mysql-proxy"), PathVariable.PathIsDir)),
        (PathVariable('PROTOBUF_DIR', 'protobuf install dir', findPrefix("PROTOBUF", "protoc"), PathVariable.PathIsDir)),
        (PathVariable('LUA_DIR', 'lua install dir', findPrefix("LUA", "lua"), PathVariable.PathIsDir)),
        (PathVariable('PYTHON', 'python binary path', SCons.Util.WhereIs("python"), PathVariable.PathIsFile)),
        (PathVariable('GEOMETRY', 'path to geometry.py', os.getenv("GEOMETRY_LIB"), PathVariable.PathAccept)),
        ('PYTHONPATH', 'pythonpath', os.getenv("PYTHONPATH"))
)
opts.Update(env)

opts.AddVariables(
        (PathVariable('prefix', 'qserv install dir', os.path.join(env['build_dir'], "dist"), PathVariable.PathIsDirCreate)),
        (PathVariable('PROTOC', 'protoc binary path', None, PathVariable.PathIsFile)),
        (PathVariable('XROOTD_INC', 'xrootd include path', os.path.join(env['XROOTD_DIR'], "include", "xrootd"), PathVariable.PathIsDir)),
        (PathVariable('XROOTD_LIB', 'xrootd libraries path', os.path.join(env['XROOTD_DIR'], "lib"), PathVariable.PathIsDir)),
        (PathVariable('MYSQL_INC', 'mysql include path', os.path.join(env['MYSQL_DIR'], "include"), PathVariable.PathIsDir)),
        (PathVariable('MYSQL_LIB', 'mysql libraries path', os.path.join(env['MYSQL_DIR'], "lib"), PathVariable.PathIsDir)),
        (PathVariable('PROTOBUF_INC', 'protobuf include path', os.path.join(env['PROTOBUF_DIR'], "include"), PathVariable.PathIsDir)),
        (PathVariable('PROTOBUF_LIB', 'protobuf libraries path', os.path.join(env['PROTOBUF_DIR'], "lib"), PathVariable.PathIsDir))
)
opts.Update(env)

opts.AddVariables(
    (PathVariable('python_prefix', 'qserv install directory for python modules', os.path.join(env['prefix'], "lib", "python"), PathVariable.PathIsDirCreate))
)
opts.Update(env)

Help(opts.GenerateHelpText(env))

#########################
#
# Defining dependencies
#
#########################

env.Default(env.Alias("build"))
env.Depends("install", env.Alias("build"))

env.Requires(env.Alias('python-tests'), env.Alias('admin'))
env.Requires(env.Alias('python-tests'), env.Alias('dist-qms'))

env.Alias("install",
        [
        env.Alias("dist-core"),
        env.Alias("dist-qms"),
        env.Alias("admin"),
        env.Alias("userconfig"),
        env.Alias("python-tests")
        ]
)

################################
#
# Install tests python modules
#
################################
python_tests = env.InstallPythonModule(target=env['python_prefix'], source=os.path.join("tests", "python"))
env.Alias("python-tests", python_tests)

# Install qms
#########################
qmsbin_target = os.path.join(env['prefix'], "bin")
env.RecursiveInstall(qmsbin_target, os.path.join("meta", "bin"))
python_qms = env.InstallPythonModule(target=env['python_prefix'], source=os.path.join("meta", "python"))
env.Alias("dist-qms",
        [
        python_qms,
        qmsbin_target
        ]
)

#########################
#
# Install admin commands
#
#########################
adminbin_target = os.path.join(env['prefix'], "bin")
env.RecursiveInstall(adminbin_target, os.path.join("admin", "bin"))
python_admin = env.InstallPythonModule(target=env['python_prefix'], source=os.path.join("admin", "python"))

template_target = os.path.join(env['prefix'], "admin", "templates")
env.RecursiveInstall(template_target, os.path.join("admin", "templates"))

sitescons_target = os.path.join(env['prefix'], "admin", "site_scons")
env.RecursiveInstall(sitescons_target, os.path.join("admin", "site_scons"))

env.Alias("admin",
        [
        python_admin,
        template_target,
        sitescons_target,
        adminbin_target,
        env.Install(os.path.join(env['prefix'], "admin"), os.path.join("admin", "SConstruct"))
        ]
)

#############################
#
# Install Qserv code
#
#############################

# Trigger the modules build
############################

# computing install target paths
#################################
def get_install_targets() :

  # Setup the #include paths
  # env.Append(CPPPATH="modules")

  coreFilesToInstall = SConscript('core/modules/SConscript', variant_dir=env['build_dir'], duplicate=1,
    exports=['env', 'ARGUMENTS'])
  targetFiles = []
  for (path, sourceNode) in coreFilesToInstall :
    installPath=os.path.join(env['prefix'], path)
    print "DEBUG : %s %s" % (installPath, sourceNode)
    targetFile = utils.replace_base_path(None, installPath, sourceNode, env)
    env.InstallAs(targetFile, sourceNode)
    targetFiles.append(targetFile)
  return targetFiles

env.Alias("dist-core", get_install_targets())

################################
#
# Fill user configuration file
#
################################

src_dir=Dir('.').srcnode().abspath
config_file_name=os.path.join(src_dir, "admin", "templates", "client", "qserv.conf")
homedir=os.path.expanduser("~")
user_config_dir=os.path.join(homedir, ".lsst")
user_config_file_name=os.path.join(user_config_dir, "qserv.conf")

script_dict = {
  '%\(QSERV_DIR\)s': os.path.join(src_dir, env['prefix']),
  '%\(XROOTD_DIR\)s': env['XROOTD_DIR'],
  '%\(LUA_DIR\)s': env['LUA_DIR'],
  '%\(MYSQL_DIR\)s': env['MYSQL_DIR'],
  '%\(MYSQLPROXY_DIR\)s': env['MYSQLPROXY_DIR'],
  '%\(PYTHON_BIN\)s': env['PYTHON'],
  '%\(PYTHONPATH\)s': env['PYTHONPATH']

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
    env.Help('  %s\n' % alias)

