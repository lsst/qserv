# -*- python -*-
import ConfigParser
import errno
import io
import logging
import os
import sys
import re
import fileutils
import SCons.Node.FS
import SCons.Util
from SCons.Script import Mkdir, Chmod, Copy, WhereIs
import shutil
import state

src_dir=Dir('.').srcnode().abspath
state.init(src_dir)
env = state.env 

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
        env.Alias("python-tests"),
        env.Alias("templates")
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
    state.log.debug("%s %s" % (installPath, sourceNode))
    targetFile = fileutils.replace_base_path(None, installPath, sourceNode, env)
    env.InstallAs(targetFile, sourceNode)
    targetFiles.append(targetFile)
  return targetFiles

env.Alias("dist-core", get_install_targets())

#########################################
#
# Templates : 
# - fill user configuration file
# - alias for Qserv start/stop commands
#
#########################################

def get_template_targets():

    template_dir_path= os.path.join("admin","templates", "install")
    target_lst = []

    state.log.info("Applying configuration information via templates files ")

    script_dict = {
        '%\(QSERV_DIR\)s': env['prefix'],
        '%\(XROOTD_DIR\)s': env['XROOTD_DIR'],
        '%\(LUA_DIR\)s': env['LUA_DIR'],
        '%\(MYSQL_DIR\)s': env['MYSQL_DIR'],
        '%\(MYSQLPROXY_DIR\)s': env['MYSQLPROXY_DIR']
    }

    for src_node in fileutils.recursive_glob(template_dir_path, "*", env):

        target_node = fileutils.replace_base_path(template_dir_path, env['prefix'], src_node,env)

        if isinstance(src_node, SCons.Node.FS.File) :

            state.log.debug("Template SOURCE : %s, TARGET : %s" % (src_node, target_node))
            env.Substfile(target_node, src_node, SUBST_DICT=script_dict)
            target_lst.append(target_node)

    return target_lst

env.Alias("templates", get_template_targets())

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

