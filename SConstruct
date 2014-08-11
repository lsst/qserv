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

src_dir = Dir('.').srcnode().abspath
state.init(src_dir)
env = state.env

#########################
#
# Defining dependencies
#
#########################

env.Default(env.Alias("build"))
env.Depends("build", env.Alias("init-build-env"))
env.Depends("install", env.Alias("build"))

env.Depends("python-tests", env.Alias("init-build-env"))
env.Requires(env.Alias('python-tests'), env.Alias('admin'))
env.Requires(env.Alias('python-tests'), env.Alias('dist-css'))

env.Alias("install",
        [
        env.Alias("dist-core"),
        env.Alias("dist-css"),
        env.Alias("admin"),
        env.Alias("python-tests"),
        env.Alias("templates")
        ]
)

################################
#
# Build documentation 
#
################################
doc = env.Command("build-doc", [], "cd doc && PATH={0} bash build.sh".format(os.getenv("PATH")))
env.Alias("doc", doc)

# documentation generation must be possible even if build
# environment isn't available
if ['doc'] == COMMAND_LINE_TARGETS:
    state.log.debug("Only building Qserv documentation") 

else:
################################
#
# Init build environment 
#
################################
    state.initBuild()

################################
#
# Install tests python modules
#
################################
    python_tests = env.InstallPythonModule(target=env['python_prefix'], source=os.path.join("tests", "python"))
    env.Alias("python-tests", python_tests)

#########################
#
# Install css
#
#########################
    cssbin_target = os.path.join(env['prefix'], "bin")
    env.RecursiveInstall(cssbin_target, os.path.join("css", "bin"))
    python_css = env.InstallPythonModule(
        target=env['python_prefix'],
        source=os.path.join("css", "python")
    )
    env.Alias("dist-css",
    [
        python_css,
        cssbin_target
    ]
    )

#########################
#
# Install admin commands
#
#########################
    adminbin_target = os.path.join(env['prefix'], "bin")
    env.RecursiveInstall(adminbin_target, os.path.join("admin", "bin"))
    env.RecursiveInstall(adminbin_target, os.path.join("tests", "bin"))
    python_admin = env.InstallPythonModule(target=env['python_prefix'], source=os.path.join("admin", "python"))

    template_target = os.path.join(env['prefix'], "admin", "templates")
    env.RecursiveInstall(template_target, os.path.join("admin", "templates"))

    env.Alias("admin",
    [
        python_admin,
        template_target,
        adminbin_target,
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

      (coreFilesToInstall, testTargets) = SConscript('core/modules/SConscript',
        variant_dir=env['build_dir'],
        duplicate=1,
        exports=['env', 'ARGUMENTS']
      )
      targetFiles = []
      for (path, sourceNode) in coreFilesToInstall :
        installPath=os.path.join(env['prefix'], path)
        state.log.debug("%s %s" % (installPath, sourceNode))
        targetFile = fileutils.replace_base_path(None, installPath, sourceNode, env)
        env.InstallAs(targetFile, sourceNode)
        targetFiles.append(targetFile)

      installTargets = targetFiles + testTargets
      state.log.debug("%s " % installTargets)

      return installTargets

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
        '{{QSERV_DIR}}': env['prefix'],
        '{{XROOTD_DIR}}': env['XROOTD_DIR'],
        '{{LUA_DIR}}': env['LUA_DIR'],
        '{{MYSQL_DIR}}': env['MYSQL_DIR'],
        '{{MYSQLPROXY_DIR}}': env['MYSQLPROXY_DIR']
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

