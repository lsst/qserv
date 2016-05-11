# -*- python -*-
# Qserv top-level SConstruct
import os
import fileutils
import SCons.Node.FS
import state

srcDir = Dir('.').srcnode().abspath
state.init(srcDir)
env = state.env

#########################
#
# Defining dependencies
#
#########################

# by default run build+test
all = env.Alias("all", [env.Alias("build"), env.Alias("test")])
env.Default(all)

env.Alias("install-notest",
          [env.Alias("build"),
           env.Alias("admin"),
           env.Alias("templates")])
env.Alias("install",
          [env.Alias("test"),
           env.Alias("install-notest")])

################################
#
# Build documentation
#
################################
doc = env.Command("build-doc", [],
                  "cd doc && PATH={0} bash build.sh".format(os.getenv("PATH")))
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
    env.Replace(configuration_prefix = os.path.join( env['prefix'], "share", "qserv", "configuration"))

#########################
#
# Install admin commands
#
#########################
    adminbin_target = os.path.join(env['prefix'], "bin")
    adminbin_targets = env.RecursiveInstall(adminbin_target, os.path.join("admin", "bin"))
    python_admin = env.InstallPythonModule(target=env['python_prefix'],
                                           source=os.path.join("admin", "python"))

    template_target = os.path.join(env['configuration_prefix'], "templates")
    template_source = os.path.join("admin", "templates", "configuration")
    template_targets = env.RecursiveInstall(template_target, template_source)

    env.Alias("admin",
              [python_admin,
               template_targets,
               adminbin_targets])

#############################
#
# Install Qserv code
#
#############################

# Trigger the modules build
############################

    SConscript('core/modules/SConscript',
               variant_dir=env['build_dir'],
               duplicate=0,
               exports=['env'])

#########################################
#
# Templates :
# - fill user configuration file
# - alias for Qserv start/stop commands
#
#########################################

    def get_template_targets():

        template_dir_path = os.path.join("admin", "templates", "installation")
        target_lst = []

        state.log.info("Applying configuration information " + 
                        "via templates files located in " +
                        "{0}".format(template_dir_path) 
        )

        script_dict = {'{{QSERV_DIR}}': os.path.abspath(env['prefix']),
                       '{{QSERV_PKGAUTOVERSION}}': env['QSERV_PKGAUTOVERSION'],
                       '{{XROOTD_DIR}}': env['XROOTD_DIR'],
                       '{{LUA_DIR}}': env['LUA_DIR'],
                       '{{MYSQL_DIR}}': env['MYSQL_DIR'],
                       '{{MYSQLPROXY_DIR}}': env['MYSQLPROXY_DIR']
                       }

        for src_node in fileutils.recursive_glob(template_dir_path, "*", env):

            target_node = fileutils.replace_base_path(template_dir_path, env['configuration_prefix'], src_node, env)

            if isinstance(src_node, SCons.Node.FS.File):

                state.log.debug("Template SOURCE : %s, TARGET : %s" % (src_node, target_node))
                env.Substfile(target_node, src_node, SUBST_DICT=script_dict)
                target_lst.append(target_node)

        return target_lst

    env.Alias("templates", get_template_targets())

    # Clean empty build/install directories (based on current target list)
    if env.GetOption('clean'):
        env.CleanEmptyDirs(BUILD_TARGETS, 'bin build share lib proxy')

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

