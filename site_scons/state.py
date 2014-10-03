#  @file
#
# This module acts like a singleton, holding all global state for sconsUtils.
# This includes the primary Environment object (state.env), the message log (state.log),
# the command-line variables object (state.opts), and a dictionary of command-line targets
# used to setup aliases, default targets, and dependencies (state.targets).  All four of
# these variables are aliased to the main lsst.sconsUtils scope, so there should be no
# need for users to deal with the state module directly.
#
# These are all initialized when the module is imported, but may be modified by other code
# (particularly dependencies.configure()).
##

import sys
import os
import SCons.Script
import SCons.Util
from SCons.Environment import *
from SCons.Variables import *

##
#  @brief A dictionary of SCons aliases and targets.
#
#  These are used to setup aliases, default targets, and dependencies by BasicSConstruct.finish().
#  While one can still use env.Alias to setup aliases (and should for "install"), putting targets
#  here will generally provide better build-time dependency handling (like ensuring everything
#  is built before we try to install, and making sure SCons doesn't rebuild the world before
#  installing).
#
#  Users can add additional keys to the dictionary if desired.
#
#  Targets should be added by calling extend() or using += on the dict values, to keep the lists of
#  targets from turning into lists-of-lists.
##

## @cond INTERNAL

env = None
log = None
opts = None

def _findPrefixFromName(product):
    product_envvar = "%s_DIR" % product.upper()
    prefix = os.getenv(product_envvar)
    if not prefix:
        log.fail("Could not locate %s install prefix using %s" % (product, product_envvar))
    return prefix    

def _getBinPath(binName, msg=None):
    if msg == None:
        msg = "Looking for %s" % binName
    log.info(msg)
    binFullPath = SCons.Util.WhereIs(binName)
    if not binFullPath:
        raise SCons.Errors.StopError('Could not locate binary : %s' % binName)
    else:
        return binFullPath

def _getBinPathFromBinList(binList, msg=None):
    binFullPath = None
    i=0
    if msg == None:
        msg = "Looking for %s" % binList
    log.info(msg)
    while i < len(binList) and not binFullPath:
        binName = binList[i]
        binFullPath = SCons.Util.WhereIs(binName)
        i=i+1
    if not binFullPath:
            raise SCons.Errors.StopError('Could not locate at least one binary in : %s' % binList)
    else:
        return binFullPath

def _findPrefixFromBin(key, binName):
    """ returns install prefix for  a dependency named 'product'
    - if the dependency binary is PREFIX/bin/binName then PREFIX is used
    """
    prefix = _findPrefixFromPath(key,  _getBinPath(binName))	
    return prefix

def _findPrefixFromPath(key, binFullPath):
    if not binFullPath :
        log.fail("_findPrefixFromPath : empty path specified for key %s" % key)
    (binpath, binname) = os.path.split(binFullPath)
    (basepath, bin) = os.path.split(binpath)
    if bin.lower() == "bin":
        prefix = basepath

    if not prefix:
        log.fail("Could not locate install prefix for product containing next binary : " % binFullPath )
    return prefix

def _initOptions():
    SCons.Script.AddOption('--verbose', dest='verbose', action='store_true', default=False,
                           help="Print additional messages for debugging.")
    SCons.Script.AddOption('--traceback', dest='traceback', action='store_true', default=False,
                           help="Print full exception tracebacks when errors occur.")

def _initLog():
    import utils
    global log
    log = utils.Log()

    #
    # Process those arguments
    #
    log.verbose = SCons.Script.GetOption('verbose')
    log.traceback = SCons.Script.GetOption('traceback')

def _setEnvWithDependencies():

    log.info("Adding build dependencies information in scons environment")
    opts.AddVariables(
            (EnumVariable('debug', 'debug gcc output and symbols', 'yes', allowed_values=('yes', 'no'))),
            (PathVariable('PROTOC', 'protoc binary path', _getBinPath('protoc',"Looking for protoc compiler"), PathVariable.PathIsFile)),
            # antlr is named runantlr on Ubuntu 13.10 and Debian Wheezy
            (PathVariable('ANTLR', 'antlr binary path', _getBinPathFromBinList(['antlr','runantlr'],'Looking for antlr parser generator'), PathVariable.PathIsFile)),
            (PathVariable('XROOTD_DIR', 'xrootd install dir', _findPrefixFromBin( 'XROOTD_DIR', "xrootd"), PathVariable.PathIsDir)),
            (PathVariable('MYSQL_DIR', 'mysql install dir', _findPrefixFromBin('MYSQL_DIR', "mysqld_safe"), PathVariable.PathIsDir)),
            (PathVariable('MYSQLPROXY_DIR', 'mysqlproxy install dir', _findPrefixFromBin('MYSQLPROXY_DIR', "mysql-proxy"), PathVariable.PathIsDir)),
            (PathVariable('LOG4CXX_DIR', 'log4cxx install dir', _findPrefixFromName('LOG4CXX'), PathVariable.PathIsDir)),
            (PathVariable('LOG_DIR', 'log install dir', _findPrefixFromName('LOG'), PathVariable.PathIsDir)),
            (PathVariable('LUA_DIR', 'lua install dir', _findPrefixFromBin('LUA_DIR', "lua"), PathVariable.PathIsDir)),
            (PathVariable('ZOOKEEPER_DIR', 'zookeeper install dir', _findPrefixFromBin('ZOOKEEPER_DIR', "zkEnv.sh"), PathVariable.PathIsDir)),
            (PathVariable('python_relative_prefix', 'qserv install directory for python modules, relative to prefix', os.path.join("lib", "python"), PathVariable.PathIsDirCreate))
            )
    opts.Update(env)

    opts.AddVariables(
            (PathVariable('PROTOBUF_DIR', 'protobuf install dir', _findPrefixFromPath('PROTOBUF_DIR',env['PROTOC']), PathVariable.PathIsDir)),
            (PathVariable('ANTLR_DIR', 'antlr install dir', _findPrefixFromPath('ANTLR_DIR', env['ANTLR']), PathVariable.PathIsDir)),
    )
    opts.Update(env)

    opts.AddVariables(
            (PathVariable('ANTLR_INC', 'antlr include path', os.path.join(env['ANTLR_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('ANTLR_LIB', 'antlr libraries path', os.path.join(env['ANTLR_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('XROOTD_INC', 'xrootd include path', os.path.join(env['XROOTD_DIR'], "include", "xrootd"), PathVariable.PathIsDir)),
            (PathVariable('XROOTD_LIB', 'xrootd libraries path', os.path.join(env['XROOTD_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('MYSQL_INC', 'mysql include path', os.path.join(env['MYSQL_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('MYSQL_LIB', 'mysql libraries path', os.path.join(env['MYSQL_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('LOG4CXX_INC', 'log4cxx include path', os.path.join(env['LOG4CXX_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('LOG4CXX_LIB', 'log4cxx libraries path', os.path.join(env['LOG4CXX_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('LOG_INC', 'log include path', os.path.join(env['LOG_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('LOG_LIB', 'log libraries path', os.path.join(env['LOG_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('PROTOBUF_INC', 'protobuf include path', os.path.join(env['PROTOBUF_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('PROTOBUF_LIB', 'protobuf libraries path', os.path.join(env['PROTOBUF_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('ZOOKEEPER_INC', 'zookeeper c-binding include path', os.path.join(env['ZOOKEEPER_DIR'], "c-binding", "include"), PathVariable.PathIsDir)),
            (PathVariable('ZOOKEEPER_LIB', 'zookeeper c-binding library path', os.path.join(env['ZOOKEEPER_DIR'], "c-binding", "lib"), PathVariable.PathIsDir))
            )
    opts.Update(env)

    opts.AddVariables(
            (PathVariable('python_prefix', 'qserv install directory for python modules', os.path.join(env['prefix'], env['python_relative_prefix']), PathVariable.PathIsDirCreate))
	    )
    opts.Update(env)

    # Allow one to specify where boost is
    boost_dir = os.getenv("BOOST_DIR")
    if boost_dir:
        opts.AddVariables(
            (PathVariable('BOOST_DIR', 'boost install dir', _findPrefixFromName("BOOST"), PathVariable.PathIsDir)),
            (PathVariable('BOOST_INC', 'boost include path', os.path.join(boost_dir, "include"), PathVariable.PathIsDir)),
            (PathVariable('BOOST_LIB', 'boost libraries path', os.path.join(boost_dir, "lib"), PathVariable.PathIsDir)),
            )
        opts.Update(env)

    SCons.Script.Help(opts.GenerateHelpText(env))


def _setBuildEnv():
    """Construction and basic setup of the state.env variable."""

    env.Tool('recinstall')
    env.Tool('swig_scanner')
    env.Tool('protoc')
    env.Tool('antlr')
    env.Tool('unittest')
    if env['debug'] == 'yes':
        log.info("Debug build flag (-g) requested.")
        env.Append(CCFLAGS = ['-g'])
    # Increase compiler strictness
    env.Append(CCFLAGS=['-pedantic', '-Wall', '-Wno-long-long', '-Wno-variadic-macros'])

    # to make shared libraries link correctly we need -rpath-link option, for now add everything
    # that is in LD_LIBRARY_PATH
    # TODO: this is Linux-gcc-specific, do we have a way to test for a platform we are running on
    if 'LD_LIBRARY_PATH' in os.environ:
        env.Append(LINKFLAGS = ["-Wl,-rpath-link="+os.environ["LD_LIBRARY_PATH"]])

    # SCons resets many envvars to make clean build, we want to pass some of them explicitly.
    # Extend the list if you need to add more.
    for key in ['LD_LIBRARY_PATH',]:
        if key in os.environ:
            env['ENV'][key] = os.environ[key]


# TODO : where to save this file ?
def _saveState():
    """Save state such as optimization level used.  The scons mailing lists were unable to tell
    RHL how to get this back from .sconsign.dblite
    """

    if env.GetOption("clean"):
        return

    import ConfigParser

    config = ConfigParser.ConfigParser()
    config.add_section('Build')
    config.set('Build', 'cc', SCons.Util.WhereIs('gcc'))
    #if env['opt']:
    #    config.set('Build', 'opt', env['opt'])

    try:
        confFile = os.path.join(os.path.join(env['prefix'],"admin"),"configuration.in.cfg")
        with open(confFile, 'wb') as configfile:
            config.write(configfile)
    except Exception, e:
        log.warn("Unexpected exception in _saveState: %s" % e)

def init(src_dir):

    global env, opts
    env = Environment(tools=['default', 'textfile', 'pymod'])
    _initOptions()
    _initLog()

    log.info("Adding general build information to scons environment")
    opts = SCons.Script.Variables("custom.py")
    opts.AddVariables(
            (PathVariable('build_dir', 'Qserv build dir', os.path.join(src_dir, 'build'), PathVariable.PathIsDirCreate)),
            ('PYTHONPATH', 'pythonpath', os.getenv("PYTHONPATH"))
    )
 
    opts.Update(env)

    opts.AddVariables(
            (PathVariable('prefix', 'qserv install dir', os.path.join(env['build_dir'], "dist"), PathVariable.PathIsDirCreate))
    )
    opts.Update(env)

#    _saveState()
## @endcond

def initBuild():
    _setEnvWithDependencies()
    _setBuildEnv()


