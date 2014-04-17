#  @file state.py
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


def _findPrefix(product, binName=None):
    """ returns install prefix for  a dependency named 'product'
    - if the dependency binary is PREFIX/bin/binName then PREFIX is used
    - else env var PRODUCT_DIR is used
    - if no prefix is detected, exit with an error message
    """
    prefix = None

    if binName:
        binFullPath = SCons.Util.WhereIs(binName)
        (binpath, binname) = os.path.split(binFullPath)
        (basepath, bin) = os.path.split(binpath)
        if bin.lower() == "bin":
           prefix = basepath

    if not prefix:
        prefix = os.getenv("%s_DIR" % product.upper())

    if not prefix:
        log.fails("Could not locate %s install prefix" % product)

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

def _initVariables(src_dir):
    opts = SCons.Script.Variables("custom.py")
    opts.AddVariables(
            (PathVariable('build_dir', 'Qserv build dir', os.path.join(src_dir,'build'), PathVariable.PathIsDirCreate)),
            (EnumVariable('debug', 'debug gcc output and symbols', 'yes', allowed_values=('yes', 'no'))),
            (PathVariable('XROOTD_DIR', 'xrootd install dir', _findPrefix("XROOTD", "xrootd"), PathVariable.PathIsDir)),
            (PathVariable('MYSQL_DIR', 'mysql install dir', _findPrefix("MYSQL", "mysql"), PathVariable.PathIsDir)),
            (PathVariable('MYSQLPROXY_DIR', 'mysqlproxy install dir', _findPrefix("MYSQLPROXY", "mysql-proxy"), PathVariable.PathIsDir)),
            (PathVariable('PROTOBUF_DIR', 'protobuf install dir', _findPrefix("PROTOBUF", "protoc"), PathVariable.PathIsDir)),
            (PathVariable('LUA_DIR', 'lua install dir', _findPrefix("LUA", "lua"), PathVariable.PathIsDir)),
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

    print "DEBUG " + str(opts)
    print "DEBUG " + env.Dump()

    opts.AddVariables(
            (PathVariable('python_prefix', 'qserv install directory for python modules', os.path.join("lib", "python"), PathVariable.PathIsDirCreate))
            )
    opts.Update(env)

    SCons.Script.Help(opts.GenerateHelpText(env))


def _initEnvironment(src_dir):
    """Construction and basic setup of the state.env variable."""

    global env
    env = Environment(tools=['default', 'textfile', 'pymod', 'protoc', 'antlr', 'swig', 'recinstall'])

    _initVariables(src_dir)

    if env['debug'] == 'yes':
        log.info("Debug build flag (-g) requested.")
        env.Append(CCFLAGS = ['-g'])

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
    _initOptions()
    _initLog()
    _initEnvironment(src_dir)
#    _saveState()
## @endcond
