# LSST Data Management System
# Copyright 2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
This module acts like a singleton, holding all global state for Qserv scons script.
This includes the primary Environment object (state.env), the message log (state.log),
the command-line variables object (state.opts).

These are all initialized when the module is imported, but may be modified by other code.

@author  Fabrice Jammes, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import os

# ----------------------------
# Imports for other modules --
# ----------------------------
import SCons.Script
import SCons.Util
from SCons.Environment import *
from SCons.Variables import *

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------


# -----------------------
# Exported definitions --
# -----------------------
env = None
# custom logger
log = None
opts = None


def _findPrefixFromName(product):
    product_envvar = "%s_DIR" % product.upper()
    prefix = os.getenv(product_envvar)
    if not prefix:
        log.fail("Could not locate %s install prefix using %s" % (product, product_envvar))
    return prefix


def _getBinPath(binName, msg=None):
    if msg is None:
        msg = "Looking for %s" % binName
    log.info(msg)
    binFullPath = SCons.Util.WhereIs(binName)
    if not binFullPath:
        raise SCons.Errors.StopError('Could not locate binary : %s' % binName)
    else:
        log.debug("Found %s here %s" % (binName, binFullPath))
        return binFullPath


def _getBinPathFromBinList(binList, msg=None):
    binFullPath = None
    i = 0
    if msg is None:
        msg = "Looking for %s" % binList
    log.info(msg)
    while i < len(binList) and not binFullPath:
        binName = binList[i]
        binFullPath = SCons.Util.WhereIs(binName)
        i = i+1
    if not binFullPath:
        raise SCons.Errors.StopError('Could not locate at least one binary in : %s' % binList)
    else:
        return binFullPath


def _findPrefixFromBin(key, binName):
    """ returns install prefix for  a dependency named 'product'
    - if the dependency binary is PREFIX/bin/binName then PREFIX is used
    """
    prefix = _findPrefixFromPath(key, _getBinPath(binName))
    return prefix


def _findPrefixFromPath(key, binFullPath):
    if not binFullPath:
        log.fail("_findPrefixFromPath : empty path specified for key %s" % key)
    (binpath, binname) = os.path.split(binFullPath)
    (basepath, bin) = os.path.split(binpath)
    if bin.lower() == "bin":
        prefix = basepath

    if not prefix:
        log.fail("Could not locate install prefix for product containing next binary : %s", binFullPath)
    return prefix


def _initOptions():
    SCons.Script.AddOption('--verbose', dest='verbose', action='store_true', default=False,
                           help="Print additional messages for debugging.")
    SCons.Script.AddOption('--traceback', dest='traceback', action='store_true', default=False,
                           help="Print full exception tracebacks when errors occur.")


def _initLog():
    import utils
    global log
    log = utils.Log(SCons.Script.GetOption('verbose'),
                    SCons.Script.GetOption('silent'),
                    SCons.Script.GetOption('traceback'))


def _setEnvWithDependencies():
    log.info("Adding build dependencies information in scons environment")
    opts.AddVariables(
        (EnumVariable('debug', 'debug gcc output and symbols', 'yes', allowed_values=('yes', 'no'))),
        (PathVariable('PROTOC', 'protoc binary path', _getBinPath('protoc', "Looking for protoc compiler"),
                      PathVariable.PathIsFile)),
        (PathVariable('SWIG_BIN', 'swig binary path', _getBinPath('swig', "Looking for swig preprocessor"),
                      PathVariable.PathIsFile)),
        # antlr is named runantlr on Ubuntu 13.10 and Debian Wheezy
        (PathVariable('ANTLR', 'antlr binary path',
                      _getBinPathFromBinList(['antlr', 'runantlr'], 'Looking for antlr parser generator'),
                      PathVariable.PathIsFile)),
        (PathVariable('XROOTD_DIR', 'xrootd install dir', _findPrefixFromBin('XROOTD_DIR', "xrootd"),
                      PathVariable.PathIsDir)),
        (PathVariable('MYSQL_DIR', 'mysql install dir', _findPrefixFromBin('MYSQL_DIR', "mysqld"),
                      PathVariable.PathIsDir)),
        (PathVariable('MYSQLPROXY_DIR', 'mysqlproxy install dir',
                      _findPrefixFromBin('MYSQLPROXY_DIR', "mysql-proxy"),
                      PathVariable.PathIsDir)),
        (PathVariable('LIBCURL_DIR', 'libcurl install dir', _findPrefixFromName('LIBCURL'),
                      PathVariable.PathIsDir)),
        (PathVariable('LOG4CXX_DIR', 'log4cxx install dir', _findPrefixFromName('LOG4CXX'),
                      PathVariable.PathIsDir)),
        (PathVariable('LOG_DIR', 'log install dir', _findPrefixFromName('LOG'),
                      PathVariable.PathIsDir)),
        (PathVariable('LUA_DIR', 'lua install dir', _findPrefixFromBin('LUA_DIR', "lua"),
                      PathVariable.PathIsDir)),
        (PathVariable('SPHGEOM_DIR', 'sphgeom library install dir', _findPrefixFromName('SPHGEOM'),
                      PathVariable.PathIsDir)),
        (PathVariable('python_relative_prefix',
                      'qserv install directory for python modules, relative to prefix',
                      os.path.join("lib", "python"), PathVariable.PathAccept))
    )
    opts.Update(env)

    opts.AddVariables(
        (PathVariable('PROTOBUF_DIR', 'protobuf install dir',
                      _findPrefixFromPath('PROTOBUF_DIR', env['PROTOC']), PathVariable.PathIsDir)),
            (PathVariable('ANTLR_DIR', 'antlr install dir',
             _findPrefixFromPath('ANTLR_DIR', env['ANTLR']), PathVariable.PathIsDir)),
    )
    opts.Update(env)

    opts.AddVariables(
        (PathVariable('ANTLR_INC', 'antlr include path',
                      os.path.join(env['ANTLR_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('ANTLR_LIB', 'antlr libraries path',
             os.path.join(env['ANTLR_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('XROOTD_INC', 'xrootd include path', os.path.join(
                env['XROOTD_DIR'], "include", "xrootd"), PathVariable.PathIsDir)),
            (PathVariable('XROOTD_LIB', 'xrootd libraries path',
             os.path.join(env['XROOTD_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('MYSQL_INC', 'mysql include path',
             os.path.join(env['MYSQL_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('MYSQL_LIB', 'mysql libraries path',
             os.path.join(env['MYSQL_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('LIBCURL_INC', 'libcurl include path',
             os.path.join(env['LIBCURL_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('LIBCURL_LIB', 'libcurl libraries path',
             os.path.join(env['LIBCURL_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('LOG4CXX_INC', 'log4cxx include path',
             os.path.join(env['LOG4CXX_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('LOG4CXX_LIB', 'log4cxx libraries path',
             os.path.join(env['LOG4CXX_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('LOG_INC', 'log include path', os.path.join(
                env['LOG_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('LOG_LIB', 'log libraries path',
             os.path.join(env['LOG_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('PROTOBUF_INC', 'protobuf include path',
             os.path.join(env['PROTOBUF_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('PROTOBUF_LIB', 'protobuf libraries path',
             os.path.join(env['PROTOBUF_DIR'], "lib"), PathVariable.PathIsDir)),
            (PathVariable('LUA_INC', 'lua include path', os.path.join(
                env['LUA_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('SPHGEOM_INC', 'sphgeom include path',
             os.path.join(env['SPHGEOM_DIR'], "include"), PathVariable.PathIsDir)),
            (PathVariable('SPHGEOM_LIB', 'sphgeom libraries path',
             os.path.join(env['SPHGEOM_DIR'], "lib"), PathVariable.PathIsDir)),
    )
    opts.Update(env)

    opts.AddVariables(
        (PathVariable('python_prefix', 'qserv install directory for python modules',
                      os.path.join(env['prefix'], env['python_relative_prefix']), PathVariable.PathAccept))
    )
    opts.Update(env)

    # Allow one to specify where boost is
    boost_dir = os.getenv("BOOST_DIR")
    if boost_dir:
        opts.AddVariables(
            (PathVariable('BOOST_DIR', 'boost install dir',
             _findPrefixFromName("BOOST"), PathVariable.PathIsDir)),
            (PathVariable('BOOST_INC', 'boost include path',
             os.path.join(boost_dir, "include"), PathVariable.PathIsDir)),
            (PathVariable('BOOST_LIB', 'boost libraries path',
             os.path.join(boost_dir, "lib"), PathVariable.PathIsDir)),
        )
        opts.Update(env)

    # SWIG_LIB_ENV specification: Useful when swig can't find its own *.i files
    # This is a SWIG_LIB envvar, read by swig, not the same as the location of
    # swig's *.so libs. For system-installed swig, this is usually /usr/share/swig/x.y.z
    # or path/to/lsst/stack/Linux64/swig/x.y.z/share/swig/x.y.z
    # No validation: a custom SWIG_LIB_ENV is not essential.
    opts.AddVariables(
        (PathVariable('SWIG_LIB_ENV',
                      'swig SWIG_LIB envvar for broken swig setups',
                      None,
                      PathVariable.PathIsDir))
    )
    opts.Update(env)
    if 'SWIG_LIB_ENV' in env:  # PathIsDir tests os.path.isdir
        env['ENV']['SWIG_LIB'] = env['SWIG_LIB_ENV']

    # if during load of 'default' tools (i.e. Environment construction):
    #   1. swig binary path was not in default internal scons PATH
    #      (this prevent recognition of .i files by scons and break the build)
    #   2. swig binary path was not the one specified by the user
    # then it is required to add it and reload the swig scons tool.
    if SCons.Util.WhereIs("swig", env['ENV']['PATH']) != env['SWIG_BIN']:
        swig_dirname = os.path.dirname(env['SWIG_BIN'])
        env.PrependENVPath('PATH', swig_dirname)
        env.Tool('swig')
        log.debug("swig tool loaded using: {0}".format(env['SWIG_BIN']))

    SCons.Script.Help(opts.GenerateHelpText(env))


def _setBuildEnv():
    """Construction and basic setup of the state.env variable."""

    env.Tool('compiler')
    env.Tool('recinstall')
    env.Tool('swig_scanner')
    env.Tool('protoc')
    env.Tool('antlr')
    env.Tool('unittest')
    env.Tool('dirclean')


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
    # if env['opt']:
    #    config.set('Build', 'opt', env['opt'])

    try:
        confFile = os.path.join(os.path.join(env['prefix'], "admin"), "configuration.in.cfg")
        with open(confFile, 'wb') as configfile:
            config.write(configfile)
    except Exception as e:
        log.warn("Unexpected exception in _saveState: %s" % e)


def init(src_dir):
    global env, opts
    env = Environment(tools=['default', 'textfile', 'pymod'])

    _initOptions()

    _initLog()

    log.info("Adding general build information to scons environment")
    opts = SCons.Script.Variables("custom.py")
    opts.AddVariables(
        PathVariable('build_dir', 'Qserv build dir', os.path.join(
                     src_dir, 'build'), PathVariable.PathIsDirCreate),
            ('PYTHONPATH', 'pythonpath', os.getenv("PYTHONPATH")),
            # Default to in-place install
            PathVariable('prefix', 'qserv install dir', src_dir, PathVariable.PathIsDirCreate),
            ('CXX', 'Choose the C++ compiler to use', env['CXX'])
    )

    opts.Update(env)

    if not log.verbose:
        env['CCCOMSTR'] = env['CXXCOMSTR'] = "Compiling static object $TARGET"
        env['LINKCOMSTR'] = "Linking static object $TARGET"
        env['SHCCCOMSTR'] = env['SHCXXCOMSTR'] = "Compiling shared object $TARGET"
        env['SHLINKCOMSTR'] = "Linking shared object $TARGET"
        env['SWIGCOMSTR'] = "Running SWIG wrapper on $SOURCE"
        env['PROTOC_COMSTR'] = "Running protoc on $SOURCE"
        env['ANTLR_COMSTR'] = "Running antlr on $SOURCE"


def initBuild():
    _setEnvWithDependencies()
    _setBuildEnv()
