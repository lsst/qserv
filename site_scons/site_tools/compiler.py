"""compiler tool.

Special tool which defines compiling/linking options.

This tool is supposed to handle all combinations of OS and compiers.
When new toolset is added this tool may need to be extended.

For some details:

https://jira.lsstcorp.org/browse/DM-3662
"""

import os
import re
import subprocess
import sys

import SCons
import state   # this should not be here

def _compiler(env):
    """
    Guess compiler type and version. Returns pair of strings:
    - toolchain name, one of 'gcc', 'clang'
    - compiler + version, e.g. 'gcc44', 'gcc48', 'clang35'
    """

    # borrowed from sconsUtils, list of version strings and their matching compiler
    version_strings = (
        (r"g(?:\+\+|cc) +\(.+\) +([0-9.a-zA-Z]+)", "gcc"),
        (r"LLVM +version +([0-9.a-zA-Z]+) ", "clang"), # clang on Mac
        (r"clang +version +([^ ]+) ", "clang"), # clang on linux
        (r"\(ICC\) +([0-9.a-zA-Z]+) ", "icc"),
    )

    # run $CXX --version
    try:
        output = subprocess.check_output([env['CXX'], '--version'], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        # process ran but returned error code
        raise SCons.Errors.StopError('Error while runnig CXX (%s --version): %s' % (env['CXX'], str(exc)))
    except Exception as exc:
        # process did not run
        raise SCons.Errors.StopError('Failed to execute CXX (%s): %s' % (env['CXX'], str(exc)))

    # try to match output to expected, see that compiler names match
    toolchain = ''
    version = ''
    for re_str, compiler_name in version_strings:
        match = re.search(re_str, output)
        if match:
            toolchain = compiler_name
            version = match.group(1)
            break

    # take two first pieces of the version
    version = re.split('[-.]+', version) + ['']
    version = ''.join(version[:2])

    return toolchain, toolchain + version


def generate(env):

    # current platform
    platform = sys.platform

    # compiler
    toolchain, comp_version = _compiler(env)

    state.log.info("Building on %s with %s" % (platform, toolchain))

    if toolchain == 'clang' and platform == 'darwin':
        # clang on OS x

        # Increase compiler strictness
        env.Append(CCFLAGS=['-pedantic', '-Wall', '-Wno-long-long', '-Wno-variadic-macros'])
        env.Append(CXXFLAGS=['-std=c++11'])

        # copied from sconsUtils
        env.Append(SHLINKFLAGS=["-undefined", "suppress", "-flat_namespace", "-headerpad_max_install_names"])

        env['LDMODULESUFFIX'] = ".so"
        if not re.search(r"-install_name", str(env['SHLINKFLAGS'])):
            env.Append(SHLINKFLAGS=["-Wl,-install_name", "-Wl,${TARGET.file}"])

    elif platform == 'linux2':
        # gcc on any platform or
        # clang on non-mac plartforms (assume it's linux)

        # Increase compiler strictness
        env.Append(CCFLAGS=['-pedantic', '-Wall', '-Wno-long-long', '-Wno-variadic-macros'])
        if toolchain == 'gcc':
            env.Append(CCFLAGS=['-Wno-unused-local-typedefs'])
        if comp_version in ['gcc44']:
            # gcc44 only supports c++0x
            env.Append(CXXFLAGS=['-std=c++0x'])
        else:
            # newer compilers are expected to be c++11
            env.Append(CXXFLAGS=['-std=c++11'])

        # to make shared libraries link correctly we need -rpath-link option, for now add everything
        # that is in LD_LIBRARY_PATH
        if 'LD_LIBRARY_PATH' in os.environ:
            env.Append(LINKFLAGS=["-Wl,-rpath-link=" + os.environ["LD_LIBRARY_PATH"]])

        # SCons resets many envvars to make clean build, we want to pass some of them explicitly.
        # Extend the list if you need to add more.
        for key in ['LD_LIBRARY_PATH',]:
            if key in os.environ:
                env['ENV'][key] = os.environ[key]

        env.Append(LINKFLAGS=["-pthread"])

    else:

        raise SCons.Errors.StopError('Unsupported platform or toolchain: %s/%s' % (platform, toolchain))

    # rest works for any compiler/platform
    if env['debug'] == 'yes':
        state.log.info("Debug build flag (-g) requested.")
        env.Append(CCFLAGS=['-g'])

    env.Append(CPPFLAGS=["-D_FILE_OFFSET_BITS=64"])

def exists(env):
    # just forward to actual swig tool
    return True
