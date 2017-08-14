"""
Google's Protoc builder

Example, will produce c++ output files in the src directory.

env = Environment(
  tools = ['default', 'protoc'],
  #...
  )

protoc_cc = env.Protoc(["src/Example.proto"],
    PROTOC_PATH='#src',
    PROTOC_CCOUT='#src',
    )
Copied from: https://raw.github.com/petriborg/Scons-Protoc/master/protoc.py
"""

import os
import SCons.Util
from SCons.Script import Builder, Action, Dir, File, Entry


def _detect(env):
    """Try to find the Protoc compiler"""
    try:
        return env['PROTOC']
    except KeyError:
        raise SCons.Errors.StopError(
            "Could not detect protoc compiler")


def _protoc_emitter(target, source, env):
    """Process target, sources, and flags"""

    # always ignore target
    target = []

    # suffix
    protoc_suffix = env.subst('$PROTOC_SUFFIX')
    protoc_h_suffix = env.subst('$PROTOC_HSUFFIX')
    protoc_cc_suffix = env.subst('$PROTOC_CCSUFFIX')
    protoc_py_suffix = env.subst('$PROTOC_PYSUFFIX')
    protoc_java_suffix = env.subst('$PROTOC_JAVASUFFIX')

    # fetch all protoc flags
    if env['PROTOC_FLAGS']:
        protocflags = env.subst("$PROTOC_FLAGS",
                                target=target, source=source)
        flags = SCons.Util.CLVar(protocflags)
    else:
        flags = SCons.Util.CLVar('')

    # flag --proto_path, -I
    proto_path = []
    if env['PROTOC_PATH']:

        inc = env['PROTOC_PATH']
        if SCons.Util.is_Scalar(inc):
            inc = [inc]
        inc = [Dir(path) for path in inc]
        # by default Dir() makes node which resides in build directory (variant_dir)
        # and we want it to be in source directory if duplication is disabled
        inc = [path if path.duplicate else path.srcnode() for path in inc]
        env['PROTOC_PATH'] = inc

        # print "env['PROTOC_PATH']:", map(str, env['PROTOC_PATH'])
        for i, path in enumerate(env['PROTOC_PATH']):
            # print "path:", path
            proto_path.append(path)
            flags.append('--proto_path=${PROTOC_PATH[%d]}' % i)

    # flag --cpp_out
    if env['PROTOC_CCOUT']:
        env['PROTOC_CCOUT'] = Dir(env['PROTOC_CCOUT'])
        flags.append('--cpp_out=${PROTOC_CCOUT}')

    # flag --python_out
    if env['PROTOC_PYOUT']:
        env['PROTOC_PYOUT'] = Dir(env['PROTOC_PYOUT'])
        flags.append('--python_out=${PROTOC_PYOUT}')

    # flag --java_out
    if env['PROTOC_JAVAOUT']:
        env['PROTOC_JAVAOUT'] = Dir(env['PROTOC_JAVAOUT'])
        flags.append('--java_out=${PROTOC_JAVAOUT}')

    # updated flags
    env['PROTOC_FLAGS'] = str(flags)
    # print "flags:", flags

    # source scons dirs
    src_struct = Dir('#')
    src_script = Dir('.').srcnode()

    # produce proper targets
    for src in source:
        src = File(src)
        if not src.duplicate:
            # file is in source directory when duplication is disabled
            src = File(src).srcnode()
        # print "src.abspath:", src.abspath

        # find proto_path for this source
        longest_common = '/'
        for path in proto_path:
            # print "path.abspath:", path.abspath
            common = os.path.commonprefix([path.abspath, src.abspath])
            if len(common) > len(longest_common):
                longest_common = common
        # print "longest_common:", longest_common

        src_relpath = os.path.relpath(src.abspath, start=longest_common)
        # print "src_relpath:", src_relpath

        # create stem by remove the $PROTOC_SUFFIX or take a guess
        if src_relpath.endswith(protoc_suffix):
            stem = src_relpath[:-len(protoc_suffix)]
        else:
            stem = src_relpath
        # print "stem:", stem

        # C++ output, append
        if env['PROTOC_CCOUT']:
            out = Dir(env['PROTOC_CCOUT'])
            target.append(out.File(stem + protoc_cc_suffix))
            target.append(out.File(stem + protoc_h_suffix))

        # python output, append
        if env['PROTOC_PYOUT']:
            out = Dir(env['PROTOC_PYOUT'])
            target.append(out.File(stem + protoc_py_suffix))

        # java output, append
        if env['PROTOC_JAVAOUT']:
            out = Dir(env['PROTOC_JAVAOUT'])
            target.append(out.File(stem + protoc_java_suffix))

    # print "protoc targets:", env.subst("${TARGETS}", target=target, source=source)
    # print "protoc sources:", env.subst("${SOURCES}", target=target, source=source)
    # print "protoc command:", env.subst("${PROTOC_COM}", target=target, source=source)

    return target, source


_protoc_builder = Builder(
    action=Action('$PROTOC_COM', '$PROTOC_COMSTR'),
    suffix='$PROTOC_CCSUFFIX',
    src_suffix='$PROTOC_SUFFIX',
    emitter=_protoc_emitter,
)


def generate(env):
    """Add Builders and construction variables."""

    _detect(env)

    env.SetDefault(

        # Additional command-line flags
        PROTOC_FLAGS=SCons.Util.CLVar(''),

        # Source path(s)
        PROTOC_PATH=SCons.Util.CLVar(''),

        # Output path
        PROTOC_CCOUT='',
        PROTOC_PYOUT='',
        PROTOC_JAVAOUT='',

        # Suffixies / prefixes
        PROTOC_SUFFIX='.proto',
        PROTOC_HSUFFIX='.pb.h',
        PROTOC_CCSUFFIX='.pb.cc',
        PROTOC_PYSUFFIX='_pb2.py',
        PROTOC_JAVASUFFIX='.java',

        # Protoc command
        PROTOC_COM="$PROTOC $PROTOC_FLAGS $SOURCES",
        PROTOC_COMSTR='',

    )

    env['BUILDERS']['Protoc'] = _protoc_builder


def exists(env):
    _detect(env)
    return True
