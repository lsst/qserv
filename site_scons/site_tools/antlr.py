import os
import os.path
import SCons.Util
from SCons.Script import Builder, Action, Dir, File, Entry


def _detect(env):
    """Try to find the Antlr parser generator"""
    try:
        return env['ANTLR']
    except KeyError:
        raise SCons.Errors.StopError(
            'Could not find antlr parser generator')


def _antlr_emitter(target, source, env):
    """Process sources and flags"""
    target = []
    antlr_suffix = env.subst('$ANTLR_SUFFIX')
    antlr_h_suffix = env.subst('$ANTLR_HSUFFIX')
    antlr_cc_suffix = env.subst('$ANTLR_CCSUFFIX')
    antlr_txt_suffix = env.subst('$ANTLR_TXTSUFFIX')
    if env['ANTLR_FLAGS']:
        antlrflags = env.subst('$ANTLR_FLAGS', target=target, source=source)
        flags = SCons.Util.CLVar(antlrflags)
    else:
        flags = SCons.Util.CLVar('')
    # -o flag
    if env['ANTLR_OUT']:
        env['ANTLR_OUT'] = Dir(env['ANTLR_OUT'])
        flags.append('-o ${ANTLR_OUT}')
    # -glib flag
    if env['ANTLR_GLIB']:
        env['ANTLR_GLIB'] = File(env['ANTLR_GLIB'])
        flags.append('-glib ${ANTLR_GLIB}')
        # TODO: ImpTokenTypes!?

    # update antlr flags
    env['ANTLR_FLAGS'] = str(flags)

    # compute targets
    deps = []
    for src in source:
        src = File(src)
        stem = src.abspath
        if stem.endswith(antlr_suffix):
            stem = stem[:-len(antlr_suffix)]
        deps.append(File(stem + 'ImpTokenTypes' + antlr_txt_suffix))
        if env['ANTLR_OUT']:
            out = Dir(env['ANTLR_OUT'])
            stem = os.path.join(out.abspath, os.path.basename(stem))
        for kind in ('Lexer', 'Parser'):
            for ext in (antlr_h_suffix, antlr_cc_suffix):
                target.append(File(stem + kind + ext))
        for kind in ('', 'Lex'):
            for ext in (antlr_h_suffix, antlr_txt_suffix):
                target.append(File(stem + kind + 'TokenTypes' + ext))
    for t in target:
        for d in deps:
            env.Depends(t, d)
    return (target, source)


_antlr_builder = Builder(
    action=Action('$ANTLR_COM', '$ANTLR_COMSTR'),
    suffix='$ANTLR_CCSUFFIX',
    src_suffix='$ANTLR_SUFFIX',
    emitter=_antlr_emitter,
)


def generate(env):
    """Add Builders and construction variables."""
    _detect(env)
    env.SetDefault(
        # Additional command-line flags
        ANTLR_FLAGS=SCons.Util.CLVar(''),
        # Output path
        ANTLR_OUT='',
        # Suffixies / prefixes
        ANTLR_SUFFIX='.g',
        ANTLR_HSUFFIX='.hpp',
        ANTLR_CCSUFFIX='.cpp',
        ANTLR_TXTSUFFIX='.txt',
        # Antlr command
        ANTLR_COM="$ANTLR $ANTLR_FLAGS $SOURCES",
        ANTLR_COMSTR='',
    )
    env['BUILDERS']['Antlr'] = _antlr_builder


def exists(env):
    _detect(env)
    return True
