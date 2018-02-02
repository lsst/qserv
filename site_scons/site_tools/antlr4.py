import os
import SCons.Util
from SCons.Script import Builder, Action, Dir, File, Entry


def _detect(env):
    """Try to find the Antlr parser generator"""
    try:
        return env['ANTLR4']
    except KeyError:
        raise SCons.Errors.StopError(
            'Could not find antlr4 parser generator')


def _antlr4_emitter(target, source, env):
    """Process sources and flags
    
    This implementation depends on having separate Lexer and Parser files, and each file name must contain
    the word 'Lexer' or 'Parser' (respectively). (It's possible this implementation would work with only a
    '...Parser.g4' file that contained the Lexer data as well, but this is untested).
    Typically the Antlr4 scons tool woudl be called twice, first with the Lexer.g4 file and then with the
    generated tokens file and the Parser.g4 file, for example this code, taken from `core/modules/parser`.
        antlr4LexerTgt = env.Antlr4(['MySqlLexer.g4'], ANTLR4_OUT='.')
	    # The exuction of the antlr4 tool on MySqlLexer.g4 (on the line above) generates
	    # `MySqlLexer.tokens` (used on the line below).
	    antlr4ParserTgt = env.Antlr4(['MySqlLexer.tokens', 'MySqlParser.g4'], ANTLR4_OUT='.')
    """
    target = []
    antlr4_suffix = env.subst('$ANTLR4_SUFFIX')
    antlr4_h_suffix = env.subst('$ANTLR4_HSUFFIX')
    antlr4_cc_suffix = env.subst('$ANTLR4_CCSUFFIX')
    antlr4_token_suffix = env.subst('$ANTLR4_TOKENSUFFIX')
    if env['ANTLR4_FLAGS']:
        antlr4flags = env.subst('$ANTLR4_FLAGS', target=target, source=source)
        flags = SCons.Util.CLVar(antlr4flags)
    else:
        flags = SCons.Util.CLVar('')

    # -o flag
    if env['ANTLR4_OUT']:
        env['ANTLR4_OUT'] = Dir(env['ANTLR4_OUT'])
        flags.append('-o ${ANTLR4_OUT}')

    # -lib flag
    if env['ANTLR4_GLIB']:
        env['ANTLR4_GLIB'] = Dir(env['ANTLR4_GLIB'])
        flags.append('-lib ${ANTLR4_GLIB}')

    # update antlr4 flags
    env['ANTLR4_FLAGS'] = str(flags)

    # compute targets
    deps = []
    grammars = [] 

    lexerFiles = (('', (antlr4_h_suffix, antlr4_cc_suffix, antlr4_token_suffix)),)
    parserFiles = (('', (antlr4_h_suffix, antlr4_cc_suffix)),
                   ('BaseListener', (antlr4_h_suffix, antlr4_cc_suffix)),
                   ('Listener', (antlr4_h_suffix, antlr4_cc_suffix)))

    for src in source:
        src = File(src)
        stem = src.abspath
        if stem.endswith(antlr4_token_suffix):
            deps.append(src)
            continue
        grammars.append(src)
        if stem.endswith(antlr4_suffix):
            stem = stem[:-len(antlr4_suffix)]
        for kind, suffixes in (lexerFiles if "Lexer" in stem else parserFiles):
	        target += [File(stem + kind + ext) for ext in suffixes]
    for t in target:
        for d in deps:
            env.Depends(t, d)
    source[:] = grammars
    return (target, grammars)


_antlr4_builder = Builder(
    action=Action('$ANTLR4_COM', '$ANTLR4_COMSTR'),
    suffix='$ANTLR4_CCSUFFIX',
    src_suffix='$ANTLR4_SUFFIX',
    emitter=_antlr4_emitter,
)


def generate(env):
    """Add Builders and construction variables."""
    _detect(env)
    env.SetDefault(
        # Additional command-line flags
        ANTLR4_FLAGS=SCons.Util.CLVar('-Dlanguage=Cpp -Xexact-output-dir'),
        # Output path
        ANTLR4_OUT='',
        # Suffixies / prefixes
        ANTLR4_SUFFIX='.g4',
        ANTLR4_HSUFFIX='.h',
        ANTLR4_CCSUFFIX='.cpp',
        ANTLR4_TOKENSUFFIX='.tokens',
        # Antlr command
        ANTLR4_COM="$ANTLR4 $ANTLR4_FLAGS $SOURCES",
        ANTLR4_COMSTR='',
        ANTLR4_GLIB='',
    )
    env['BUILDERS']['Antlr4'] = _antlr4_builder
    # We must add 'ANTLR4_DIR' to the execution environment becuase our eups-packaged antlr4 executable
    # must have access to this variable (see the antlr4 script in our packaged antlr4 at
    # github.com/lsst/antlr4)
    env['ENV']['ANTLR4_DIR'] = os.environ['ANTLR4_DIR']


def exists(env):
    _detect(env)
    return True
