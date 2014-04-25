"""swig_scanner

Special tool which fixes standard swig scanner behavior.

Standard scons swig tool defines scanner which does not work too well
when one includes C++ headers using %include syntax. Scanner tries to 
scan all includes recursively but it's no suitable for scanning pure 
C++ headers as it does not recognize #include syntax. 

For some details:
http://scons.tigris.org/issues/show_bug.cgi?id=2798
https://dev.lsstcorp.org/trac/ticket/1800
https://jira.lsstcorp.org/browse/DM-546

This tool replaces standard swig scanner with a special scanner which 
is a combination of standard swig and C++ scanners, decision which one 
to use is based on file extension.
"""

import re

import SCons.Scanner
from SCons.Tool import swig

class SwigScanner(SCons.Scanner.ClassicCPP):
    '''
    Special scanner which is a combination of SWIG and C++ scanners 
    '''

    # these are regexps used by default scons SWIG and C++ scanners 
    swig_expr = re.compile('^[ \t]*%[ \t]*(?:include|import|extern)[ \t]*(<|"?)([^>\s"]+)(?:>|"?)', re.M)
    cpp_expr = re.compile('^[ \t]*#[ \t]*(?:include|import)[ \t]*(<|")([^>"]+)(>|")', re.M)
    
    def __init__(self):
        SCons.Scanner.ClassicCPP.__init__(self, "SWIG_CPP_Scan", ".i", "SWIGPATH", "")

    def find_include_names(self, node):
        # use swig regexp on .i files, assume all other files are C++
        if str(node).endswith('.i'):
            cre = self.swig_expr
        else:
            cre = self.cpp_expr
        return cre.findall(node.get_text_contents())

# use one global scanner instance
_scanner = SwigScanner()

def generate(env):
    # make sure that swig tool is there and then fix scanners

    # method to replace native swig scanner with our own
    def _repl(scanner):
        if scanner.name == 'SWIGScan':
            return _scanner
        return scanner

    if 'SWIG' not in env:
        # Try to force-generate SWIG tool just in case it is missing yet.
        # This should not be needed because swig is in default tools
        # and is always loaded first but there may be some corner cases.
        swig.generate(env)

    # replace SWIG scanners    
    scanners = [_repl(scanner) for scanner in env['SCANNERS']]
    env.Replace(SCANNERS = scanners)

def exists(env):
    # just forward to actual swig tool
    return swig.exists(env)
