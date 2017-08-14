#!/usr/bin/python
# LSST Data Management System
# Copyright 2014 LSST Corporation.
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
#
# convertVocab.py converts ANTLRv2 token vocabularies to c++ header files
from __future__ import print_function
from itertools import chain
from optparse import OptionParser
import hashlib
import os
import subprocess
import sys

from string import digits
try:
    from string import ascii_letters
except ImportError:
    from string import letters as ascii_letters

sample = """// $ANTLR 2.7.2
SqlSQL2Imp    // input token vocab name
SQL2NRW_ada="ada"=5
SQL2NRW_c="c"=6
SQL2NRW_catalog_name="catalog_name"=7
SQL2NRW_character_set_catalog="character_set_catalog"=8
SQL2NRW_character_set_name="character_set_name"=9
SQL2NRW_character_set_schema="character_set_schema"=10
SQL2NRW_class_origin="class_origin"=11
SQL2NRW_cobol="cobol"=12
SQL2NRW_collation_catalog="collation_catalog"=13
SQL2RW_drop="drop"=126
SQL2RW_immediate="immediate"=156
SQL2RW_in="in"=157
SQL2RW_indicator="indicator"=158
SQL2RW_view="view"=273
SQL2RW_when="when"=274
SQL2RW_whenever="whenever"=275
SQL2RW_where="where"=276
SQL2RW_with="with"=277
SQL2RW_work="work"=278
SQL2RW_write="write"=279
SQL2RW_year="year"=280
SQL2RW_zone="zone"=281
UNSIGNED_INTEGER("an integer number")=282 // Imaginary token based on subtoken typecasting - see the rule <EXACT_NUM_LIT>
APPROXIMATE_NUM_LIT("a number")=283 // Imaginary token based on subtoken typecasting - see the rule <EXACT_NUM_LIT>
QUOTE("'")=284 // Imaginary token based on subtoken typecasting - see the rule <CHAR_STRING>
PERIOD(".")=285 // Imaginary token based on subtoken typecasting - see the rule <EXACT_NUM_LIT>
MINUS_SIGN("-")=286 // Imaginary token based on subtoken typecasting - see the rule <SEP
NOT_EQUALS_OP("<>")=289 // Imaginary token based on subtoken typecasting - see the rule <LESS_THAN_OP>
LESS_THAN_OR_EQUALS_OP("<=")=290 // Imaginary token based on subtoken typecasting - see the rule <LESS_THAN_OP>
GREATER_THAN_OR_EQUALS_OP(">=")=291 // Imaginary token based on subtoken typecasting - see the rule <GREATER_THAN_OP>
CONCATENATION_OP("||")=292 // Imaginary token based on subtoken typecasting
"""
# {0} header filename
# {1} token ident
sampleCC = """
#include "{0}"

int main(int argc, char** argv) {{
    int catalogtoken = {1}::SQL2NRW_catalog_name;
    char const* quote = {1}::toString({1}::QUOTE);
    return 0;
}}
"""


def splitEq(line):
    # Consider writing with a regex.
    clean = line.strip()
    # remove comment
    commentLoc = clean.find("//")
    if commentLoc != -1:
        clean = clean[:commentLoc]
    # split by equals sign
    lefteq = clean.find("=")
    righteq = clean.rfind("=")
    if "(" in clean[:lefteq]:  # If there is a paren ( FOO("foo")=3423 format)
        # Make sure the closing paren is also in there.
        if ")" not in clean[:lefteq]:
            # if not, advance to closing paren and look for equals from there.
            lefteq = clean.find("=", clean.find(")"))
    if lefteq == -1:
        return ()
    if lefteq == righteq:
        return (clean[:lefteq], clean[lefteq+1:])
    else:
        return (clean[:lefteq], clean[lefteq+1:righteq], clean[righteq+1:])


def stripQuotes(s):
    if s[0] == s[-1] and s[0] in ['"', "'"]:
        return s[1:-1]
    return s


class NoValueError(ValueError):

    def __init__(self, descr):
        super(NoValueError, self).__init__(descr)
        pass


# Mimic ANTLR 2.7 generated header format.
# {0} sanitized filename
# {1} source filename
# {2} this file ( __file__ )
# {3} struct name
# {4} 8-space indented definitions
# {5} 12-space indented descriptions
# (braces "{}" are doubled to escape them in Python's string formatter)
cppTemplate = """#ifndef INC_{0}
#define INC_{0}
/* Computed from {1} using {2} */
struct {3} {{
    enum {{
{4}
    }};
    static char const* toString(int n) {{
        switch(n) {{
{5}
        default: return 0;
    }}
}}
}};

#endif /* INC_{0} */
"""
enumTemplate = "{0} = {1}"
caseTemplate = "case {0}: return \"{1}\";"


class Vocabulary:
    """Vocabulary is an object that bundles a ANTLRv2 token vocabulary.
It consists of an identifier, some textual description or representation,
and an integer tokenid.
See: http://www.antlr2.org/doc/vocab.html
"""

    def __init__(self):
        self.tokens = []
        self.legalIdent = set(chain(ascii_letters, digits, ["_"]))
        self.legalFirst = set(chain(ascii_letters, ["_"]))
        self.sourceFile = "undefined"

    def importBuffer(self, text):
        """Import a text buffer into the vocabulary"""
        for line in text.split("\n"):
            if not line:
                continue
            try:
                self.tokens.append(Token(line))
            except NoValueError as e:
                # print e.message
                print("ignoring", line)
                pass
        pass

    def importFile(self, filename):
        """Import the contents of a file into the vocabulary and remember the
source file name"""
        self.sourceFile = self.sanitizeIdent(os.path.basename(filename))
        self.importBuffer(open(filename).read())

    def exportCppHeader(self, targetFile=None):
        """Export the current vocabulary as a C++ header file"""
        # {0} sanitized filename
        # {1} source filename
        # {2} this file ( __file__ )
        # {3} struct name
        # {4} 8-space indented definitions
        # {5} 8-space indented descriptions
        filename = "tokens_h"
        sourceFilename = self.sourceFile
        structName = "tokens"
        if targetFile:
            basename = os.path.basename(targetFile)
            filename = self.sanitizeIdent(basename)
            structName = self.sanitizeIdent(os.path.splitext(basename)[0])
        enums = ",\n".join(s.toEnum() for s in self.tokens)
        cases = "\n".join(s.toCase() for s in self.tokens)
        return cppTemplate.format(filename, sourceFilename, __file__,
                                  structName, enums, cases)

    def sanitizeIdent(self, raw):
        """Sanitize an identifier by converting illegal characters to underscores
and prefixing with 'x' if the first character is still not legal."""
        def makeLegal(ch):
            if ch not in self.legalIdent:
                return "_"
            else:
                return ch
        sanitized = [makeLegal(l) for l in raw]
        if sanitized[0] not in self.legalFirst:
            sanitized = ["x"] + sanitized
        return "".join(sanitized)

    pass


class Token:
    """A Token object represents an entry in an ANTLR vocabulary"""

    def __init__(self, line):
        t = splitEq(line)
        if not t:
            raise NoValueError("No token definition in: [%s]" % line.strip())
        ident = t[0]
        tokenid = t[-1]
        descr = None
        if len(t) == 2:  # like: MINUS_SIGN("-")=286 // Imaginary token based on
            # look for parens
            lparen = t[0].find("(")
            rparen = t[0].rfind(")")
            if lparen == -1:
                raise RuntimeError('Expected Foo("foo")=1234, got' + line)

            descr = stripQuotes(t[0][lparen+1:rparen])
            ident = t[0][:lparen]
            pass
        elif len(t) == 3:  # like: SQL2RW_zone="zone"=281
            # strip quotes from descr
            descr = stripQuotes(t[1])
        else:
            pass
        self.ident = ident
        self.descr = descr
        self.tokenid = int(tokenid)
        # print "ident",ident,"descr",descr,"tokenid",tokenid
        pass

    def toEnum(self, indent="        "):
        return indent + enumTemplate.format(self.ident, self.tokenid)

    def toCase(self, indent="            "):
        return indent + caseTemplate.format(self.ident, self.descr)


def debugTest():
    v = Vocabulary()
    v.importBuffer(sample)
    print(v.exportCppHeader("SomeTokens.h"))


class UnitTest:
    """Unit test this module by exercising the conversion over sample token data
    and ensuring that the result can be compiled in g++"""

    def __init__(self):
        # Make a probably-unique digest from: userid + __file__ + pid
        m = hashlib.md5()
        m.update(str(os.getuid()))
        m.update(__file__)
        # m.update(str(os.getpid()))
        digest = m.hexdigest()
        v = Vocabulary()
        self.digest = v.sanitizeIdent(digest)
        # Decide on some pathnames
        self.inputTokenFile = os.path.join("/tmp", self.digest + ".txt")
        self.outputHeaderFile = os.path.join("/tmp", self.digest + "gen.h")
        self.ccFile = os.path.join("/tmp", self.digest + "test.cc")
        self.progFile = os.path.splitext(self.ccFile)[0]
        self.testFiles = [self.inputTokenFile, self.outputHeaderFile,
                          self.ccFile, self.progFile]

    def writeFiles(self):
        # Write the sample files
        open(self.inputTokenFile, "w").write(sample)
        m = Main()
        m.convertFile(self.inputTokenFile, self.outputHeaderFile)

        # Compute a test .cc file and write it
        ccTest = sampleCC.format(self.outputHeaderFile, self.digest+"gen_h")

        open(self.ccFile, "w").write(sampleCC.format(self.outputHeaderFile,
                                                     self.digest+"gen"))

    def compile(self):
        # Try compiling
        progFile = os.path.splitext(self.ccFile)[0]

        try:
            retcode = subprocess.call(" ".join(["g++", "-o",
                                                self.progFile, self.ccFile]),
                                      shell=True)
            if retcode < 0:
                print("Test failed: terminated by signal", -retcode, file=sys.stderr)
            else:
                if retcode == 0:
                    print("Test success", file=sys.stderr)
                    # Cleanup
                    for f in self.testFiles:
                        os.remove(f)
                else:
                    print("Compilation failure: g++ returned", retcode, file=sys.stderr)
                    print("Test files:", " ".join(self.testFiles))

        except OSError as e:
            print("Execution failed:", e, file=sys.stderr)
        pass

    def run(self):
        self.writeFiles()
        self.compile()


class Main:

    def __init__(self):
        usage = "usage: %prog [options] <tokens.txt> <output.h>"
        op = OptionParser(usage=usage)
        op.add_option("-t", "--test", action="store_true", dest="testOnly",
                      default=False, help="run debug test (default: don't test)")
        op.add_option("-u", "--unittest", action="store_true", dest="unitTest",
                      default=False, help="run unit test (generate and compile)")

        self.oParser = op
        pass

    def convertFile(self, tokenFile, headerFile):
        v = Vocabulary()
        v.importFile(tokenFile)
        open(headerFile, "w").write(v.exportCppHeader(headerFile))

    def run(self):
        (options, args) = self.oParser.parse_args()
        if options.testOnly:
            debugTest()
            return
        elif options.unitTest:
            u = UnitTest()
            u.run()
            return
        else:
            if len(args) != 2:
                self.oParser.print_help()
                return
            self.convertFile(args[0], args[1])


if __name__ == "__main__":
    # debugTest()
    m = Main()
    m.run()
