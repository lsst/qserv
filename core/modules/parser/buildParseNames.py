#!/usr/bin/python
#
# LSST Data Management System
# Copyright 2012 LSST Corporation.
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
# Internal build-time generator of parser symbolic constants for use
# in C++ code from ANTLR symbol definitions.
#
# Usage:
# buildParseNames.py <infile.txt> <out.h>
#
# Note, this tool is no longer needed for qserv, but may be useful in
#  the future if ANTLR parse symbols are needed in other languages in
#  qserv.

import re
import sys


def openReadAndWrite(read, write):
    fpRead = open(read)
    fpWrite = open(write, "w")
    return (fpRead, fpWrite)


quotedStrMatch = '"([^"]+)"'
defRegex = re.compile('^(\w+)\(' + quotedStrMatch + '\)\s*=\s*(\d+)')


def parseDef(line):
    """Parses a line formatted as:
    SELECT_LIST("select_list")=1001

    @return ("SELECT_LIST", "select_list", 1001)
    where: "SELECT_LIST" is the symbolic name,
    "select_list" is the token description, and
    1001 is the (integer) identification.
    """
    match = defRegex.match(line)
    if not match:
        return  # return empty if no match
    return (match.group(1), match.group(2), int(match.group(3)))


def tokenSource(defs):
    magicBegin = "// IMPORT"
    magicEnd = "// END_IMPORT"
    isReading = False
    for line in defs:
        if line.startswith(magicBegin):
            isReading = True
        elif isReading and line.startswith(magicEnd):
            isReading = False
        elif isReading:
            yield parseDef(line)


def formatIntLine(defn):
    return 'const int ANTLR_%s = %i;' % (defn[0], defn[2])


def formatStrLine(defn):
    return 'const char ANTLR_%s_STR[] = "%s";' % (defn[0], defn[1])


def mangleTargetName(filename):
    # Convert . to _, then filter out everything non alphanumeric/underscore
    return re.sub("[^A-Za-z_]", "", re.sub("[./]", "_", filename)).upper()


headerPreamble = """ // Auto-generated header. DO NOT EDIT.
// Generated from %(src)s
#ifndef %(mangledtarget)s
#define %(mangledtarget)s
"""
headerFooter = "\n#endif // %(mangledtarget)s not defined.\n"


def main():
    assert len(sys.argv) == 3
    (fpTokens, fpHeader) = openReadAndWrite(sys.argv[1], sys.argv[2])
    src = tokenSource(fpTokens)
    subs = {"mangledtarget": mangleTargetName(sys.argv[2]),
            "src": sys.argv[1]}
    srcList = list(src)
    fpHeader.write(headerPreamble % subs)
    fpHeader.write("\n".join([formatIntLine(item) for item in srcList]))
    fpHeader.write("\n".join([formatStrLine(item) for item in srcList]))
    fpHeader.write(headerFooter % subs)
    fpTokens.close()
    fpHeader.close()


if __name__ == "__main__":
    main()
