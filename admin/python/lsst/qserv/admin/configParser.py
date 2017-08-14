# LSST Data Management System
# Copyright 2014 AURA/LSST.
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
Module defining ConfigParser class and related methods.

ConfigParser class is used to parse configuration files in the format
defined by partitioner. For details check code in partitioner/CmdLineUtils.
Most of the code is stolen from partitioner C++ code and adopted for Python.

@author  Andy Salnikov, SLAC
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
from builtins import chr

# -----------------------------
# Imports for other modules --
# -----------------------------

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

# ------------------------
# Exported definitions --
# ------------------------


class ConfigParser(object):
    """
    Instances of ConfigParser class are used to parse configuration from
    a single file.
    """

    escapes = {'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t'}

    def __init__(self, source, keySeparator='.'):
        """
        @param source:   file (or file-like) object which contains data to parse
        @param keySeparator: Separator character for key components
        """
        self._sep = keySeparator
        self._data = source.read()
        self._cur = 0
        self._end = len(self._data)

    def _skipWhitespace(self):
        """
        Skips all whitespace characters in an input
        """
        while self._cur < self._end:
            c = self._data[self._cur]
            if c not in '\t\n\r ':
                break
            self._cur += 1

    def _skipLine(self):
        """
        Skips until after new-line character
        """
        while self._cur < self._end:
            c = self._data[self._cur]
            if c in '\r\n':
                break
            self._cur += 1

    def _join(self, keys):
        """
        Join all key components into a single path, return a string.

        @param keys:   list of key components
        @return   string conststing of componenets joined with separator character
        """
        # remove leading/trailing separators from each component
        kres = [k.strip(self._sep) for k in keys if k]
        return self._sep.join(kres)

    def _parseValue(self):
        """
        get next unquoted value from a file
        """
        val = ""
        while self._cur < self._end:
            c = self._data[self._cur]
            if c in "\t\n\r #,:=()[]{}":
                return val
            elif ord(c) < 0x20:
                raise ValueError("Unquoted values must not "
                                 "contain control characters.")
            val += c
            self._cur += 1

        return val

    def _parseUnicodeEscape(self):
        """
        Extracts value of the unicode value as UTF-8 encoded string
        """
        # Extract 1-4 hexadecimal digits to build a Unicode
        # code-point in the Basic Multilingual Plane.
        val = ""
        end = min(self._end, self._cur + 4)
        while self._cur < end:
            c = self._data[self._cur]
            if c in '0123456789ABCDEFabcdef':
                val += c
            else:
                break
            self._cur += 1

        if not val:
            raise ValueError("Invalid unicode escape in quoted value")

        # code point
        cp = int(val, 16)

        # UTF-8 encode the code-point.
        val = chr(cp).encode('utf_8')

        return val

    def _parseQuotedValue(self, quote):
        """
        Extracts value of a quoted string.
        """
        val = ""
        while True:
            if self._cur >= self._end:
                raise ValueError("Unmatched quote character.")

            c = self._data[self._cur]
            self._cur += 1
            if c == quote:
                break
            elif c == '\\':
                # Handle escape sequence.
                if self._cur >= self._end:
                    raise ValueError("Unmatched quote character.")

                c = self._data[self._cur]
                self._cur += 1
                if c == 'u':
                    c = self._parseUnicodeEscape()
                else:
                    c = self.escapes.get(c, c)

            val += c

        return val

    def parse(self):
        """
        Parse the whole thing, return a list of (key, value) tuples, where key
        is a full otion path name.
        """
        parsed = []
        keys = []
        groups = [(0, '')]    # list of pairs (level, open_brace)
        lvl = 0
        while True:

            self._skipWhitespace()
            if self._cur >= self._end:
                break

            c = self._data[self._cur]
            s = ""
            if c == '#':
                self._cur += 1
                self._skipLine()
                continue
            elif c == ',':
                self._cur += 1
                continue
            elif c in '([{':
                self._cur += 1
                groups.append((lvl, c))
                continue
            elif c in ')]}':
                self._cur += 1
                p = groups[-1]
                del groups[-1]
                if p[1] == '(' and c != ')':
                    raise ValueError("Unmatched (.")
                elif p[1] == '[' and c != ']':
                    raise ValueError("Unmatched [.")
                elif p[1] == '{' and c != '}':
                    raise ValueError("Unmatched {.")
                elif p[1] == '':
                    raise ValueError("Unmatched ), ], or }.")
                while lvl > groups[-1][0]:
                    del keys[-1]
                    lvl -= 1
                continue
            elif c in "\"\'":
                self._cur += 1
                s = self._parseQuotedValue(c)
            else:
                s = self._parseValue()

            self._skipWhitespace()
            c = self._data[self._cur] if self._cur < self._end else ','
            if c in ':=':
                self._cur += 1
                keys.append(s)
                lvl += 1
                continue

            if not keys:
                key = s
                val = None
            else:
                key = self._join(keys)
                val = s

            while lvl > groups[-1][0]:
                del keys[-1]
                lvl -= 1

            parsed.append((key, val))

        if keys or lvl != 0 or len(groups) != 1:
            raise ValueError("Missing value for key, "
                             "or unmatched (, [ or {.")

        return parsed
