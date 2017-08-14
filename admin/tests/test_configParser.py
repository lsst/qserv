#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2014 AURA/LSST.
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
This is a unittest for configParser

@author  Andy Salnikov, SLAC

"""

import io
import unittest

from lsst.qserv.admin.configParser import ConfigParser


class TestConfigParser(unittest.TestCase):

    def test1(self):

        data = "a, b: c\n[d,e]"

        parser = ConfigParser(io.StringIO(data))
        options = parser.parse()

        self.assertEqual(len(options), 4)
        self.assertEqual(options, [('a', None), ('b', 'c'), ('d', None), ('e', None)])

    def test2(self):

        data = "a = { b = [c, d, e] }"

        parser = ConfigParser(io.StringIO(data))
        options = parser.parse()

        self.assertEqual(len(options), 3)
        self.assertEqual(options, [('a.b', 'c'), ('a.b', 'd'), ('a.b', 'e')])

    def test3(self):

        data = r'''a = { b = "a b 'c", c: '\t\n\b\r\f\u1AX' }'''

        parser = ConfigParser(io.StringIO(data))
        options = parser.parse()

        self.assertEqual(len(options), 2)
        self.assertEqual(options, [('a.b', 'a b \'c'), ('a.c', '\t\n\b\r\f\x1AX')])

    def test3(self):

        data = r''' "a b c" : 'a b c' '''

        parser = ConfigParser(io.StringIO(data))
        options = parser.parse()

        self.assertEqual(len(options), 1)
        self.assertEqual(options, [('a b c', 'a b c')])

    def testComments(self):

        data = '''#comment\na = b  # comment\n\n  #comment\n'''

        parser = ConfigParser(io.StringIO(data))
        options = parser.parse()

        self.assertEqual(len(options), 1)
        self.assertEqual(options, [('a', 'b')])

    def testExcept1(self):

        data = "a = { b "

        parser = ConfigParser(io.StringIO(data))
        self.assertRaises(ValueError, parser.parse)

    def testExcept2(self):

        data = "a = 'b"

        parser = ConfigParser(io.StringIO(data))
        self.assertRaises(ValueError, parser.parse)

    def testExcept3(self):

        data = """a = 'b" """

        parser = ConfigParser(io.StringIO(data))
        self.assertRaises(ValueError, parser.parse)

    def testExcept4(self):

        data = """ a = x\x13y """

        parser = ConfigParser(io.StringIO(data))
        self.assertRaises(ValueError, parser.parse)


#

if __name__ == "__main__":
    unittest.main()
