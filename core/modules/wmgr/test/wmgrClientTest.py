#!/bin/env python
#
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
#

"""Test application for wmgr database management commands.

This is a unit test but it requires few other things to be present:
- mysql server up and running
- application config file, passed via WMGRCONFIG envvar

@author Andy Salnikov - salnikov@slac.stanford.edu
"""

# ------------------------------
#  Module's version from CVS --
# ------------------------------
__version__ = "$Revision: 8 $"
# $Source$

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import io
import logging
import sys
import unittest

# -----------------------------
# Imports for other modules --
# -----------------------------
from lsst.qserv.wmgr import client

# ---------------------
# Local definitions --
# ---------------------


def _readAll(input):
    """
    Rad all data from an input file.
    """
    body = ''
    while True:
        chunk = input.read()
        if chunk:
            body += chunk
        else:
            break
    return body

# -------------------------------
#  Unit test class definition --
# -------------------------------


logging.basicConfig(level=logging.WARNING)


class wmgrClientTest(unittest.TestCase):

    def test00MPEncoder(self):
        """ Test for _MPEncoder class, single part. """

        data = [dict(name='name1', data='data1')]
        encoder = client._MPEncoder(data, boundary='1234567890')

        body = _readAll(encoder)

        lines = body.split('\r\n')

        expected = ['--1234567890',
                    'Content-Disposition: form-data; name="name1"',
                    'Content-Transfer-Encoding: binary',
                    '',
                    'data1',
                    '--1234567890--',
                    ''
                    ]
        expectedLen = sum(map(len, expected)) + 2 * (len(expected) - 1)
        self.assertEqual(len(encoder), expectedLen)
        self.assertEqual(lines, expected)

    def test01MPEncoder(self):
        """ Test for _MPEncoder class, two parts. """

        data = [dict(name='name1', data='data1', content_type='text/plain'),
                dict(name='name2', data='data2=1&data22=test',
                     content_type='application/x-www-form-urlencoded')]
        encoder = client._MPEncoder(data, boundary='1234567890')

        body = _readAll(encoder)

        lines = body.split('\r\n')

        expected = ['--1234567890',
                    'Content-Disposition: form-data; name="name1"',
                    'Content-Type: text/plain',
                    'Content-Transfer-Encoding: binary',
                    '',
                    'data1',
                    '--1234567890',
                    'Content-Disposition: form-data; name="name2"',
                    'Content-Type: application/x-www-form-urlencoded',
                    'Content-Transfer-Encoding: binary',
                    '',
                    'data2=1&data22=test',
                    '--1234567890--',
                    ''
                    ]
        expectedLen = sum(map(len, expected)) + 2 * (len(expected) - 1)
        self.assertEqual(lines, expected)
        self.assertEqual(len(encoder), expectedLen)

    def test02MPEncoder(self):
        """ Test for _MPEncoder class, reading data from file."""

        fobj = io.BytesIO('line,1,\\N\nline,2,\0x0\nline,3,\0x1')

        data = [dict(name='load-options', data='data2=1&data22=test',
                     content_type='application/x-www-form-urlencoded'),
                dict(name='table-data', data=fobj, filename='tabledata.dat',
                     content_type='binary/octet-stream')]
        encoder = client._MPEncoder(data)

        body = _readAll(encoder)

        lines = body.split('\r\n')

        expected = ['--part-X-of-the-multipart-request',
                    'Content-Disposition: form-data; name="load-options"',
                    'Content-Type: application/x-www-form-urlencoded',
                    'Content-Transfer-Encoding: binary',
                    '',
                    'data2=1&data22=test',
                    '--part-X-of-the-multipart-request',
                    'Content-Disposition: form-data; name="table-data"; filename="tabledata.dat"',
                    'Content-Type: binary/octet-stream',
                    'Content-Transfer-Encoding: binary',
                    '',
                    'line,1,\\N\nline,2,\0x0\nline,3,\0x1',
                    '--part-X-of-the-multipart-request--',
                    ''
                    ]
        expectedLen = sum(map(len, expected)) + 2 * (len(expected) - 1)
        self.assertEqual(lines, expected)
        self.assertEqual(len(encoder), expectedLen)


#
#  run unit tests when imported as a main module
#
if __name__ == "__main__":
    unittest.main()
