#!/usr/bin/env python

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
This is a unittest for checking CSS version functionality in CssAccess.

@author  Andy Salnikov, SLAC

"""

import unittest

from lsst.qserv import css


def _makeCss(data=None):
    """
    Create CssAccess instance with some pre-defined data.
    """

    # make an instance
    data = data or ""
    instance = css.CssAccess.createFromData(data, "")

    return instance


class TestCssVersion(unittest.TestCase):

    def setUp(self):
        self.dbStriping = css.StripingParams(10, 10, 0, 0.0)

    def test1(self):
        """
        Check that if version is missing then it is created on
        first database operation.
        """

        # instantiate CSS with empty initial data
        css_inst = _makeCss()

        # create database, this should also create version key
        css_inst.createDb('TESTDB', self.dbStriping, "L2", "RELEASED")

        # look at the version key
        kvi = css_inst.getKvI()
        self.assertTrue(kvi.exists(css.VERSION_KEY))
        self.assertEqual(kvi.get(css.VERSION_KEY), str(css.VERSION))

    def test2(self):
        """
        Check for existing correct version number.
        """

        # instantiate CSS with some initial data
        initData = """\
/DBS\t\\N
/css_meta\t\\N
/css_meta/version\t""" + str(css.VERSION)
        css_inst = _makeCss(initData)

        # create database, this checks version number
        css_inst.createDb('TESTDB', self.dbStriping, "L2", "RELEASED")

    def test3(self):
        """
        Check for existing correct version number, use dropDb.
        """

        # instantiate CSS with some initial data
        initData = """\
/DBS\t\\N
/DBS/TESTDB\tREADY
/DBS/TESTDB/.packed.json\t\\N
/css_meta\t\\N
/css_meta/version\t""" + str(css.VERSION)
        css_inst = _makeCss(initData)

        # drop database, this checks version number
        css_inst.dropDb('TESTDB')

    def test4(self):
        """
        Check for existing but mis-matching version number.
        """

        # instantiate CSS with some initial data
        initData = """\
/DBS\t\\N
/css_meta\t\\N
/css_meta/version\t1000000"""

        # create CSS instance, this checks version number
        with self.assertRaises(css.VersionMismatchError):
            css_inst = _makeCss(initData)

#


if __name__ == "__main__":
    unittest.main()
