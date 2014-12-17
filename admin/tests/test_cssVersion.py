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
This is a unittest for checking CSS version functionality in qservAdmin module.

@author  Andy Salnikov, SLAC

"""

import os
import tempfile
import unittest

import lsst.qserv.admin.qservAdmin as qservAdmin
from lsst.qserv.admin.qservAdminException import QservAdminException


def _makeAdmin(data=None):
    """
    Create QservAdmin instance with some pre-defined data.
    """
    if data is None:
        # read from /dev/null
        connection = '/dev/null'
    else:
        # make temp file and save data in it
        file = tempfile.NamedTemporaryFile(delete=False)
        connection = file.name
        file.write(data)
        file.close()

    # make an instance
    config = dict(technology='mem', connection=connection)
    admin = qservAdmin.QservAdmin(config=config)

    # remove tmp file
    if connection != '/dev/null':
        os.unlink(connection)

    return admin


class TestCssVersion(unittest.TestCase):

    def test1(self):
        """
        Check that if version is missing then it is created on
        first database operation.
        """

        # instantiate kvI with empty initial data
        admin = _makeAdmin()

        # create database, this should also create version key
        dbOptions = dict(nStripes='10', nSubStripes='10', overlap='0.0', storageClass='L2')
        admin.createDb('TESTDB', dbOptions)

        # look at the version key
        kvi = admin._kvI
        self.assertTrue(kvi.exists(qservAdmin.VERSION_KEY))
        self.assertEqual(kvi.get(qservAdmin.VERSION_KEY), str(qservAdmin.VERSION))

    def test2(self):
        """
        Check for existing correct version number.
        """

        # instantiate kvI with empty initial data
        initData = """\
/DBS\t\\N
/css_meta\t\\N
/css_meta/version\t""" + str(qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        # create database, this checks version number
        dbOptions = dict(nStripes='10', nSubStripes='10', overlap='0.0', storageClass='L2')
        admin.createDb('TESTDB', dbOptions)

    def test3(self):
        """
        Check for existing correct version number, use dropDb.
        """

        # instantiate kvI with empty initial data
        initData = """\
/DBS\t\\N
/DBS/TESTDB\tREADY
/DBS/TESTDB.json\t\\N
/css_meta\t\\N
/css_meta/version\t""" + str(qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        # dor database, this checks version number
        admin.dropDb('TESTDB')

    def test4(self):
        """
        Check for existing but mis-matching version number.
        """

        # instantiate kvI with empty initial data
        initData = """\
/DBS\t\\N
/css_meta\t\\N
/css_meta/version\t1000000"""
        admin = _makeAdmin(initData)

        # create database, this checks version number
        dbOptions = dict(nStripes='10', nSubStripes='10', overlap='0.0', storageClass='L2')
        with self.assertRaises(QservAdminException):
            admin.createDb('TESTDB', dbOptions)

####################################################################################

if __name__ == "__main__":
    unittest.main()
