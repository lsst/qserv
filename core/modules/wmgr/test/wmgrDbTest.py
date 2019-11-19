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
import logging
import sys
import unittest

# -----------------------------
# Imports for other modules --
# -----------------------------
from flask import Flask, json
from lsst.qserv.wmgr.config import Config
from lsst.qserv.wmgr import dbMgr

# ---------------------
# Local definitions --
# ---------------------

# -------------------------------
#  Unit test class definition --
# -------------------------------

logging.basicConfig(level=logging.WARNING)


class wmgrDbTest(unittest.TestCase):

    def setUp(self):
        """
        Method called to prepare the test fixture.
        """
        app = Flask(__name__)
        app.config.from_envvar('WMGRCONFIG')
        Config.init(app)
        app.register_blueprint(dbMgr.dbService, url_prefix='/dbs')

        self.app = app.test_client()

        self.dbName = "wmgrTest123"

        # delete it just in case
        response = self._getJson(self.app.get('/dbs'))
        if self.dbName in [db['name'] for db in response['results']]:
            self.app.delete('/dbs/' + self.dbName)

    def tearDown(self):
        """
        Method called immediately after the test method has been called and
        the result recorded.
        """
        # delete it in case failure happened and database was not removed by test
        response = self._getJson(self.app.get('/dbs'))
        if self.dbName in [db['name'] for db in response['results']]:
            self.app.delete('/dbs/' + self.dbName)

    def _getJson(self, rv, expectCode=200):
        """ convert response to json """
        self.assertEqual(rv.mimetype, 'application/json')
        self.assertEqual(rv.status_code, expectCode)
        return json.loads(rv.get_data())

    def test00DbCreateDelete(self):
        """ Test for database creation/listing/deletion. """

        # make semi-random database name
        dbName = self.dbName

        # check that database is not there yet
        response = self._getJson(self.app.get('/dbs'))
        dbs = [db['name'] for db in response['results']]
        self.assertNotIn(dbName, dbs)

        # create it
        response = self._getJson(self.app.post('/dbs', data=dict(db=dbName)), 201)
        self.assertEqual(response['result']['name'], dbName)
        self.assertEqual(response['result']['uri'], '/dbs/' + dbName)

        # check that database is there
        response = self._getJson(self.app.get('/dbs'))
        dbs = [db['name'] for db in response['results']]
        self.assertIn(dbName, dbs)

        # Try again, should get 409 code (this will also print errors in the log)
        response = self._getJson(self.app.post('/dbs', data=dict(db=dbName)), 409)
        self.assertEqual(response['exception'], 'DatabaseExists')

        # drop it
        response = self._getJson(self.app.delete('/dbs/' + dbName))
        self.assertEqual(response['result']['name'], dbName)
        self.assertEqual(response['result']['uri'], '/dbs/' + dbName)

        # check that database is not there
        response = self._getJson(self.app.get('/dbs'))
        dbs = [db['name'] for db in response['results']]
        self.assertNotIn(dbName, dbs)

    def test10TableCreateDelete(self):
        """ Test for database creation/listing/deletion. """

        # make semi-random database name
        dbName = self.dbName
        tblName = "ObjectX"
        tablesURI = '/dbs/' + dbName + '/tables'

        # create database first
        response = self._getJson(self.app.post('/dbs', data=dict(db=dbName)), 201)

        # create table first
        options = dict(table=tblName, schemaSource="request",
                       schema="CREATE TABLE " + tblName + " (X INT, Y INT)")
        response = self._getJson(self.app.post(tablesURI, data=options), 201)
        self.assertEqual(response['result']['name'], tblName)
        tblURI = response['result']['uri']
        self.assertEqual(tblURI, tablesURI + '/' + tblName)

        # Get table list
        response = self._getJson(self.app.get(tablesURI))
        tables = [obj['name'] for obj in response['results']]
        self.assertIn(tblName, tables)

        # try again, shoud get 409 (this will also print errors in the log)
        response = self._getJson(self.app.post(tablesURI, data=options), 409)
        self.assertEqual(response['exception'], 'TableExists')

        # drop table
        response = self._getJson(self.app.delete(tblURI))

        # drop database
        response = self._getJson(self.app.delete('/dbs/' + dbName))

    def test20ChunkCreateDelete(self):
        """ Test for database creation/listing/deletion. """

        # make semi-random database name
        dbName = self.dbName
        tblName = "ObjectX"
        tablesURI = '/dbs/' + dbName + '/tables'
        chunksURI = tablesURI + '/' + tblName + '/chunks'

        # create database/table first
        response = self._getJson(self.app.post('/dbs', data=dict(db=dbName)), 201)
        options = dict(table=tblName, schemaSource="request",
                       schema="CREATE TABLE " + tblName + " (X INT, Y INT)")
        response = self._getJson(self.app.post(tablesURI, data=options), 201)

        # create few chunks
        copt = dict(chunkId=10)
        response = self._getJson(self.app.post(chunksURI, data=copt), 201)
        self.assertEqual(response['result']['chunkId'], 10)
        self.assertEqual(response['result']['uri'], chunksURI + '/10')
        self.assertEqual(response['result']['chunkTable'], True)
        self.assertEqual(response['result']['overlapTable'], True)
        copt = dict(chunkId=20, overlapFlag='False')
        response = self._getJson(self.app.post(chunksURI, data=copt), 201)
        self.assertEqual(response['result']['chunkId'], 20)
        self.assertEqual(response['result']['uri'], chunksURI + '/20')
        self.assertEqual(response['result']['chunkTable'], True)
        self.assertEqual(response['result']['overlapTable'], False)
        copt = dict(chunkId=30, overlapFlag=True)
        response = self._getJson(self.app.post(chunksURI, data=copt), 201)
        self.assertEqual(response['result']['chunkId'], 30)
        self.assertEqual(response['result']['uri'], chunksURI + '/30')
        self.assertEqual(response['result']['chunkTable'], True)
        self.assertEqual(response['result']['overlapTable'], True)

        # drop table, without options this should delete main table but leave all chunks
        response = self._getJson(self.app.delete(tablesURI + '/' + tblName))

        # Get table list
        response = self._getJson(self.app.get(tablesURI))
        tables = set(obj['name'] for obj in response['results'])
        expected = set(
            ['ObjectX_10', 'ObjectX_20', 'ObjectX_30', 'ObjectXFullOverlap_10', 'ObjectXFullOverlap_30'])
        self.assertEqual(tables, expected)

        # re-create table
        response = self._getJson(self.app.post(tablesURI, data=options), 201)

        # now drop with dropChunks flag and re-test, must be empty
        response = self._getJson(self.app.delete(tablesURI + '/' + tblName + "?dropChunks=yes"))
        response = self._getJson(self.app.get(tablesURI))
        tables = [obj['name'] for obj in response['results']]
        self.assertEqual(tables, [])

        # drop database
        response = self._getJson(self.app.delete('/dbs/' + dbName))


#
#  run unit tests when imported as a main module
#
if __name__ == "__main__":
    unittest.main()
