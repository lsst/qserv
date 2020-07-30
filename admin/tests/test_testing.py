#!/usr/bin/env python

# LSST Data Management System
# Copyright 2020 SLAC.
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

"""This is a unittest for admin.testing package
"""

import io
import unittest

from lsst.qserv.testing import config, mock_db


_CFG1 = """
queryClasses:
  LV:
    concurrentQueries: 80
    targetTime: 10s
    maxRate: 100
  FTSObj:
    concurrentQueries: 12
    targetTimeSec: 1h
    maxRate: null
queries:
  LV:
    q1:
      template: >
        SELECT ra, decl, raVar, declVar, radeclCov, u_psfFlux, u_psfFluxSigma, u_apFlux
        FROM Object
        WHERE deepSourceId = {objectId}
      variables:
        objectId:
          path: /dev/null
          mode: random
    q2:
      template: >
        SELECT ra, decl, raVar, declVar, radeclCov, u_psfFlux, u_psfFluxSigma, u_apFlux
        FROM Object
        WHERE qserv_areaspec_box({raMin}, {declMin}, {raMin}+{raDist}, {declMin}+{declDist})
      variables:
        raMin: {distribution: uniform, min: 0, max: 350}
        declMin: {distribution: uniform, min: -87, max: 45}
        raDist: {distribution: uniform, min: 0.01, max: 0.2}
        declDist: {distribution: uniform, min: 0.01, max: 0.2}
  FTSObj:
    q1: "SELECT COUNT(*) FROM Object WHERE y_instFlux > 0.05"
    q2: "SELECT ra, decl, u_psfFlux, g_psfFlux, r_psfFlux FROM Object WHERE y_shapeIxx BETWEEN 20 AND 40"
    q3: "SELECT COUNT(*) FROM Object WHERE y_instFlux > u_instFlux"
"""

_CFG2 = """
queryClasses:
  LV:
    concurrentQueries: 100
  FTSObj:
    maxRate: 1
"""


class TestConfig(unittest.TestCase):

    def test_merge(self):

        cfg1 = {
            "a": 1, "b": 2, "c": {"d": 3, "0": 0}
        }
        cfg2 = {
            "a": 3, "e": 4, "c": {"0": [], "x": None}
        }
        expect = {
            "a": 3, "b": 2, "e": 4, "c": {"d": 3, "x": None, "0": []}
        }
        cfg = config.Config._merge(cfg1, cfg2)
        self.assertEqual(cfg, expect)

    def test_construct_empty(self):

        with self.assertRaises(ValueError):
            config.Config([])

    def test_construct_yaml_one(self):

        # construct with single YAML
        cfg = config.Config.from_yaml([io.StringIO(_CFG1)])
        self.assertEqual(set(cfg._config.keys()), {"queryClasses", "queries"})
        self.assertEqual(set(cfg._config["queryClasses"].keys()), {"LV", "FTSObj"})
        self.assertEqual(set(cfg._config["queries"].keys()), {"LV", "FTSObj"})
        self.assertEqual(set(cfg._queries.keys()), {"LV", "FTSObj"})

        self.assertEqual(cfg.classes(), {"LV", "FTSObj"})
        self.assertEqual(len(cfg.queries("LV")), 2)
        self.assertEqual(len(cfg.queries("FTSObj")), 3)
        self.assertEqual(cfg.concurrentQueries("LV"), 80)
        self.assertEqual(cfg.concurrentQueries("FTSObj"), 12)
        self.assertEqual(cfg.maxRate("LV"), 100)
        self.assertIsNone(cfg.maxRate("FTSObj"))

    def test_construct_yaml_two(self):

        # construct with overrides
        cfg = config.Config.from_yaml([io.StringIO(_CFG1), io.StringIO(_CFG2)])
        self.assertEqual(set(cfg._config.keys()), {"queryClasses", "queries"})
        self.assertEqual(set(cfg._config["queryClasses"].keys()), {"LV", "FTSObj"})
        self.assertEqual(set(cfg._config["queries"].keys()), {"LV", "FTSObj"})
        self.assertEqual(set(cfg._queries.keys()), {"LV", "FTSObj"})

        self.assertEqual(cfg.classes(), {"LV", "FTSObj"})
        self.assertEqual(len(cfg.queries("LV")), 2)
        self.assertEqual(len(cfg.queries("FTSObj")), 3)
        self.assertEqual(cfg.concurrentQueries("LV"), 100)
        self.assertEqual(cfg.concurrentQueries("FTSObj"), 12)
        self.assertEqual(cfg.maxRate("LV"), 100)
        self.assertEqual(cfg.maxRate("FTSObj"), 1)

    def test_split(self):

        # construct with single YAML
        cfg = config.Config.from_yaml([io.StringIO(_CFG1)])

        self.assertEqual(cfg.concurrentQueries("LV"), 80)
        self.assertEqual(cfg.concurrentQueries("FTSObj"), 12)
        self.assertEqual(cfg.maxRate("LV"), 100)
        self.assertIsNone(cfg.maxRate("FTSObj"))

        cfg0 = cfg.split(3, 0)
        cfg1 = cfg.split(3, 1)
        cfg2 = cfg.split(3, 2)
        self.assertEqual(cfg0.concurrentQueries("LV"), 27)
        self.assertEqual(cfg1.concurrentQueries("LV"), 27)
        self.assertEqual(cfg2.concurrentQueries("LV"), 26)
        self.assertEqual(cfg0.maxRate("LV"), 100. / 3.)
        self.assertEqual(cfg1.maxRate("LV"), 100. / 3.)
        self.assertEqual(cfg2.maxRate("LV"), 100. / 3.)
        self.assertEqual(cfg0.concurrentQueries("FTSObj"), 4)
        self.assertEqual(cfg1.concurrentQueries("FTSObj"), 4)
        self.assertEqual(cfg2.concurrentQueries("FTSObj"), 4)
        self.assertIsNone(cfg0.maxRate("FTSObj"))
        self.assertIsNone(cfg1.maxRate("FTSObj"))
        self.assertIsNone(cfg2.maxRate("FTSObj"))


class TestMockDb(unittest.TestCase):

    def test_mock_db(self):

        # can take any args
        conn = mock_db.connect(host="host", port=4040, user='qsmaster',
                               passwd='', db='LSST')

        cursor = conn.cursor()

        query = "SELECT * from Object"
        cursor.execute(query)
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 2)

        query = "SELECT * from Object where O = 5"
        cursor.execute(query)
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 5)

        query = "SELECT * from Ob22ject"
        cursor.execute(query)
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 22)


if __name__ == "__main__":
    unittest.main()
