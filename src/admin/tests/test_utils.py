# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit tests for the utils module.
"""

import unittest
from unittest.mock import patch

from lsst.qserv.admin.cli import utils


class SplitKvTestCase(unittest.TestCase):

    def test_split_kv(self):
        self.assertEquals(utils.split_kv([]), dict())
        self.assertEquals(utils.split_kv(["a=1"]), dict(a="1"))
        self.assertEquals(utils.split_kv(["a=1,b=2"]), dict(a="1", b="2"))
        self.assertEquals(utils.split_kv(["a=1,b=2", "c=3"]), dict(a="1", b="2", c="3"))

    def test_invalid_input(self):
        with self.assertRaises(RuntimeError):
            utils.split_kv(["a"])
        with self.assertRaises(RuntimeError):
            utils.split_kv(["a=b=c"])


if __name__ == "__main__":
    unittest.main()
