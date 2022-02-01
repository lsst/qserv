# This file is part of qserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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


import unittest

from lsst.qserv.admin.cli.render_targs import render_targs, UnresolvableTemplate


class RenderTargsTestCase(unittest.TestCase):

    def testMutualReference(self):
        """Test for failure when targs refer directly to each other."""
        with self.assertRaises(UnresolvableTemplate) as r:
            render_targs({"a": "{{b}}", "b": "{{a}}"})
        self.assertIn("a={{b}}, b={{a}}", str(r.exception))

    def testCircularReference(self):
        """Test for failure when there is a circular reference in targs."""
        with self.assertRaises(UnresolvableTemplate) as r:
            render_targs({"a": "{{b}}", "b": "{{c}}", "c": "{{a}}"})
        self.assertIn("a={{b}}, b={{c}}, c={{a}}", str(r.exception))

    def testSelfReference(self):
        """Test for failure when a targ refers to itself."""
        with self.assertRaises(UnresolvableTemplate) as r:
            render_targs({"a": "{{a}}"})
        self.assertIn("a={{a}}", str(r.exception))

    def testMissingValue(self):
        """Test for failure when a template value is not provided."""
        with self.assertRaises(UnresolvableTemplate) as r:
            render_targs({"a": "{{b}}"})
        self.assertIn("Missing template value:", str(r.exception))

    def testList(self):
        """Verify that lists can be used as values and manipulated by the template."""
        self.assertEqual(
            render_targs({"a": "{{b|join(' ')}}", "b": ["foo", "bar"]}),
            {"a": "foo bar", "b": ["foo", "bar"]}
        )

    def testResolves(self):
        """Verify that a dict with legal values resolves correctly."""
        self.assertEqual(
            render_targs({"a": "{{b}}", "b": "{{c}}", "c": "d"}),
            {"a": "d", "b": "d", "c": "d"}
        )

    def testNone(self):
        """Test that a dict with None as a value resolves correctly."""
        self.assertEqual(
            render_targs({"a": None}),
            {"a": None}
        )

    def testBool(self):
        """Test that a dict with a bool as a value resolves correctly."""
        self.assertEqual(
            render_targs({"a": True}),
            {"a": True}
        )


if __name__ == "__main__":
    unittest.main()
