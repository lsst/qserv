import math
import optparse
import pdb
import random
import sys
from textwrap import dedent
import time
import unittest

import lsst.qserv.master.geometry as g


def _pointsOnCircle(c, r, n, clockwise=False):
    """Generates an n-gon lying on the circle with center c and
    radius r. Vertices are equi-spaced.
    """
    c = g.cartesianUnitVector(c)
    north, east = g.northEast(c)
    points = []
    sr = math.sin(math.radians(r))
    cr = math.cos(math.radians(r))
    aoff = random.uniform(0.0, 2.0 * math.pi)
    for i in xrange(n):
        a = 2.0 * i * math.pi / n
        if not clockwise:
            a = -a
        sa = math.sin(a + aoff)
        ca = math.cos(a + aoff)
        p = (ca * north[0] + sa * east[0],
             ca * north[1] + sa * east[1],
             ca * north[2] + sa * east[2])
        points.append(g.normalize((cr * c[0] + sr * p[0],
                                   cr * c[1] + sr * p[1],
                                   cr * c[2] + sr * p[2])))
    return points

def _pointsOnEllipse(c, smaa, smia, ang, n):
    """Generates points lying on the ellipse with center c,
    semi-major/semi-minor axis lengths smaa/smia, and major axis
    angle (E of N) ang.
    """
    points = []
    c = g.cartesianUnitVector(c)
    north, east = g.northEast(c)
    sa = math.sin(math.radians(ang))
    ca = math.cos(math.radians(ang))
    smaa = math.radians(smaa * g.DEG_PER_ARCSEC)
    smia = math.radians(smia * g.DEG_PER_ARCSEC)
    aoff = random.uniform(0.0, 2.0 * math.pi)
    for i in xrange(n):
        a = aoff + 2.0 * i * math.pi / n
        x = smaa * math.cos(a)
        y = smia * math.sin(a)
        # rotate x, y by a
        nc = ca * x - sa * y
        ec = sa * x + ca * y
        cc = math.sqrt(abs(1.0 - nc * nc - ec * ec))
        points.append(g.normalize((cc * c[0] + nc * north[0] + ec * east[0],
                                   cc * c[1] + nc * north[1] + ec * east[1],
                                   cc * c[2] + nc * north[2] + ec * east[2])))
    return points


class UtilsTestCase(unittest.TestCase):
    """Tests for geometry related utility methods.
    """
    def testCross(self):
        v = g.cross((1.0, 0.0, 0.0), (0.0, 1.0, 0.0))
        self.assertEqual(v, (0.0, 0.0, 1.0))
        v = g.cross((0.0, 1.0, 0.0), (1.0, 0.0, 0.0))
        self.assertEqual(v, (0.0, 0.0, -1.0))
        v = g.cross((0.0, 1.0, 0.0), (0.0, 0.0, 1.0))
        self.assertEqual(v, (1.0, 0.0, 0.0))
        v = g.cross((0.0, 0.0, 1.0), (0.0, 1.0, 0.0))
        self.assertEqual(v, (-1.0, 0.0, 0.0))
        v = g.cross((0.0, 0.0, 1.0), (1.0, 0.0, 0.0))
        self.assertEqual(v, (0.0, 1.0, 0.0))
        v = g.cross((1.0, 0.0, 0.0), (0.0, 0.0, 1.0))
        self.assertEqual(v, (0.0, -1.0, 0.0))

    def testDot(self):
        d = g.dot((1.0, 0.0, 0.0), (1.0, 0.0, 0.0))
        self.assertEqual(d, 1.0)
        d = g.dot((0.0, 1.0, 0.0), (0.0, 1.0, 0.0))
        self.assertEqual(d, 1.0)
        d = g.dot((0.0, 0.0, 1.0), (0.0, 0.0, 1.0))
        self.assertEqual(d, 1.0)
        d = g.dot((0.0, 0.0, 1.0), (1.0, 0.0, 0.0))
        self.assertEqual(d, 0.0)

    def testNormalize(self):
        self.assertRaises(RuntimeError, g.normalize, (0.0, 0.0, 0.0))
        v = g.normalize((1.0, 1.0, 1.0))
        self.assertAlmostEqual(g.dot(v, v), 1.0)

    def testSphericalCoords(self):
        sc = g.sphericalCoords((1.0, 1.0))
        self.assertEqual(sc, (1.0, 1.0))
        sc = g.sphericalCoords((1.0, 0.0, 0.0))
        self.assertAlmostEqual(sc[0], 0.0)
        self.assertAlmostEqual(sc[1], 0.0)
        sc = g.sphericalCoords((0.0, 1.0, 0.0))
        self.assertAlmostEqual(sc[0], 90.0)
        self.assertAlmostEqual(sc[1], 0.0)
        sc = g.sphericalCoords((0.0, -1.0, 0.0))
        self.assertAlmostEqual(sc[0], 270.0)
        self.assertAlmostEqual(sc[1], 0.0)
        sc = g.sphericalCoords((1.0, 1.0, 1.0))
        self.assertAlmostEqual(sc[0], 45.0)
        self.assertAlmostEqual(sc[1], 35.2643896827547)

    def testCartesianUnitVector(self):
        v = g.cartesianUnitVector((1.0, 0.0, 0.0))
        self.assertEqual(v[0], 1.0)
        self.assertEqual(v[1], 0.0)
        self.assertEqual(v[2], 0.0)
        v = g.cartesianUnitVector((1.0, 1.0, 1.0))
        self.assertAlmostEqual(v[0], 0.577350269189626)
        self.assertAlmostEqual(v[1], 0.577350269189626)
        self.assertAlmostEqual(v[2], 0.577350269189626)
        v = g.cartesianUnitVector((45.0, 35.2643896827547))
        self.assertAlmostEqual(v[0], 0.577350269189626)
        self.assertAlmostEqual(v[1], 0.577350269189626)
        self.assertAlmostEqual(v[2], 0.577350269189626)
        v = g.cartesianUnitVector((180.0, 0.0))
        self.assertAlmostEqual(v[0], -1.0)
        self.assertAlmostEqual(v[1], 0.0)
        self.assertAlmostEqual(v[2], 0.0)

    def testClampPhi(self):
        self.assertEqual(g.clampPhi(100.0), 90.0)
        self.assertEqual(g.clampPhi(-100.0), -90.0)
        self.assertEqual(g.clampPhi(0.0), 0.0)

    def testAngularSep(self):
        tv = [((1.0, 1.0, 0.0), (1.0, 0.0, 0.0), 45.0),
              ((1.0, 1.0, 0.0), (0.0, 1.0, 0.0), 45.0),
              ((1.0, 0.0, 1.0), (1.0, 0.0, 0.0), 45.0),
              ((0.0, 1.0, 1.0), (0.0, 0.0, 1.0), 45.0),
              ((1.0, 1.0, 0.0), (1.0, -1.0, 0.0), 90.0),
              ((1.0, 0.0, 1.0), (1.0, 0.0, -1.0), 90.0)
             ]
        for v1, v2, a in tv:
            d = [g.cartesianAngularSep(v1, v2)]
            d.append(g.cartesianAngularSep(v2, v1))
            sc1, sc2 = g.sphericalCoords(v1), g.sphericalCoords(v2)
            d.append(g.sphericalAngularSep(sc1, sc2))
            d.append(g.sphericalAngularSep(sc2, sc1))
            for x in d:
                self.assertAlmostEqual(x, a)

    def testAlpha(self):
        c = (0.0, 0.0)
        for r in (1.0, 10.0, 20.0, 45.0):
            self.assertEqual(g.alpha(r, 0.0, r + 1), None)
            self.assertEqual(g.alpha(r, 0.0, -r - 1), None)
            alpha = g.alpha(r, 0.0, 0.0)
            self.assertAlmostEqual(alpha, r)
            phi = 0.5 * r
            a1 = g.alpha(r, 0.0, phi)
            a2 = g.alpha(r, 0.0, -phi)
            self.assertAlmostEqual(a1, a2)
            self.assertAlmostEqual(g.sphericalAngularSep((a1, phi), c), r)
            self.assertAlmostEqual(g.sphericalAngularSep((-a1, phi), c), r)
            self.assertAlmostEqual(g.sphericalAngularSep((a2, -phi), c), r)
            self.assertAlmostEqual(g.sphericalAngularSep((-a2, -phi), c), r)
        c = (0.0, 89.0)
        self.assertEqual(g.alpha(3.0, 89.0, 89.0), None)
        c = (0.0, -89.0)
        self.assertEqual(g.alpha(3.0, -89.0, -89.0), None)

    def testMaxAlpha(self):
        c = (0.0, 0.0)
        for r in (1.0, 10.0, 20.0, 45.0):
            for phi in (-40.0, -20.0, 0.0, 10.0, 40.0):
                a = g.maxAlpha(r, phi) + g.ANGLE_EPSILON
                for x in (phi, phi + r, phi - r):
                    self.assertTrue(g.sphericalAngularSep((a, x), c) > r)
                    self.assertTrue(g.sphericalAngularSep((-a, x), c) > r)
                points = _pointsOnCircle((0.0, phi), r, 50)
                box = g.SphericalBox((-a, phi - r - g.ANGLE_EPSILON),
                                     (a, phi + r + + g.ANGLE_EPSILON))
                for p in points:
                    self.assertTrue(box.contains(p))


class SphericalBoxTestCase(unittest.TestCase):
    """Tests for the SphericalBox class.
    """
    def testInit(self):
        b = g.SphericalBox()
        self.assertTrue(b.isEmpty())
        self.assertFalse(b.isFull())
        b = g.SphericalBox((0.0, -90.0), (360.0, 90.0))
        self.assertTrue(b.isFull())
        self.assertFalse(b.isEmpty())
        b = g.SphericalBox((-10.0, 0.0), (10.0, 0.0))
        self.assertEqual(b, g.SphericalBox((350.0, 0.0), (10.0, 0.0)))
        self.assertEqual(b, g.SphericalBox((350.0, 0.0), (370.0, 0.0)))
        self.assertRaises(RuntimeError, g.SphericalBox,
                          (350.0, 0.0), (-10.0, 0.0))
        self.assertRaises(RuntimeError, g.SphericalBox,
                          (390.0, 0.0), (-10.0, 0.0))

    def testIntersects(self):
        # box-box intersection
        b1 = g.SphericalBox((0.0, 0.0), (10.0, 10.0))
        self.assertTrue(b1.intersects(b1))
        b2 = g.SphericalBox((0.0, 20.0), (20.0, 25.0))
        self.assertFalse(b1.intersects(b2))
        b2 = g.SphericalBox((0.0, -20.0), (20.0, -5.0))
        self.assertFalse(b1.intersects(b2))
        b2 = g.SphericalBox((30.0, 0.0), (40.0, 25.0))
        self.assertFalse(b1.intersects(b2))
        b2 = g.SphericalBox((350.0, 0.0), (359.0, 25.0))
        self.assertFalse(b1.intersects(b2))
        b2 = g.SphericalBox((0.0, 10.0), (20.0, 25.0))
        self.assertTrue(b1.intersects(b2))
        b2 = g.SphericalBox((10.0, 10.0), (20.0, 25.0))
        self.assertTrue(b1.intersects(b2))
        b2 = g.SphericalBox((350.0, -10.0), (360.0, 0.0))
        self.assertTrue(b1.intersects(b2))
        b2 = g.SphericalBox((-10.0, -10.0), (5.0, 0.0))
        self.assertTrue(b1.intersects(b2))
        b1 = g.SphericalBox((-5.0, -5.0), (1.0, 5.0))
        self.assertTrue(b1.intersects(b2))

    def testContains(self):
        # box-box containment
        b1 = g.SphericalBox((10.0, 0.0), (20.0, 10.0))
        self.assertTrue(b1.contains(b1))
        b2 = g.SphericalBox((11.0, 0.0), (19.0, 10.0))
        self.assertTrue(b1.contains(b2))
        self.assertFalse(b2.contains(b1))
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2 = g.SphericalBox((355.0, 0.0), (365.0, 10.0))
        self.assertTrue(b1.contains(b2))
        self.assertFalse(b2.contains(b1))
        b2 = g.SphericalBox((355.0, 0.0), (359.0, 10.0))
        self.assertTrue(b1.contains(b2))
        self.assertFalse(b2.contains(b1))
        b2 = g.SphericalBox((0.0, 0.0), (5.0, 10.0))
        self.assertTrue(b1.contains(b2))
        self.assertFalse(b2.contains(b1))
        # box-point containment
        b = g.SphericalBox((0.0, 0.0), (10.0, 10.0))
        self.assertTrue(b.contains(b.getMin()))
        self.assertTrue(b.contains(b.getMax()))
        self.assertTrue(b.contains(b.getCenter()))
        b = g.SphericalBox((350.0, 0.0), (370.0, 1.0))
        self.assertTrue(b.contains(b.getMin()))
        self.assertTrue(b.contains(b.getMax()))
        self.assertTrue(b.contains(b.getCenter()))
        self.assertTrue(b.contains((360.0, 0.5)))
        self.assertTrue(b.contains((720.0, 0.5)))
        self.assertTrue(b.contains((5.0, 0.5)))
        self.assertTrue(b.contains((355.0, 0.5)))

    def testShrink(self):
        b1 = g.SphericalBox((0.0, 0.0), (10.0, 10.0))
        b2 = g.SphericalBox((10.0, 10.0), (20.0, 20.0))
        self.assertFalse(b1.contains(b2))
        self.assertTrue(b1.intersects(b2))
        b1.shrink(b2)
        self.assertEqual(b1, g.SphericalBox((10.0, 10.0), (10.0, 10.0)))
        b1 = g.SphericalBox((0.0, 0.0), (15.0, 10.0))
        b1.shrink(b2)
        self.assertFalse(b1.contains(b2))
        self.assertEqual(b1, g.SphericalBox((10.0, 10.0), (15.0, 10.0)))
        b1 = g.SphericalBox((0.0, 0.0), (10.0, 9.0))
        self.assertFalse(b1.contains(b2))
        self.assertFalse(b1.intersects(b2))
        self.assertTrue(b1.shrink(b2).isEmpty())
        b1 = g.SphericalBox((0.0, 0.0), (9.0, 15.0))
        self.assertFalse(b1.contains(b2))
        self.assertFalse(b1.intersects(b2))
        self.assertTrue(b1.shrink(b2).isEmpty())
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2 = g.SphericalBox((5.0, 10.0), (355.0, 20.0))
        b1.shrink(b2)
        self.assertFalse(b1.contains(b2))
        self.assertTrue(b1.intersects(b2))
        self.assertEqual(b1, g.SphericalBox((350.0, 10.0), (370.0, 10.0)))
        b1 = g.SphericalBox((190.0, 0.0), (540.0, 10.0))
        b2 = g.SphericalBox((180.0, 10.0), (190.0, 20.0))
        b1.shrink(b2)
        self.assertFalse(b1.contains(b2))
        self.assertTrue(b1.intersects(b2))
        self.assertEqual(b1, g.SphericalBox((180.0, 10.0), (190.0, 10.0)))

    def testExtend(self):
        # box-box extension
        b1 = g.SphericalBox((0.0, 0.0), (10.0, 10.0))
        b2 = g.SphericalBox((10.0, 10.0), (20.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((0.0, 0.0), (20.0, 20.0)))
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2 = g.SphericalBox((10.0, 10.0), (20.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((350.0, 0.0), (380.0, 20.0)))
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2 = g.SphericalBox((340.0, 10.0), (350.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((340.0, 0.0), (370.0, 20.0)))
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2 = g.SphericalBox((5.0, 10.0), (355.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((0.0, 0.0), (360.0, 20.0)))
        b1 = g.SphericalBox((0.0, 0.0), (10.0, 10.0))
        b2 = g.SphericalBox((20.0, 10.0), (30.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((0.0, 0.0), (30.0, 20.0)))
        b1 = g.SphericalBox((0.0, 0.0), (10.0, 10.0))
        b2 = g.SphericalBox((350.0, 10.0), (360.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((350.0, 0.0), (370.0, 20.0)))
        b1 = g.SphericalBox((5.0, 0.0), (10.0, 10.0))
        b2 = g.SphericalBox((350.0, 10.0), (355.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((350.0, 0.0), (370.0, 20.0)))
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2 = g.SphericalBox((340.0, 10.0), (375.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((340.0, 0.0), (375.0, 20.0)))
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2 = g.SphericalBox((330.0, 10.0), (340.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((330.0, 0.0), (370.0, 20.0)))
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2 = g.SphericalBox((20.0, 10.0), (30.0, 20.0))
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((350.0, 0.0), (390.0, 20.0)))
        b1 = g.SphericalBox()
        b2 = g.SphericalBox()
        b1.extend(b2)
        self.assertTrue(b1.isEmpty())
        b2.setFull()
        b1.extend(b2)
        self.assertTrue(b1.isFull())
        b1.extend(b2)
        self.assertTrue(b1.isFull())
        b1 = g.SphericalBox((350.0, 0.0), (370.0, 10.0))
        b2.setEmpty()
        b1.extend(b2)
        self.assertEqual(b1, g.SphericalBox((350.0, 0.0), (370.0, 10.0)))
        # box-point extension
        b = g.SphericalBox((0.0, 0.0), (10.0, 10.0))
        b.extend((20.0, 20.0))
        self.assertEqual(b, g.SphericalBox((0.0, 0.0), (20.0, 20.0)))
        b.extend((350.0, 15.0))
        self.assertEqual(b, g.SphericalBox((350.0, 0.0), (380.0, 20.0)))
        self.assertRaises(RuntimeError, g.SphericalBox.extend, b, (0.0, 100.0))
        self.assertRaises(RuntimeError, g.SphericalBox.extend, b, (0.0, -100.0))

    def testEdge(self):
        v1 = g.cartesianUnitVector(0.0, 45.0)
        v2 = g.cartesianUnitVector(180.0, 45.0)
        n = (0.0, -1.0, 0.0)
        b = g.SphericalBox.edge(v1, v2, n)
        self.assertEqual(b.getMin()[0], 0.0)
        self.assertEqual(b.getMax()[0], 360.0)
        self.assertAlmostEqual(b.getMin()[1], 45.0)
        self.assertEqual(b.getMax()[1], 90.0)
        v1 = g.cartesianUnitVector(0.0, 45.0)
        v2 = g.cartesianUnitVector(0.0, 75.0)
        b = g.SphericalBox.edge(v1, v2, n)
        self.assertAlmostEqual(b.getMin()[0], 0.0)
        self.assertAlmostEqual(b.getMax()[0], 0.0)
        self.assertAlmostEqual(b.getMin()[1], 45.0)
        self.assertAlmostEqual(b.getMax()[1], 75.0)
        v1 = g.cartesianUnitVector(-0.1 * g.ANGLE_EPSILON, 45.0)
        v2 = g.cartesianUnitVector(180 + 0.1 *g.ANGLE_EPSILON, 45.0)
        n = g.normalize(g.cross(v1, v2))
        b = g.SphericalBox.edge(v1, v2, n)
        self.assertEqual(b.getMin()[0], 0.0)
        self.assertEqual(b.getMax()[0], 360.0)
        self.assertAlmostEqual(b.getMin()[1], 45.0)
        self.assertEqual(b.getMax()[1], 90.0)
        v1 = g.cartesianUnitVector(-0.1 * g.ANGLE_EPSILON, 45.0)
        v2 = g.cartesianUnitVector(0.1 * g.ANGLE_EPSILON, 75.0)
        n = g.normalize(g.cross(v1, v2))
        b = g.SphericalBox.edge(v1, v2, n)
        self.assertAlmostEqual(b.getMin()[0], 360.0)
        self.assertAlmostEqual(b.getMax()[0], 0.0)
        self.assertAlmostEqual(b.getMin()[1], 45.0)
        self.assertAlmostEqual(b.getMax()[1], 75.0)
        v1 = g.cartesianUnitVector(1.0, 1.0)
        v2 = g.cartesianUnitVector(2.0, 2.0)
        n = g.normalize(g.cross(v1, v2))
        b = g.SphericalBox.edge(v1, v2, n)
        self.assertAlmostEqual(b.getMin()[0], 1.0)
        self.assertAlmostEqual(b.getMax()[0], 2.0)
        self.assertAlmostEqual(b.getMin()[1], 1.0)
        self.assertAlmostEqual(b.getMax()[1], 2.0)
        v1 = g.cartesianUnitVector(-1.0, 3.0)
        v2 = g.cartesianUnitVector(2.0, -1.0)
        n = g.normalize(g.cross(v1, v2))
        b = g.SphericalBox.edge(v1, v2, n)
        self.assertAlmostEqual(b.getMin()[0], 359.0)
        self.assertAlmostEqual(b.getMax()[0], 2.0)
        self.assertAlmostEqual(b.getMin()[1], -1.0)
        self.assertAlmostEqual(b.getMax()[1], 3.0)


class SphericalCircleTestCase(unittest.TestCase):
    """Tests for the SphericalCircle class.
    """
    def testInit(self):
        self.assertRaises(RuntimeError, g.SphericalCircle, (0.0, 0.0), 181.0)
        self.assertRaises(RuntimeError, g.SphericalCircle, (0.0, 91.0), 1.0)
        self.assertRaises(RuntimeError, g.SphericalCircle, (0.0, -91.0), 1.0)

    def testSpatialPredicates(self):
        # circle-circle, circle-box, box-circle
        c1 = g.SphericalCircle((0.0, 0.0), 1.0)
        self.assertTrue(c1.intersects(c1))
        self.assertTrue(c1.contains(c1))
        b1 = c1.getBoundingBox()
        self.assertTrue(b1.contains(c1))
        self.assertTrue(b1.intersects(c1))
        c2 = g.SphericalCircle((3.0, 0.0), 1.0)
        b2 = c2.getBoundingBox()
        self.assertTrue(b2.contains(c2))
        self.assertTrue(b2.intersects(c2))
        self.assertFalse(c1.contains(c2))
        self.assertFalse(c1.intersects(c2))
        c1 = g.SphericalCircle((0.0, 0.0), 5.0)
        c3 = g.SphericalCircle((180.0, 0.0), 5.0)
        b1 = c1.getBoundingBox()
        self.assertTrue(c1.contains(c2))
        self.assertFalse(c2.contains(c1))
        self.assertTrue(c1.intersects(c2))
        self.assertTrue(c2.intersects(c1))
        self.assertTrue(b2.intersects(c1))
        self.assertTrue(b2.intersects(c2))
        self.assertTrue(b1.intersects(c1))
        self.assertTrue(b1.intersects(c2))
        self.assertFalse(c3.intersects(b1))
        self.assertFalse(b1.intersects(c3))
        self.assertFalse(c3.contains(b1))
        self.assertFalse(b1.contains(c3))
        b3 = g.SphericalBox((1.0, -1.0), (359.0, 1.0))
        self.assertTrue(b3.intersects(c1))
        self.assertTrue(b3.intersects(c2))
        self.assertTrue(b3.contains(c2))
        self.assertFalse(b3.contains(c1))
        self.assertFalse(c1.contains(b3))
        self.assertFalse(c2.contains(b3))
        # circle-point
        self.assertTrue(c1.contains((1.0, 0.0, 0.0)))
        self.assertTrue(c1.intersects((0.0, 0.0)))
        self.assertFalse(c1.intersects((10.0, 0.0)))
        self.assertFalse(c1.contains((-10.0, 0.0)))
        points = _pointsOnCircle((0.0, 0.0), 4.999, 100)
        for p in points:
            self.assertTrue(c1.contains(p))
        points = _pointsOnCircle((0.0, 0.0), 5.001, 100)
        for p in points:
            self.assertFalse(c1.contains(p))
        # circle-polygon
        p1 = g.SphericalConvexPolygon(_pointsOnCircle((0.0, 0.0), 10.0, 100))
        p2 = g.SphericalConvexPolygon(_pointsOnCircle((0.0, 0.0), 3.0, 100))
        self.assertTrue(c1.contains(p2))
        self.assertTrue(c1.intersects(p2))
        self.assertFalse(c1.contains(p1))
        self.assertTrue(c1.intersects(p2))
        self.assertTrue(p1.intersects(c1))
        self.assertTrue(p2.intersects(c1))
        self.assertTrue(p1.contains(c1))
        self.assertFalse(p2.contains(c1))


class SphericalEllipseTestCase(unittest.TestCase):
    """Tests for the SphericalEllipse class.
    """
    def testInit(self):
        self.assertRaises(RuntimeError, g.SphericalEllipse,
                          (0.0, 0.0), 11.0 * g.ARCSEC_PER_DEG, 1.0, 0.0)
        self.assertRaises(RuntimeError, g.SphericalEllipse,
                          (0.0, 0.0), 0.5, 1.0, 0.0)
        self.assertRaises(RuntimeError, g.SphericalEllipse,
                          (0.0, 0.0), -0.5, -1.0, 0.0)
        self.assertRaises(RuntimeError, g.SphericalEllipse,
                          (0.0, 0.0), 0.5, -1.0, 0.0)
        self.assertRaises(RuntimeError, g.SphericalEllipse,
                          (0.0, 0.0), -0.5, 1.0, 0.0)
        self.assertRaises(RuntimeError, g.SphericalEllipse,
                          (0.0, 91.0), 0.0, 0.0, 0.0)
        self.assertRaises(RuntimeError, g.SphericalEllipse,
                          (0.0, -91.0), 0.0, 0.0, 0.0)

    def testSpatialPredicates(self):
        # ellipse-point
        for cen in ((0.0, 0.0), (45.0, 45.0), (135.0, -45.0), (0.0, 90.0)):
            smaa = random.uniform(30, 20000)
            smia = random.uniform(1, smaa)
            a = random.uniform(0.0, 360.0)
            e = g.SphericalEllipse(cen, smaa, smia, a)
            self.assertTrue(e.contains(cen))
            self.assertTrue(e.contains(g.cartesianUnitVector(cen)))
            self.assertTrue(e.intersects(cen))
            self.assertTrue(e.intersects(g.cartesianUnitVector(cen)))
            points = _pointsOnCircle(
                cen, e.getInnerCircle().getRadius() - g.ANGLE_EPSILON, 100)
            for p in points:
                self.assertTrue(e.contains(p))
            points = _pointsOnCircle(
                cen, e.getBoundingCircle().getRadius() + g.ANGLE_EPSILON, 100)
            for p in points:
                self.assertFalse(e.intersects(p))
            points = _pointsOnEllipse(
                cen, smaa - g.ANGLE_EPSILON * g.ARCSEC_PER_DEG,
                smia - g.ANGLE_EPSILON * g.ARCSEC_PER_DEG, a, 100)
            for p in points:
                self.assertTrue(e.contains(p))
            points = _pointsOnEllipse(
                cen, smaa + g.ANGLE_EPSILON * g.ARCSEC_PER_DEG,
                smia + g.ANGLE_EPSILON * g.ARCSEC_PER_DEG, a, 100)
            for p in points:
                self.assertFalse(e.contains(p))
        # ellipse-circle
        cen = (0.0, 0.0)
        e = g.SphericalEllipse(cen, 7200.0, 3600.0, 90.0)
        c = g.SphericalCircle(
            cen, e.getInnerCircle().getRadius() - g.ANGLE_EPSILON)
        self.assertTrue(e.intersects(c))
        self.assertTrue(c.intersects(e))
        self.assertTrue(e.contains(c))
        self.assertFalse(c.contains(e))
        # ellipse-box
        b = g.SphericalBox((-0.5, -0.5), (0.5, 0.5))
        self.assertTrue(e.intersects(b))
        self.assertTrue(e.contains(b))
        b = g.SphericalBox((0.5, -0.5), (359.5, 0.5))
        self.assertTrue(e.intersects(b))
        self.assertFalse(e.contains(b))


class SphericalConvexPolygonTestCase(unittest.TestCase):
    """Tests for the SphericalConvexPolygon class.
    """
    def testInit(self):
        self.assertRaises(RuntimeError, g.SphericalConvexPolygon,
                          [(1.0, 0.0, 0.0)])
        self.assertRaises(RuntimeError, g.SphericalConvexPolygon,
                          [(1.0, 0.0, 0.0), (0.0, 1.0, 0.0)])
        self.assertRaises(RuntimeError, g.SphericalConvexPolygon,
                          [(1.0, 0.0, 0.0), (0.0, 1.0, 0.0), (0.0, 0.0, 1.0)],
                          [(0.0, 0.0, 1.0)])

    def testSpatialPredicates(self):
        # polygon-point, polygon-circle
        p = g.SphericalConvexPolygon(_pointsOnCircle((0.0, 0.0), 1.0, 8))
        self.assertTrue(p.contains(p.getBoundingCircle().getCenter()))
        self.assertTrue(p.intersects(p.getBoundingCircle().getCenter()))
        points = _pointsOnCircle((0.0, 0.0), 0.5, 50)
        for v in points:
            self.assertTrue(p.contains(v))
            self.assertTrue(p.intersects(v))
        c = g.SphericalCircle((0.0, 0.0), 0.5)
        self.assertTrue(p.contains(c))
        self.assertTrue(p.intersects(c))
        self.assertFalse(c.contains(p))
        self.assertTrue(c.intersects(p))
        points = _pointsOnCircle((0.0, 0.0), 1.001, 50)
        for v in points:
            self.assertFalse(p.contains(v))
            self.assertFalse(p.intersects(v))
        c = g.SphericalCircle((0.0, 0.0), 1.001)
        c2 = g.SphericalCircle((180.0, 0.0), 1.001)
        self.assertTrue(c.contains(p))
        self.assertTrue(c.intersects(p))
        self.assertFalse(c2.intersects(p))
        self.assertFalse(p.intersects(c2))
        self.assertFalse(c2.contains(p))
        self.assertFalse(p.contains(c2))
        self.assertFalse(p.contains(c))
        self.assertTrue(p.intersects(c))
        # polygon-box
        b = g.SphericalBox((-0.5, -0.5), (0.5, 0.5))
        self.assertTrue(b.intersects(p))
        self.assertTrue(p.intersects(b))
        self.assertTrue(p.contains(b))
        self.assertFalse(b.contains(p))
        b = g.SphericalBox((0.5, -0.5), (359.5, 0.5))
        self.assertTrue(b.intersects(p))
        self.assertTrue(p.intersects(b))
        self.assertFalse(b.contains(p))
        self.assertFalse(p.contains(b))
        # polygon-polygon
        self.assertTrue(p.intersects(p))
        p2 = g.SphericalConvexPolygon(_pointsOnCircle((0.5, 0.0), 1.0, 8))
        self.assertTrue(p.intersects(p2))
        self.assertTrue(p2.intersects(p))
        self.assertFalse(p.contains(p2))
        self.assertFalse(p2.contains(p))
        p3 = g.SphericalConvexPolygon(_pointsOnCircle((0.25, 0.0), 0.25, 8))
        self.assertTrue(p.contains(p3))


class MedianTestCase(unittest.TestCase):
    """Tests the "median-of-medians" median finding algorithm.
    """
    def testEdgeCases(self):
        a = []
        self.assertEqual(g.median(a), None)
        for i in xrange(1, 50):
            a = [1] * i
            self.assertEqual(g.median(a), 1)

    def testMedian(self):
        for i in xrange(2, 100):
            a = [j for j in xrange(1, i)]
            m = a[len(a) / 2]
            self.assertEqual(g.median(a), m)
            a.reverse()
            self.assertEqual(g.median(a), m)
            random.shuffle(a)
            self.assertEqual(g.median(a), m)
        for i in xrange(2, 50):
            a = []
            for j in xrange(1, i):
                for k in xrange(random.randint(1, 10)):
                    a.append(j)
            m = a[len(a) / 2]
            self.assertEqual(g.median(a), m)
            a.reverse()
            self.assertEqual(g.median(a), m)
            random.shuffle(a)
            self.assertEqual(g.median(a), m)


def _hemPoints(v, n):
    """Randomly generates a list of n points in the hemisphere
    given by the plane with normal v.
    """
    v = g.normalize(v)
    north, east = g.northEast(v)
    points = []
    for i in xrange(n):
        z = -1.0
        while z < 0.0:
            x = random.uniform(-1.0 + 1.0e-7, 1.0 - 1.0e-7)
            y = random.uniform(-1.0 + 1.0e-7, 1.0 - 1.0e-7)
            z = 1.0 - x * x - y * y
        z = math.sqrt(z)
        p = (z * v[0] + x * north[0] + y * east[0],
             z * v[1] + x * north[1] + y * east[1],
             z * v[2] + x * north[2] + y * east[2])
        points.append(g.normalize(p))
    return points

def _opposing(points):
    """Returns a point p such that adding p to points
    results in a non-hemispherical point list.
    """
    if len(points) > 1:
        n = min(3, len(points))
        return g.normalize((sum(-u[0] for u in points[:n]),
                            sum(-u[1] for u in points[:n]),
                            sum(-u[2] for u in points[:n])))
    elif len(points) == 1:
        return (-points[0][0], -points[0][1], -points[0][2])


class HemisphericalTestCase(unittest.TestCase):
    """Tests the hemispherical() function.
    """
    def testHemispherical(self):
        x = (1.0, 0.0, 0.0)
        y = (0.0, 1.0, 0.0)
        z = (0.0, 0.0, 1.0)
        nx = (-1.0, 0.0, 0.0)
        ny = (0.0, -1.0, 0.0)
        nz = (0.0, 0.0, -1.0)
        # Cannot use itertools.product and itertools.permutations until
        # Python 2.6+ becomes standard.
        self.assertTrue(g.hemispherical([x, y, z]))
        self.assertTrue(g.hemispherical([x, y, nz]))
        self.assertTrue(g.hemispherical([x, ny, z]))
        self.assertTrue(g.hemispherical([x, ny, nz]))
        self.assertTrue(g.hemispherical([nx, y, z]))
        self.assertTrue(g.hemispherical([nx, y, nz]))
        self.assertTrue(g.hemispherical([nx, ny, z]))
        self.assertTrue(g.hemispherical([nx, ny, nz]))
        self.assertTrue(g.hemispherical([x]))
        self.assertTrue(g.hemispherical([y]))
        self.assertTrue(g.hemispherical([z]))
        self.assertFalse(g.hemispherical([x, nx]))
        self.assertFalse(g.hemispherical([y, ny]))
        self.assertFalse(g.hemispherical([z, nz]))
        self.assertFalse(g.hemispherical([x, nx, z]))
        self.assertFalse(g.hemispherical([x, nx, y]))
        self.assertFalse(g.hemispherical([x, nx, nz]))
        self.assertFalse(g.hemispherical([x, nx, ny]))
        self.assertFalse(g.hemispherical([y, ny, x]))
        self.assertFalse(g.hemispherical([y, ny, z]))
        self.assertFalse(g.hemispherical([y, ny, nx]))
        self.assertFalse(g.hemispherical([y, ny, nz]))
        self.assertFalse(g.hemispherical([z, nz, x]))
        self.assertFalse(g.hemispherical([z, nz, y]))
        self.assertFalse(g.hemispherical([z, nz, nx]))
        self.assertFalse(g.hemispherical([z, nz, ny]))
        self.assertFalse(g.hemispherical([x, y, z, nx, ny, nz]))
        for v in ((1.0, 1.0, 1.0), (-1.0, 1.0, 1.0), (1.0, -1.0, 1.0),
                  (1.0, 1.0, -1.0), (1.0, -1.0, -1.0), (-1.0, 1.0, -1.0),
                  (-1.0, -1.0, 1.0), (-1.0, -1.0, -1.0), x, y, z, nx, ny, nz):
            # test with randomly generated point clouds
            points = _hemPoints(v, 1000)
            p = _opposing(points)
            self.assertTrue(g.hemispherical(points))
            self.assertFalse(g.hemispherical(points + [p]))
            random.shuffle(points)
            p = _opposing(points)
            self.assertTrue(g.hemispherical(points))
            self.assertFalse(g.hemispherical(points + [p]))
        for v in ((1.0, 1.0, 1.0), (-1.0, 1.0, 1.0),
                  (1.0, -1.0, 1.0), (1.0, 1.0, -1.0), x, y, z):
            # test with points lying on circles
            p1 = _pointsOnCircle(v, 89.9999, 10)
            p2 = _pointsOnCircle((-v[0], -v[1], -v[2]), 89.9999, 10)
            self.assertTrue(g.hemispherical(p1))
            self.assertTrue(g.hemispherical(p2))
            self.assertFalse(g.hemispherical(p1 + p2))


def _pointsOnPeriodicHypotrochoid(c, R, r, d, n):
    assert isinstance(r, int) and isinstance(R, int)
    assert R > r
    assert (2 * r) % (R - r) == 0
    points = []
    c = g.cartesianUnitVector(c)
    north, east = g.northEast(c)
    scale = 1.0 / (R - r + d + 1)
    period = (2 * r) / (R - r)
    if period % 2 != 0:
        period *= 2
    for i in xrange(n):
        theta = i * period * math.pi / n
        x = (R - r) * math.cos(theta) + d * math.cos((R - r) * theta / r)
        y = (R - r) * math.sin(theta) - d * math.sin((R - r) * theta / r)
        x *= scale
        y *= scale
        z = math.sqrt(1.0 - x * x - y * y)
        points.append(g.normalize((z * c[0] + x * north[0] + y * east[0],
                                   z * c[1] + x * north[1] + y * east[1],
                                   z * c[2] + x * north[2] + y * east[2])))
    return points

def _checkHull(hull, points):
    vertices = set(hull.getVertices())
    for p in points:
        if p not in vertices:
            for e in hull.getEdges():
                if g.dot(p, e) < -g.CROSS_N2MIN:
                    return False
    return True

class ConvexTestCase(unittest.TestCase):
    """Tests the convex hull algorithm and convexity test.
    """
    def testConvex(self):
        for c in ((0.0, 0.0), (0.0, 90.0), (0.0, -90.0), (45.0, 45.0)):
            points = _pointsOnCircle(c, 10.0, 100)
            results = g.convex(points)
            self.assertTrue(results[0])
            self.assertTrue(results[1])
            points.reverse()
            results = g.convex(points)
            self.assertTrue(results[0])
            self.assertFalse(results[1])
            smaa = random.uniform(0.5 * g.ARCSEC_PER_DEG,
                                  5.0 * g.ARCSEC_PER_DEG)
            smia = random.uniform(0.01 * g.ARCSEC_PER_DEG, smaa)
            ang = random.uniform(0.0, 180.0)
            points = _pointsOnEllipse(c, smaa, smia, ang, 100)
            results = g.convex(points)
            self.assertTrue(results[0])
            self.assertFalse(results[1])
            points.reverse()
            results = g.convex(points)
            self.assertTrue(results[0])
            self.assertTrue(results[1])
        # Test with duplicate vertices
        points = _pointsOnCircle(c, 10.0, 10)
        points += points
        results = g.convex(points)
        self.assertFalse(results[0])
        # Test with non-convex vertex lists
        points = [(1.0, 0.0, 0.0),
                  g.normalize((1.0, 1.0, 1.0)),
                  (0.0, 1.0, 0.0),
                  (0.0, 0.0, 1.0)]
        results = g.convex(points)
        self.assertFalse(results[0])
        # consecutive edges of the following hypotrochoids look convex,
        # but the polygon formed by them is self-intersecting - it winds
        # around the vertex centroid multiple times.
        points = _pointsOnPeriodicHypotrochoid((1.0, 1.0, 1.0), 5, 3, 5, 100)
        results = g.convex(points)
        self.assertFalse(results[0])
        points = _pointsOnPeriodicHypotrochoid((1.0, 1.0, 1.0), 6, 4, 6, 100)
        results = g.convex(points)
        self.assertFalse(results[0])

    def testConvexHull(self):
        points = _hemPoints((1.0, 1.0, 1.0), 1000)
        hull = g.convexHull(points)
        self.assertNotEqual(hull, None)
        self.assertTrue(g.convex(hull.getVertices()))
        self.assertTrue(_checkHull(hull, points))
        points = _pointsOnPeriodicHypotrochoid((1.0, 1.0, -1.0),
                                               15, 13, 15, 1000)
        hull = g.convexHull(points)
        self.assertNotEqual(hull, None)
        self.assertTrue(g.convex(hull.getVertices()))
        self.assertTrue(_checkHull(hull, points))
        random.shuffle(points)
        hull = g.convexHull(points)
        self.assertNotEqual(hull, None)
        self.assertTrue(g.convex(hull.getVertices()))
        self.assertTrue(_checkHull(hull, points))
        # Test with duplicate vertices
        points = _pointsOnCircle((0.0, 0.0), 10.0, 100, True)
        points2 = points + points
        hull = g.convexHull(points2)
        self.assertNotEqual(hull, None)
        self.assertTrue(g.convex(hull.getVertices()))
        vertices = set(hull.getVertices())
        self.assertEqual(len(points), len(vertices))
        for p in points:
            self.assertTrue(p in vertices)
        random.shuffle(points2)
        hull = g.convexHull(points2)
        self.assertNotEqual(hull, None)
        self.assertTrue(g.convex(hull.getVertices()))
        vertices = set(hull.getVertices())
        self.assertEqual(len(points), len(vertices))
        for p in points:
            self.assertTrue(p in vertices)
        # Test with non hemispherical points
        points = _hemPoints((1.0, -1.0, -0.5), 1000)
        points.append(_opposing(points))
        hull = g.convexHull(points)
        self.assertEqual(hull, None)


class SphericalBoxPartitionMapTestCase(unittest.TestCase):
    """Tests for the spherical box unit sphere partitioning scheme.
    """
    def testSubList(self):
        a = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        s = g._SubList(a)
        s.append(9)
        s.append(5)
        s.append(4)
        s.append(1)
        self.assertEqual(len(s), 4)
        x = max(a) + 1
        for y in s:
            self.assertTrue(y < x)
            x = y
        s.filter(lambda x: x == 5)
        self.assertEqual(len(s), 3)
        x = max(a) + 1
        for y in s:
            self.assertTrue(y < x)
            x = y
        s.append(0)
        self.assertEqual(len(s), 4)
        x = max(a) + 1
        for y in s:
            self.assertTrue(y < x)
            x = y

    def testInit(self):
        self.assertRaises(TypeError, g.SphericalBoxPartitionMap, 1.0, 1)
        self.assertRaises(TypeError, g.SphericalBoxPartitionMap, 1, 1.0)
        self.assertRaises(RuntimeError, g.SphericalBoxPartitionMap, 0, 1)
        self.assertRaises(RuntimeError, g.SphericalBoxPartitionMap, 1, 0)

    def testIter(self):
        pmap = g.SphericalBoxPartitionMap(1, 1)
        nc = 0
        for chunkId, subIter in pmap:
            nc += 1
            self.assertEqual(chunkId, 0)
            nsc = 0
            for subChunkId in subIter:
                nsc += 1
                self.assertEqual(subChunkId, 0)
            self.assertEqual(nsc, 1)
        self.assertEqual(nc, 1)
        pmap = g.SphericalBoxPartitionMap(18, 10)
        nc = sum(pmap.numChunks)
        for chunkId, subIter in pmap:
            nc -= 1
        self.assertEqual(nc, 0)

    def testIntersect(self):
        pmap = g.SphericalBoxPartitionMap(1, 1)
        b = g.SphericalBox((0.0, -90.0), (360.0, 90.0))
        for chunkId, subIter in pmap.intersect(b):
            self.assertEqual(chunkId, 0)
            for subChunkId, regions in subIter:
                self.assertEqual(subChunkId, 0)
                self.assertEqual(regions, set())
        pmap = g.SphericalBoxPartitionMap(18, 10)
        b = g.SphericalBox((0.01, 0.01), (0.99, 0.99))
        for chunkId, subIter in pmap.intersect(b):
            self.assertEqual(chunkId, 324)
            for subChunkId, regions in subIter:
                self.assertEqual(subChunkId, 0)
                self.assertEqual(len(regions), 1)
                self.assertTrue(b in regions)
        # Test with an assortment of regions
        regions = [g.SphericalBox((-0.01, -0.01), (11.0, 11.0)),
                   g.SphericalBox((350.0, 80.0), (360.0, 90.0)),
                   g.SphericalBox((350.0, -90.0), (360.0, -80.0)),
                   g.SphericalBox((-5.0, 20.0), (5.0, 22.0)),
                   g.SphericalBox((0.0, -25.0), (2.0, -20.0)),
                   g.SphericalCircle((45.0, 45.0), 1.0),
                   g.SphericalConvexPolygon((-1.0, 0.0, 0.0), (0.0, 1.0, 0.0),
                                            g.normalize((-1.0, 1.0, -1.0))),
                  ]
        # compute expected results by brute force
        results = {}
        for chunkId, subIter in pmap:
            for subChunkId in subIter:
                bbox = pmap.getSubChunkBoundingBox(chunkId, subChunkId)
                for i in xrange(len(regions)):
                    if regions[i].contains(bbox):
                        results[(chunkId, subChunkId)] = set()
                        break
                    elif regions[i].intersects(bbox):
                        if (chunkId, subChunkId) not in results:
                            results[(chunkId, subChunkId)] = set()
                        results[(chunkId, subChunkId)].add(regions[i])
        # and compare them to the results of intersect()
        for chunkId, subIter in pmap.intersect(regions):
            for subChunkId, xr in subIter:
                exr = results.pop((chunkId, subChunkId), None)
                self.assertEqual(xr, exr)
        self.assertEqual(len(results), 0)


def main():
    # The seed value used by the python random number generator can
    # be passed in on the command line, allowing for unit test
    # repeatability.
    parser = optparse.OptionParser("%prog [options]")
    parser.add_option(
        "-s", "--seed", type="long", dest="seed",
        help=dedent("""\
            Specifies the seed used to initialize the python random number
            generator. By default, the system time is used to initialize
            the generator."""))
    opts, inputs = parser.parse_args()
    if len(inputs) != 0:
        parser.error('Command line contains extraneous arguments')
    seed = opts.seed
    if seed == None:
        seed = long(time.time())
    print 'Seeding random number generator with %d' % seed
    random.seed(seed)
    suite = unittest.TestSuite(map(unittest.makeSuite,
        [UtilsTestCase,
         SphericalBoxTestCase,
         SphericalCircleTestCase,
         SphericalEllipseTestCase,
         SphericalConvexPolygonTestCase,
         MedianTestCase,
         HemisphericalTestCase,
         ConvexTestCase,
         SphericalBoxPartitionMapTestCase
        ]))
    run = unittest.TextTestRunner().run(suite)
    sys.exit(0)

if __name__ == '__main__':
    main()
