/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

#include <set>
#include <stdexcept>
#include <vector>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Vector
#include "boost/test/unit_test.hpp"
#include "boost/math/constants/constants.hpp"

#include "Constants.h"
#include "Geometry.h"

using std::cos;
using std::exception;
using std::fabs;
using std::pair;
using std::sin;
using std::sqrt;
using std::vector;

using boost::math::constants::pi;

using lsst::qserv::admin::dupr::Matrix3d;
using lsst::qserv::admin::dupr::Vector3d;

namespace dupr = lsst::qserv::admin::dupr;


namespace {

    void checkClose(Vector3d const & u, Vector3d const & v, double fraction) {
        BOOST_CHECK_CLOSE_FRACTION(u.dot(v), u.norm()*v.norm(), fraction);
    }

    void checkClose(pair<double, double> const & u,
                    pair<double, double> const & v, double fraction) {
        BOOST_CHECK_CLOSE_FRACTION(u.first, v.first, fraction);
        BOOST_CHECK_CLOSE_FRACTION(u.second, v.second, fraction);
    }

    // Given a unit vector v, construct 2 vectors n and e such that:
    //  - all 3 vectors are orthonormal
    //  - n is tangent (at v) to the great circle segment joining v
    //    to the north pole, i.e. it is the "north" vector at v.
    //  - e is the "east" vector at v.
    // (n,e) thus form a basis for the plane tangent to the unit sphere at v.
    void northEast(Vector3d & n, Vector3d & e, Vector3d const & v) {
        n(0) = -v(0)*v(2);
        n(1) = -v(1)*v(2);
        n(2) = v(0)*v(0) + v(1)*v(1);
        if (n(0) == 0.0 && n(1) == 0.0 && n(2) == 0.0) {
           n(0) = -1.0;
           e(0) = 0.0;
           e(1) = 1.0;
           e(2) = 0.0;
        } else {
           n = n.normalized();
           e = n.cross(v).normalized();
        }
    }

    enum {
        S0 = (0 + 8), S00 = (0 + 8)*4, S01, S02, S03,
        S1 = (1 + 8), S10 = (1 + 8)*4, S11, S12, S13,
        S2 = (2 + 8), S20 = (2 + 8)*4, S21, S22, S23,
        S3 = (3 + 8), S30 = (3 + 8)*4, S31, S32, S33,
        N0 = (4 + 8), N00 = (4 + 8)*4, N01, N02, N03,
        N1 = (5 + 8), N10 = (5 + 8)*4, N11, N12, N13,
        N2 = (6 + 8), N20 = (6 + 8)*4, N21, N22, N23,
        N3 = (7 + 8), N30 = (7 + 8)*4, N31, N32, N33
    };

    size_t const NPOINTS = 38;
    double const C0 = 0.577350269189625764509148780503; // √3/3
    double const C1 = 0.270598050073098492199861602684; // 1 / (2*√(2 + √2))
    double const C2 = 0.923879532511286756128183189400; // (1 + √2) / (√2 * √(2 + √2))

    Vector3d const _points[NPOINTS] = {
        Vector3d(  1,  0,  0), //  x
        Vector3d(  0,  1,  0), //  y
        Vector3d(  0,  0,  1), //  z
        Vector3d( -1,  0,  0), // -x
        Vector3d(  0, -1,  0), // -y
        Vector3d(  0,  0, -1), // -z
        Vector3d( C0, C0, C0), // center of N3
        Vector3d(-C0, C0, C0), // center of N2
        Vector3d(-C0,-C0, C0), // center of N1
        Vector3d( C0,-C0, C0), // center of N0
        Vector3d( C0, C0,-C0), // center of S0
        Vector3d(-C0, C0,-C0), // center of S1
        Vector3d(-C0,-C0,-C0), // center of S2
        Vector3d( C0,-C0,-C0), // center of S3
        Vector3d( C1, C1, C2), // center of N31
        Vector3d( C2, C1, C1), // center of N32
        Vector3d( C1, C2, C1), // center of N30
        Vector3d(-C1, C1, C2), // center of N21
        Vector3d(-C1, C2, C1), // center of N22
        Vector3d(-C2, C1, C1), // center of N20
        Vector3d(-C1,-C1, C2), // center of N11
        Vector3d(-C2,-C1, C1), // center of N12
        Vector3d(-C1,-C2, C1), // center of N10
        Vector3d( C1,-C1, C2), // center of N01
        Vector3d( C1,-C2, C1), // center of N02
        Vector3d( C2,-C1, C1), // center of N00
        Vector3d( C1, C1,-C2), // center of S01
        Vector3d( C2, C1,-C1), // center of S00
        Vector3d( C1, C2,-C1), // center of S02
        Vector3d(-C1, C1,-C2), // center of S11
        Vector3d(-C1, C2,-C1), // center of S10
        Vector3d(-C2, C1,-C1), // center of S12
        Vector3d(-C1,-C1,-C2), // center of S21
        Vector3d(-C2,-C1,-C1), // center of S20
        Vector3d(-C1,-C2,-C1), // center of S22
        Vector3d( C1,-C1,-C2), // center of S31
        Vector3d( C1,-C2,-C1), // center of S30
        Vector3d( C2,-C1,-C1), // center of S32
    };

    uint32_t const _ids[NPOINTS] = {
        N32, N22, N31, N12, N02, S01,
        N33, N23, N13, N03, S03, S13, S23, S33,
        N31, N32, N30, N21, N22, N20, N11, N12,
        N10, N01, N02, N00, S01, S00, S02, S11,
        S10, S12, S21, S20, S22, S31, S30, S32
    };

    // Generate n points in a circle of radius r around (lon,lat).
    vector<pair<double, double> > const ngon(
        double lon, double lat, double r, int nv)
    {
        vector<pair<double, double> > points;
        Vector3d n, e, v = dupr::cartesian(lon, lat);
        northEast(n, e, v);
        double sinr = sin(r * dupr::RAD_PER_DEG);
        double cosr = cos(r * dupr::RAD_PER_DEG);
        double da = 360.0/nv;
        for (double a = 0; a < 360.0 - dupr::EPSILON_DEG; a += da) {
            double sina = sin(a * dupr::RAD_PER_DEG);
            double cosa = cos(a * dupr::RAD_PER_DEG);
            Vector3d p = cosr * v + sinr * (cosa * e + sina * n);
            points.push_back(spherical(p));
        }
        return points;
    }

    dupr::SphericalTriangle const tri(double lon, double lat, double r) {
        vector<pair<double, double> > p = ngon(lon, lat, r, 3);
        return dupr::SphericalTriangle(dupr::cartesian(p[0]),
                                       dupr::cartesian(p[1]),
                                       dupr::cartesian(p[2]));
    }

    // Find IDs of HTM triangles overlapping a box.
    vector<uint32_t> const htmIds(dupr::SphericalBox const & b, int level) {
         std::set<uint32_t> ids;
         double lon = b.getLonMin(), lat = b.getLatMin();
         double deltaLon = b.getLonExtent() / 128;
         double deltaLat = (b.getLatMax() - b.getLatMin()) / 128;
         for (int i = 0; i < 128; ++i) {
             for (int j = 0; j < 128; ++j) {
                 Vector3d v = dupr::cartesian(
                     lon + deltaLon*i, lat + deltaLat*j);
                 ids.insert(dupr::htmId(v, level)); 
             }
         }
         return vector<uint32_t>(ids.begin(), ids.end());
    }

    bool isSubset(vector<uint32_t> const & v1, vector<uint32_t> const & v2) {
        typedef vector<uint32_t>::const_iterator Iter;
        Iter j = v2.begin(), je = v2.end();
        for (Iter i = v1.begin(), ie = v1.end(); i != ie && j != je; ++i) {
            for (; j != je && *i != *j; ++j) { }
        }
        return j != je;
    }

} // unnamed namespace


BOOST_AUTO_TEST_CASE(ClampLatTest) {
    BOOST_CHECK_EQUAL(dupr::clampLat(-91.0), -90.0);
    BOOST_CHECK_EQUAL(dupr::clampLat( 91.0),  90.0);
    BOOST_CHECK_EQUAL(dupr::clampLat( 89.0),  89.0);
}

BOOST_AUTO_TEST_CASE(MinDeltaLonTest) {
    BOOST_CHECK_EQUAL(dupr::minDeltaLon(1.0, 2.0), 1.0);
    BOOST_CHECK_EQUAL(dupr::minDeltaLon(359.0, 1.0), 2.0);
    BOOST_CHECK_EQUAL(dupr::minDeltaLon(10.0, 350.0), 20.0);
}

BOOST_AUTO_TEST_CASE(ReduceLonTest) {
    BOOST_CHECK_EQUAL(dupr::reduceLon(0.0), 0.0);
    BOOST_CHECK_EQUAL(dupr::reduceLon(360.0), 0.0);
    BOOST_CHECK_EQUAL(dupr::reduceLon(540.0), 180.0);
    BOOST_CHECK_EQUAL(dupr::reduceLon(-180.0), 180.0);
}

BOOST_AUTO_TEST_CASE(MaxAlphaTest) {
    // Check corner cases.
    BOOST_CHECK_EQUAL(dupr::maxAlpha(10.0, 85.0), 180.0);
    BOOST_CHECK_EQUAL(dupr::maxAlpha(10.0, -85.0), 180.0);
    BOOST_CHECK_EQUAL(dupr::maxAlpha(0.0, 30.0), 0.0);
    BOOST_CHECK_THROW(dupr::maxAlpha(-1.0, 0.0), exception);
    BOOST_CHECK_THROW(dupr::maxAlpha(91.0, 0.0), exception);
    // Generate points in a circle of radius 1 deg and check that
    // each point has longitude within alpha of the center longitude.
    vector<pair<double, double> > circle = ngon(0.0, 45.0, 1.0, 360*16);
    double alpha = dupr::maxAlpha(1.0, 45.0);
    for (size_t i = 0; i < circle.size(); ++i) {
        double lon = dupr::minDeltaLon(0.0, circle[i].first);
        BOOST_CHECK_LT(lon, alpha + dupr::EPSILON_DEG);
    }
}

BOOST_AUTO_TEST_CASE(HtmIdTest) {
    // Check corner cases.
    Vector3d x(1,0,0);
    BOOST_CHECK_THROW(dupr::htmId(x, -1), exception);
    BOOST_CHECK_THROW(dupr::htmId(x, dupr::HTM_MAX_LEVEL + 1), exception);
    // Check test points.
    for (size_t i = 0; i < NPOINTS; ++i) {
        uint32_t h = _ids[i];
        BOOST_CHECK_EQUAL(dupr::htmId(_points[i], 1), h);
        BOOST_CHECK_EQUAL(dupr::htmId(_points[i], 0), (h >> 2));
    }
}

BOOST_AUTO_TEST_CASE(HtmLevelTest) {
    for (uint32_t i = 0; i < 8; ++i) {
        BOOST_CHECK_EQUAL(dupr::htmLevel(i), -1);
    }
    for (uint32_t i = 8; i < 16; ++i) {
        BOOST_CHECK_EQUAL(dupr::htmLevel(i), 0);
    }
    BOOST_CHECK_EQUAL(dupr::htmLevel(0x80), 2);
    for (int l = 0; l <= dupr::HTM_MAX_LEVEL; ++l) {
        BOOST_CHECK_EQUAL(dupr::htmLevel(0x8 << (2*l)), l);
        BOOST_CHECK_EQUAL(dupr::htmLevel(0x8 << (2*l + 1)), -1);
    }
}

BOOST_AUTO_TEST_CASE(CartesianTest) {
    double const f = 1e-15;
    checkClose(dupr::cartesian( 90,  0), Vector3d( 0, 1, 0), f);
    checkClose(dupr::cartesian(180,  0), Vector3d(-1, 0, 0), f);
    checkClose(dupr::cartesian( 55, 90), Vector3d( 0, 0, 1), f);
    checkClose(dupr::cartesian(999,-90), Vector3d( 0, 0,-1), f);
    checkClose(dupr::cartesian( 45,  0)*2, Vector3d(sqrt(2.), sqrt(2.), 0), f);
    checkClose(dupr::cartesian( 45, 45)*2, Vector3d(1, 1, sqrt(2.)), f);
}

BOOST_AUTO_TEST_CASE(SphericalTest) {
    checkClose(pair<double, double>(45, 45),
               dupr::spherical(1, 1, sqrt(2.)), 1e-15);
    checkClose(pair<double, double>(45, -45),
               dupr::spherical(1, 1, -sqrt(2.)), 1e-15);
}

BOOST_AUTO_TEST_CASE(AngSepTest) {
    double const f = 1e-15;
    BOOST_CHECK_CLOSE(
        dupr::angSep(Vector3d(1,0,0), Vector3d(0,0,1)), 0.5*pi<double>(), f);
    BOOST_CHECK_CLOSE(
        dupr::angSep(Vector3d(1,-1,1), Vector3d(-1,1,-1)), pi<double>(), f);
    BOOST_CHECK_EQUAL(dupr::angSep(Vector3d(1,1,1), Vector3d(1,1,1)), 0);
}

BOOST_AUTO_TEST_CASE(SphericalTriangleTransformTest) {
    double const f = 1e-15;
    Vector3d v;
    dupr::SphericalTriangle s03(S03);
    dupr::SphericalTriangle n13(N13);
    Vector3d s03c( C0, C0,-C0);
    Vector3d n13c(-C0,-C0, C0);
    v = n13.getCartesianTransform() * (s03.getBarycentricTransform() * s03c);
    checkClose(v, n13c, f);
    v = s03.getCartesianTransform() * (n13.getBarycentricTransform() * n13c);
    checkClose(v, s03c, f);
    Matrix3d m = n13.getCartesianTransform() * s03.getBarycentricTransform();
    for (int i = 0; i < 3; ++i) {
        v = m * s03.vertex(i);
        checkClose(v, n13.vertex(i), f);
    }
}

BOOST_AUTO_TEST_CASE(SphericalTriangleAreaTest) {
    double const f = 1e-15;
    dupr::SphericalTriangle t(Vector3d(0,1,0),
                              Vector3d(0,0,1),
                              Vector3d(1,0,0));
    dupr::SphericalTriangle s0(S0);
    dupr::SphericalTriangle s00(S00);
    dupr::SphericalTriangle s01(S01);
    dupr::SphericalTriangle s02(S02);
    dupr::SphericalTriangle s03(S03);
    BOOST_CHECK_CLOSE_FRACTION(t.area(), s0.area(), f);
    BOOST_CHECK_CLOSE_FRACTION(s0.area(), 0.5*pi<double>(), f);
    BOOST_CHECK_CLOSE_FRACTION(s00.area(), s01.area(), f);
    BOOST_CHECK_CLOSE_FRACTION(s01.area(), s02.area(), f);
    BOOST_CHECK_CLOSE_FRACTION(
        s0.area(), s00.area() + s01.area() + s02.area() + s03.area(), f);
}

BOOST_AUTO_TEST_CASE(SphericalBoxTest) {
    dupr::SphericalBox b;
    BOOST_CHECK(b.isFull());
    BOOST_CHECK(!b.isEmpty());
    b = dupr::SphericalBox(-10, 10, 0, 0);
    BOOST_CHECK(b.wraps());
    BOOST_CHECK_EQUAL(b.getLonMin(), 350);
    BOOST_CHECK_EQUAL(b.getLonMax(), 10);
    BOOST_CHECK_EQUAL(b.getLonExtent(), 20);
    b = dupr::SphericalBox(350, 370, -10, 10);
    BOOST_CHECK(b.wraps());
    BOOST_CHECK_EQUAL(b.getLonMin(), 350);
    BOOST_CHECK_EQUAL(b.getLonMax(), 10);
    BOOST_CHECK_EQUAL(b.getLatMin(), -10);
    BOOST_CHECK_EQUAL(b.getLatMax(), 10);
    BOOST_CHECK_EQUAL(b.getLonExtent(), 20);
    b = dupr::SphericalBox(10, 20, 30, 40);
    BOOST_CHECK(!b.wraps());
    BOOST_CHECK_EQUAL(b.getLonExtent(), 10);
    BOOST_CHECK_THROW(dupr::SphericalBox(0,1,1,-1), exception);
    BOOST_CHECK_THROW(dupr::SphericalBox(370,0,0,1), exception);
}

BOOST_AUTO_TEST_CASE(SphericalBoxAreaTest) {
    dupr::SphericalBox b(0, 90, 0, 90);
    BOOST_CHECK_CLOSE_FRACTION(b.area(), 0.5*pi<double>(), 1e-15);
    b = dupr::SphericalBox(135, 180, -90, 90);
    BOOST_CHECK_CLOSE_FRACTION(b.area(), 0.5*pi<double>(), 1e-15);
    b = dupr::SphericalBox(-45, 45, -90, -45);
    BOOST_CHECK_CLOSE_FRACTION(
        b.area(), 0.5*pi<double>()*(1 - 0.5*sqrt(2.)), 1e-15);
}

BOOST_AUTO_TEST_CASE(SphericalBoxExpandTest) {
    dupr::SphericalBox b(10, 20, 80, 85);
    BOOST_CHECK_THROW(b.expand(-1), exception);
    b.expand(0);
    BOOST_CHECK_EQUAL(b.getLonMin(), 10);
    BOOST_CHECK_EQUAL(b.getLonMax(), 20);
    BOOST_CHECK_EQUAL(b.getLatMin(), 80);
    BOOST_CHECK_EQUAL(b.getLatMax(), 85);
    b.expand(6);
    BOOST_CHECK_EQUAL(b.getLonMin(), 0);
    BOOST_CHECK_EQUAL(b.getLonMax(), 360);
    BOOST_CHECK_EQUAL(b.getLatMin(), 74);
    BOOST_CHECK_EQUAL(b.getLatMax(), 90);
    b = dupr::SphericalBox(1,2,-89,89);
    b.expand(2);
    BOOST_CHECK(b.isFull());
    double const lon[2] = { 10, 20 };
    double const lat[2] = { -35, 45 };
    b = dupr::SphericalBox(lon[0], lon[1], lat[0], lat[1]);
    b.expand(10);
    for (int i = 0; i < 2; ++i) {
        for (int j = 0; j < 2; ++j) {
            vector<pair<double, double> > circle = ngon(
                lon[i], lat[j], 10 - dupr::EPSILON_DEG, 360);
            for (size_t k = 0; k < circle.size(); ++k) {
                BOOST_CHECK(b.contains(circle[k]));
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(SphericalBoxContainsTest) {
    dupr::SphericalBox b(10, 20, -1, 1);
    BOOST_CHECK(b.contains(15,0));
    BOOST_CHECK(!b.contains(25,0));
    BOOST_CHECK(!b.contains(5,0));
    BOOST_CHECK(!b.contains(15,2));
    BOOST_CHECK(!b.contains(15,-2));
    b = dupr::SphericalBox(-1, 1, -1, 1);
    BOOST_CHECK(b.contains(359.5,0));
}

BOOST_AUTO_TEST_CASE(SphericalBoxIntersectsTest) {
    dupr::SphericalBox b1(10, 20, -10, 10);
    dupr::SphericalBox b2(-5, 5, -1, 1);
    BOOST_CHECK(!b1.intersects(b2));
    BOOST_CHECK(!b2.intersects(b1));
    b2 = dupr::SphericalBox(20, 21, 10, 11);
    BOOST_CHECK(b1.intersects(b2));
    BOOST_CHECK(b2.intersects(b1));
    b1 = dupr::SphericalBox(-10, 10, 1, 2);
    b2 = dupr::SphericalBox(300, 350, 0, 1);
    BOOST_CHECK(b1.intersects(b2));
    BOOST_CHECK(b2.intersects(b1));
    b2 = dupr::SphericalBox(-1, 1, 3, 4);
    BOOST_CHECK(!b1.intersects(b2));
    BOOST_CHECK(!b2.intersects(b1));
    b1 = dupr::SphericalBox(-10, 10, 3.5, 90);
    BOOST_CHECK(b1.intersects(b2));
    BOOST_CHECK(b2.intersects(b1));
}

BOOST_AUTO_TEST_CASE(SphericalBoxHtmIdsTest) {
    dupr::SphericalBox b(135, 145, 88, 89);
    vector<uint32_t> ids;
    b.htmIds(ids, 5);
    BOOST_CHECK(isSubset(htmIds(b, 5), ids));
    b = dupr::SphericalBox(359, 1, -90, 0);
    ids.clear();
    b.htmIds(ids, 3);
    BOOST_CHECK(isSubset(htmIds(b, 3), ids));
    b = dupr::SphericalBox(1,2,-1,1);
    ids.clear();
    b.htmIds(ids, 7);
    BOOST_CHECK(isSubset(htmIds(b, 7), ids));
}

BOOST_AUTO_TEST_CASE(IntersectionAreaTest) {
    double a = 0.5*pi<double>()*(1 - 0.5*sqrt(2.));
    dupr::SphericalBox b(0, 360, 45, 90);
    dupr::SphericalTriangle t(N0);
    BOOST_CHECK_CLOSE_FRACTION(t.intersectionArea(b), a, 1e-12);
    b = dupr::SphericalBox(0, 360, -90, -45);
    t = dupr::SphericalTriangle(S2);
    BOOST_CHECK_CLOSE_FRACTION(t.intersectionArea(b), a, 1e-12);
    t = tri(0, -90, 20);
    BOOST_CHECK_CLOSE_FRACTION(t.intersectionArea(b), t.area(), 1e-12);
    b = dupr::SphericalBox(10, 190, -90, -89);
    BOOST_CHECK_CLOSE_FRACTION(t.intersectionArea(b), b.area(), 1e-12);
    t = tri(45, 90, 10);
    BOOST_CHECK_EQUAL(t.intersectionArea(b), 0.0);
    b = dupr::SphericalBox(0, 360, 89, 90);
    BOOST_CHECK_CLOSE_FRACTION(t.intersectionArea(b), b.area(), 1e-12);
    b = dupr::SphericalBox(-5, 5, -5, 5);
    t = dupr::SphericalTriangle(dupr::cartesian(1,6),
                                dupr::cartesian(-6,0),
                                dupr::cartesian(1,-6));
    a = t.intersectionArea(b);
    BOOST_CHECK_LT(a, t.area());
    BOOST_CHECK_LT(a, b.area());
    t = tri(0, 90, 30);
    b = dupr::SphericalBox(0, 360, -90, 89);
    a = t.area() - dupr::SphericalBox(0, 360, 89, 90).area();
    BOOST_CHECK_CLOSE_FRACTION(t.intersectionArea(b), a, 1e-12);
    b = dupr::SphericalBox(0, 360, 65, 90);
    a = t.intersectionArea(b);
    BOOST_CHECK_LT(a, t.area());
    BOOST_CHECK_LT(a, b.area());
    // TODO: intersectionArea() cannot handle the case where
    // a triangle is split into disjoint pieces yet - it gets
    // the Euler characteristic woefully wrong, and the area
    // computation explodes.
#if 0
    b = dupr::SphericalBox(0, 360, 50, 65);
    a += t.intersectionArea(b);
    BOOST_CHECK_CLOSE(a, t.area(), 1e-10);
#endif
}
