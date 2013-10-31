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

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Vector
#include "boost/test/unit_test.hpp"

#include "Vector.h"


using lsst::qserv::admin::dupr::Vector3d;
using lsst::qserv::admin::dupr::Matrix3d;


namespace {
    bool operator==(Vector3d const & u, Vector3d const & v) {
        return u(0) == v(0) &&
               u(1) == v(1) &&
               u(2) == v(2);
    }

    bool operator==(Matrix3d const & m, Matrix3d const & n) {
        return m.col(0) == n.col(0) &&
               m.col(1) == n.col(1) &&
               m.col(2) == n.col(2);
    }
}


BOOST_AUTO_TEST_CASE(VectorComponentAccessTest) {
    Vector3d v(1,2,3);
    BOOST_CHECK_EQUAL(v(0), 1);
    BOOST_CHECK_EQUAL(v(1), 2);
    BOOST_CHECK_EQUAL(v(2), 3);
    v(1) *= -1;
    BOOST_CHECK_EQUAL(v(1), -2);
}

BOOST_AUTO_TEST_CASE(VectorDotProductTest) {
    Vector3d x(1,0,0), y(0,1,0), z(0,0,1);
    BOOST_CHECK_EQUAL(x.dot(y), 0);
    BOOST_CHECK_EQUAL(y.dot(z), 0);
    Vector3d u(1,2,3), v(3,.5,2);
    BOOST_CHECK_EQUAL(z.dot(v), v(2));
    BOOST_CHECK_EQUAL(u.dot(v), 10);
}

BOOST_AUTO_TEST_CASE(VectorCrossProductTest) {
    Vector3d x(1,0,0), y(0,1,0), z(0,0,1);
    BOOST_CHECK(x.cross(y) == z);
    BOOST_CHECK(y.cross(z) == x);
    BOOST_CHECK(z.cross(x) == y);
    Vector3d u(1,1,1), v(-2,-.5,-.25);
    BOOST_CHECK(u.cross(v) == -1*v.cross(u));
    BOOST_CHECK(u.cross(u) == Vector3d(0,0,0));
    BOOST_CHECK(u.cross(v) == Vector3d(.25,-1.75,1.5));
}

BOOST_AUTO_TEST_CASE(VectorNormTest) {
    Vector3d nil(0,0,0), x(1,0,0), v(2,3,6);
    BOOST_CHECK_EQUAL(nil.squaredNorm(), 0);
    BOOST_CHECK_EQUAL(nil.norm(), 0);
    BOOST_CHECK_EQUAL(x.norm(), 1);
    BOOST_CHECK_EQUAL(v.squaredNorm(), 49);
    BOOST_CHECK_EQUAL(v.norm(), 7);
    BOOST_CHECK_CLOSE(v.normalized().norm(), 1, 0.0000001);
}

BOOST_AUTO_TEST_CASE(VectorScalarProductTest) {
    BOOST_CHECK(Vector3d(1, 2, -3)*2 == Vector3d(2, 4, -6));
    BOOST_CHECK(0.5*Vector3d(-8, 2, 4) == Vector3d(-4, 1, 2));
}

BOOST_AUTO_TEST_CASE(VectorSumTest) {
    BOOST_CHECK(Vector3d(1, 2, 3) + Vector3d(-3, -2, -1) == Vector3d(-2, 0, 2));
    BOOST_CHECK(Vector3d(4, -1, 3) + Vector3d(-4, 1, -3) == Vector3d(0, 0, 0));
}

BOOST_AUTO_TEST_CASE(VectorDifferenceTest) {
    BOOST_CHECK(Vector3d(1, 2, 3) - Vector3d(-3, -2, -1) == Vector3d(4, 4, 4));
    BOOST_CHECK(Vector3d(4, -1, 3) - Vector3d(1, 2, 3) == Vector3d(3, -3, 0));
}


BOOST_AUTO_TEST_CASE(MatrixComponentAccessTest) {
    Matrix3d m;
    m.col(0) = Vector3d(0,1,2);
    m.col(1) = Vector3d(3,4,5);
    m.col(2) = Vector3d(6,7,8);
    for (int r = 0; r < 2; ++r) {
        for (int c = 0; c < 2; ++c) {
            BOOST_CHECK_EQUAL(m(r,c), r + c*3);
        }
    }
    m(1,1) -= 4;
    BOOST_CHECK_EQUAL(m(1,1), 0);
}

BOOST_AUTO_TEST_CASE(MatrixVectorProductTest) {
    Vector3d v(1,2,3);
    Matrix3d m = Matrix3d::Identity();
    BOOST_CHECK(m*v == v);
    m(0,0) =  1; m(0,1) = -1; m(0,2) =  0;
    m(1,0) =  1; m(1,1) =  1; m(1,2) =  0;
    m(2,0) =  0; m(2,1) =  0; m(2,2) =  1;
    Matrix3d n;
    n(0,0) =  1; n(0,1) =  1; n(0,2) =  0;
    n(1,0) = -1; n(1,1) =  1; n(1,2) =  0;
    n(2,0) =  0; n(2,1) =  0; n(2,2) =  1;
    BOOST_CHECK(n*(m*v) == Vector3d(2,4,3));
}

BOOST_AUTO_TEST_CASE(MatrixMatrixProductTest) {
    Matrix3d m;
    m(0,0) =  1; m(0,1) = -1; m(0,2) =  1;
    m(1,0) =  2; m(1,1) =  1; m(1,2) = -1;
    m(2,0) = -1; m(2,1) =  2; m(2,2) =  3;
    Matrix3d n;
    n(0,0) =  4; n(0,1) =  4; n(0,2) =  4;
    n(1,0) = -1; n(1,1) =  1; n(1,2) =  0;
    n(2,0) =  1; n(2,1) = -1; n(2,2) = -1;
    Matrix3d p = m*n;
    BOOST_CHECK(p.col(0) == Vector3d(6,6,-3));
    BOOST_CHECK(p.col(1) == Vector3d(2,10,-5));
    BOOST_CHECK(p.col(2) == Vector3d(3,9,-7));
}

BOOST_AUTO_TEST_CASE(MatrixInverseTest) {
    Matrix3d n;
    n(0,0) =  4; n(0,1) =  4; n(0,2) =  4;
    n(1,0) = -1; n(1,1) =  1; n(1,2) =  0;
    n(2,0) =  1; n(2,1) = -1; n(2,2) = -1;
    Matrix3d m = n.inverse();
    Matrix3d r;
    r(0,0) = .125; r(0,1) =  0; r(0,2) = .5;
    r(1,0) = .125; r(1,1) =  1; r(1,2) = .5;
    r(2,0) =    0; r(2,1) = -1; r(2,2) = -1;
    BOOST_CHECK(m == r);
    BOOST_CHECK(n*m == Matrix3d::Identity());
    BOOST_CHECK(m*n == Matrix3d::Identity());
}
