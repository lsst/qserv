// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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

// LSST headers
#include "lsst/sphgeom/Box.h"
#include "lsst/sphgeom/Ellipse.h"
#include "lsst/sphgeom/UnitVector3d.h"

// Qserv headers
#include "qproc/geomAdapter.h"
#include "qproc/QueryProcessingError.h"

// Boost unit test header
#define BOOST_TEST_MODULE GeomAdapter
#include "boost/test/included/unit_test.hpp"


namespace test = boost::test_tools;

using namespace lsst::qserv;


BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(arcsecToDegrees) {
    BOOST_CHECK_EQUAL(qproc::arcsecToDegrees(1.), 1./3600.);
}


BOOST_AUTO_TEST_CASE(boxValidParams) {
    auto box = qproc::getBoxFromParams({1.1, 2.1, 3.0, 1.3});
    auto box2 = lsst::sphgeom::Box::fromDegrees(1.1, 2.1, 3.0, 1.3);
    BOOST_REQUIRE(box != nullptr);
    BOOST_CHECK_EQUAL(*box, box2);
}


BOOST_AUTO_TEST_CASE(boxTooFewParameters) {
    BOOST_CHECK_THROW(qproc::getBoxFromParams({1.1, 1.2, 2.1}), qproc::QueryProcessingError);
}


BOOST_AUTO_TEST_CASE(boxTooManyParameters) {
    BOOST_CHECK_THROW(qproc::getBoxFromParams({1.1, 1.2, 2.1, 2.0, 1.3}), qproc::QueryProcessingError);
}


BOOST_AUTO_TEST_CASE(circleValidParams) {
    auto circle = qproc::getCircleFromParams({1.1, 2.1, 3.0});
    lsst::sphgeom::LonLat center = lsst::sphgeom::LonLat::fromDegrees(1.1, 2.1);
    lsst::sphgeom::Angle a = lsst::sphgeom::Angle::fromDegrees(3.0);
    lsst::sphgeom::Circle circle2(lsst::sphgeom::UnitVector3d(center), a);
    BOOST_REQUIRE(circle != nullptr);
    BOOST_CHECK_EQUAL(*circle, circle2);
}


BOOST_AUTO_TEST_CASE(circleTooFewParameters) {
    BOOST_CHECK_THROW(qproc::getCircleFromParams({1.1, 1.2}), qproc::QueryProcessingError);
}


BOOST_AUTO_TEST_CASE(circleTooManyParameters) {
    BOOST_CHECK_THROW(qproc::getCircleFromParams({1.1, 1.2, 2.1, 2.0}), qproc::QueryProcessingError);
}


BOOST_AUTO_TEST_CASE(ellipseValidParams) {
    auto ellipse = qproc::getEllipseFromParams({1.1, 1.2, 2.1, 2.0, 1.3});
    auto ellipse2 = lsst::sphgeom::Ellipse(
            lsst::sphgeom::UnitVector3d(lsst::sphgeom::LonLat::fromDegrees(1.1, 1.2)),
            lsst::sphgeom::Angle::fromDegrees(qproc::arcsecToDegrees(2.1)),
            lsst::sphgeom::Angle::fromDegrees(qproc::arcsecToDegrees(2.0)),
            lsst::sphgeom::Angle::fromDegrees(1.3));
    BOOST_REQUIRE(ellipse != nullptr);
    BOOST_CHECK_EQUAL(*ellipse, ellipse2);
}


BOOST_AUTO_TEST_CASE(ellispeTooFewParameters) {
    BOOST_CHECK_THROW(qproc::getEllipseFromParams({1.1, 1.2, 2.1, 2.0}), qproc::QueryProcessingError);
}


BOOST_AUTO_TEST_CASE(ellispeTooManyParameters) {
    BOOST_CHECK_THROW(qproc::getEllipseFromParams({1.1, 1.2, 2.1, 2.0, 1.3, 1.4}), qproc::QueryProcessingError);
}


BOOST_AUTO_TEST_CASE(convexPolyValidParams) {
    std::vector<double> parameters = {1.1, 1.2, 1.3, 1.4, 1.5, 1.6};
    auto poly = qproc::getConvexPolyFromParams(parameters);
    BOOST_REQUIRE(poly != nullptr);
    std::vector<lsst::sphgeom::UnitVector3d> uv3;
    std::vector<std::pair<double, double>> rawParameters = {
        {1.1, 1.2}, {1.3, 1.4}, {1.5, 1.6}};
    for (auto const& paramPair : rawParameters) {
        lsst::sphgeom::LonLat vx = lsst::sphgeom::LonLat::fromDegrees(paramPair.first, paramPair.second);
        uv3.push_back(lsst::sphgeom::UnitVector3d(vx));
    }
    auto poly2 = lsst::sphgeom::ConvexPolygon(uv3);
    BOOST_CHECK_EQUAL(*poly, poly2);
}


BOOST_AUTO_TEST_CASE(convexPolyTooFewParameters) {
    BOOST_CHECK_THROW(qproc::getConvexPolyFromParams({1., 2., 3., 4., 5.}), qproc::QueryProcessingError);
}


BOOST_AUTO_TEST_CASE(convexPolyOddNumParameters) {
    BOOST_CHECK_THROW(qproc::getConvexPolyFromParams({1., 2., 3., 4., 5., 6., 7.}), qproc::QueryProcessingError);
}


BOOST_AUTO_TEST_SUITE_END()
