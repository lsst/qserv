// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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

// System headers
#include <stdexcept>
#include <string>
#include <vector>

// Qserv headers
#include "qproc/geomAdapter.h"
#include "query/AreaRestrictor.h"
#include "query/BoolFactor.h"
#include "query/QueryTemplate.h"
#include "lsst/sphgeom/Box.h"
#include "lsst/sphgeom/Circle.h"
#include "lsst/sphgeom/Ellipse.h"
#include "lsst/sphgeom/ConvexPolygon.h"

// Boost unit test header
#define BOOST_TEST_MODULE AreaRestrictor
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"


using namespace lsst::qserv;
using namespace lsst::qserv::query;
using namespace std;


BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(BoxRender) {
    auto restrictor = AreaRestrictorBox("1", "2", "3", "4");
    BOOST_CHECK_EQUAL(restrictor.sqlFragment(), "qserv_areaspec_box(1,2,3,4)");
}

BOOST_AUTO_TEST_CASE(CircleRender) {
    auto restrictor = AreaRestrictorCircle("1", "2", "3");
    BOOST_CHECK_EQUAL(restrictor.sqlFragment(), "qserv_areaspec_circle(1,2,3)");
}

BOOST_AUTO_TEST_CASE(EllipseRender) {
    auto restrictor = AreaRestrictorEllipse("1", "2", "3", "4", "5");
    BOOST_CHECK_EQUAL(restrictor.sqlFragment(), "qserv_areaspec_ellipse(1,2,3,4,5)");
}

BOOST_AUTO_TEST_CASE(PolyRender) {
    auto restrictor1 = AreaRestrictorPoly({"1", "2"});
    BOOST_CHECK_EQUAL(restrictor1.sqlFragment(), "qserv_areaspec_poly(1,2)");
    auto restrictor2 = AreaRestrictorPoly({"1", "2", "3", "4"});
    BOOST_CHECK_EQUAL(restrictor2.sqlFragment(), "qserv_areaspec_poly(1,2,3,4)");
    auto restrictor3 = AreaRestrictorPoly({"1", "2", "3", "4", "5", "6"});
    BOOST_CHECK_EQUAL(restrictor3.sqlFragment(), "qserv_areaspec_poly(1,2,3,4,5,6)");
    auto restrictor4 = AreaRestrictorPoly({"1", "2", "3", "4", "5", "6", "7", "8"});
    BOOST_CHECK_EQUAL(restrictor4.sqlFragment(), "qserv_areaspec_poly(1,2,3,4,5,6,7,8)");
}


BOOST_AUTO_TEST_CASE(BoxIncorrectParams) {
    BOOST_CHECK_THROW(AreaRestrictorBox({"1", "2", "3"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorBox({"1", "2", "3", "4", "5"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorBox("1", "2", "3", "a"), invalid_argument);
    vector<string> args({"1", "2", "3", "a"});
    BOOST_CHECK_THROW(auto obj = AreaRestrictorBox(args), invalid_argument);
}


BOOST_AUTO_TEST_CASE(CircleIncorrectParams) {
    BOOST_CHECK_THROW(AreaRestrictorCircle({"1", "2"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorCircle({"1", "2", "3", "4"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorCircle("1", "2", "a"), invalid_argument);
    vector<string> args({"1", "2", "a"});
    BOOST_CHECK_THROW(auto obj = AreaRestrictorCircle(args), invalid_argument);
}


BOOST_AUTO_TEST_CASE(EllipseIncorrectParams) {
    BOOST_CHECK_THROW(AreaRestrictorEllipse({"1", "2", "3", "4"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorEllipse({"1", "2", "3", "4", "5", "6"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorEllipse("a", "2", "3", "4", "5"), invalid_argument);
    vector<string> args({"a", "2", "3", "4", "5"});
    BOOST_CHECK_THROW(auto obj = AreaRestrictorEllipse(args), invalid_argument);
}


BOOST_AUTO_TEST_CASE(PolyIncorrectParams) {
    BOOST_CHECK_THROW(AreaRestrictorPoly({"1"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorPoly({"1", "2", "3"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorPoly({"1", "2", "3", "4", "5"}), logic_error);
    BOOST_CHECK_THROW(AreaRestrictorPoly({"1", "2", "3", "4", "5", "6", "7"}), logic_error);
    vector<string> args({"a", "2", "3", "4", "5", "6"});
    BOOST_CHECK_THROW(auto obj = AreaRestrictorPoly(args), invalid_argument);
}


BOOST_AUTO_TEST_CASE(BoxToSciSql) {
    auto restrictor = AreaRestrictorBox("1", "2", "3", "4");
    auto boolFactor = restrictor.asSciSqlFactor("table",
            make_pair<string, string>("chunkColumn1", "chunkColumn2"));
    BOOST_REQUIRE(boolFactor != nullptr);
    QueryTemplate qt;
    boolFactor->renderTo(qt);
    BOOST_CHECK_EQUAL(qt.sqlFragment(),
                      "scisql_s2PtInBox(`table`.`chunkColumn1`,`table`.`chunkColumn2`,1,2,3,4)=1");
}


BOOST_AUTO_TEST_CASE(CircleToSciSql) {
    auto restrictor = AreaRestrictorCircle("1", "2", "3");
    auto boolFactor = restrictor.asSciSqlFactor("table",
            make_pair<string, string>("chunkColumn1", "chunkColumn2"));
    BOOST_REQUIRE(boolFactor != nullptr);
    QueryTemplate qt;
    boolFactor->renderTo(qt);
    BOOST_CHECK_EQUAL(qt.sqlFragment(),
                      "scisql_s2PtInCircle(`table`.`chunkColumn1`,`table`.`chunkColumn2`,1,2,3)=1");
}


BOOST_AUTO_TEST_CASE(EllipseToSciSql) {
    auto restrictor = AreaRestrictorEllipse("1", "2", "3", "4", "5");
    auto boolFactor = restrictor.asSciSqlFactor("table",
            make_pair<string, string>("chunkColumn1", "chunkColumn2"));
    BOOST_REQUIRE(boolFactor != nullptr);
    QueryTemplate qt;
    boolFactor->renderTo(qt);
    BOOST_CHECK_EQUAL(qt.sqlFragment(),
                      "scisql_s2PtInEllipse(`table`.`chunkColumn1`,`table`.`chunkColumn2`,1,2,3,4,5)=1");
}


BOOST_AUTO_TEST_CASE(PolyToSciSql) {
    auto restrictor = AreaRestrictorPoly({"1", "2", "3", "4", "5", "6", "7", "8"});
    auto boolFactor = restrictor.asSciSqlFactor("table",
            make_pair<string, string>("chunkColumn1", "chunkColumn2"));
    BOOST_REQUIRE(boolFactor != nullptr);
    QueryTemplate qt;
    boolFactor->renderTo(qt);
    BOOST_CHECK_EQUAL(qt.sqlFragment(),
            "scisql_s2PtInCPoly(`table`.`chunkColumn1`,`table`.`chunkColumn2`,1,2,3,4,5,6,7,8)=1");
}


BOOST_AUTO_TEST_CASE(BoxToRegion) {
    auto restrictor = AreaRestrictorBox("1", "2", "3", "4");
    auto region = restrictor.getRegion();
    auto boxRegion = dynamic_pointer_cast<lsst::sphgeom::Box>(region);
    BOOST_REQUIRE(boxRegion != nullptr);
    QueryTemplate qt;
    auto compRegion = qproc::getBoxFromParams({1., 2., 3., 4.});
    BOOST_REQUIRE(compRegion != nullptr);
    BOOST_CHECK_EQUAL(*boxRegion, *compRegion);
}


BOOST_AUTO_TEST_CASE(CircleToRegion) {
    auto restrictor = AreaRestrictorCircle("1", "2", "3");
    auto region = restrictor.getRegion();
    auto circleRegion = dynamic_pointer_cast<lsst::sphgeom::Circle>(region);
    BOOST_REQUIRE(circleRegion != nullptr);
    QueryTemplate qt;
    auto compRegion = qproc::getCircleFromParams({1., 2., 3.});
    BOOST_REQUIRE(compRegion != nullptr);
    BOOST_CHECK_EQUAL(*circleRegion, *compRegion);
}


BOOST_AUTO_TEST_CASE(EllipseToRegion) {
    auto restrictor = AreaRestrictorEllipse("1", "2", "3", "4", "5");
    auto region = restrictor.getRegion();
    auto ellipseRegion = dynamic_pointer_cast<lsst::sphgeom::Ellipse>(region);
    BOOST_REQUIRE(ellipseRegion != nullptr);
    QueryTemplate qt;
    auto compRegion = qproc::getEllipseFromParams({1., 2., 3., 4., 5.});
    BOOST_REQUIRE(compRegion != nullptr);
    BOOST_CHECK_EQUAL(*ellipseRegion, *compRegion);
}


BOOST_AUTO_TEST_CASE(PolyToRegion) {
    auto restrictor = AreaRestrictorPoly({"1", "2", "3", "4", "5", "6", "7", "8"});
    auto region = restrictor.getRegion();
    auto polyRegion = dynamic_pointer_cast<lsst::sphgeom::ConvexPolygon>(region);
    BOOST_REQUIRE(polyRegion != nullptr);
    QueryTemplate qt;
    auto compRegion = qproc::getConvexPolyFromParams({1., 2., 3., 4., 5., 6., 7., 8.});
    BOOST_REQUIRE(compRegion != nullptr);
    BOOST_CHECK_EQUAL(*polyRegion, *compRegion);
}


BOOST_AUTO_TEST_CASE(IsEqual) {
    shared_ptr<AreaRestrictor> restrictor1 = make_shared<AreaRestrictorBox>("1", "2", "3", "4");
    shared_ptr<AreaRestrictor> restrictor2 = make_shared<AreaRestrictorBox>("1", "2", "3", "4");
    vector<string> polyArgs({"1", "2", "3", "4", "5", "6"});
    restrictor2 = make_shared<AreaRestrictorPoly>(polyArgs);

    restrictor1 = make_shared<AreaRestrictorBox>("1", "2", "3", "4");
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);
    restrictor2 = make_shared<AreaRestrictorBox>("1", "2", "3", "4");
    BOOST_CHECK_EQUAL(*restrictor1, *restrictor2);
    restrictor2 = make_shared<AreaRestrictorBox>("1", "2", "3", "5");
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);

    restrictor1 = make_shared<AreaRestrictorCircle>("1", "2", "3");
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);
    restrictor2 = make_shared<AreaRestrictorCircle>("1", "2", "3");
    BOOST_CHECK_EQUAL(*restrictor1, *restrictor2);
    restrictor2 = make_shared<AreaRestrictorCircle>("1", "2", "4");
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);

    restrictor1 = make_shared<AreaRestrictorEllipse>("1", "2", "3", "4", "5");
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);
    restrictor2 = make_shared<AreaRestrictorEllipse>("1", "2", "3", "4", "5");
    BOOST_CHECK_EQUAL(*restrictor1, *restrictor2);
    restrictor2 = make_shared<AreaRestrictorEllipse>("1", "2", "3", "4", "6");
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);

    polyArgs = {"1", "2", "3", "4", "5", "6", "7", "8"};
    restrictor1 = make_shared<AreaRestrictorPoly>(polyArgs);
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);
    restrictor2 = make_shared<AreaRestrictorPoly>(polyArgs);
    BOOST_CHECK_EQUAL(*restrictor1, *restrictor2);
    polyArgs = {"1", "2", "3", "4", "5", "6", "7", "9"};
    restrictor2 = make_shared<AreaRestrictorPoly>(polyArgs);
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);
    polyArgs = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
    restrictor2 = make_shared<AreaRestrictorPoly>(polyArgs);
    BOOST_CHECK_EQUAL(*restrictor1 == *restrictor2, false);
}


BOOST_AUTO_TEST_SUITE_END()
