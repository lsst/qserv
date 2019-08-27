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
#include "query/BetweenPredicate.h"
#include "query/CompPredicate.h"
#include "query/InPredicate.h"
#include "query/QueryTemplate.h"
#include "query/SecIdxRestrictor.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"

// Boost unit test header
#define BOOST_TEST_MODULE TableRef
#include "boost/test/data/test_case.hpp"
#include "boost/test/included/unit_test.hpp"


using namespace lsst::qserv;
using namespace lsst::qserv::query;
using namespace std;


BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(SecIdxCompRestrictorTestLeft){
    auto restrictor = SecIdxCompRestrictor(
            make_shared<CompPredicate>(ValueExpr::newColumnExpr("db", "tbl", "", "objectId"),
                                       CompPredicate::EQUALS_OP,
                                       ValueExpr::newColumnExpr("123456")),
                                       true);
    BOOST_CHECK_EQUAL(restrictor.sqlFragment(), "db.tbl.objectId=123456");
    BOOST_CHECK_EQUAL(restrictor.getSecIdxLookupQuery("db", "tbl", "chunkColumn", "subChunkColumn"),
                      "SELECT `chunkColumn`, `subChunkColumn` "
                      "FROM `db`.`tbl` "
                      "WHERE objectId=123456");
    auto secIdxColRef = restrictor.getSecIdxColumnRef();
    BOOST_REQUIRE(secIdxColRef != nullptr);
    BOOST_CHECK_EQUAL(*secIdxColRef, ColumnRef("db", "tbl", "", "objectId"));
}


BOOST_AUTO_TEST_CASE(SecIdxCompRestrictorTestRight){
    auto restrictor = SecIdxCompRestrictor(
            make_shared<CompPredicate>(ValueExpr::newColumnExpr("123456"),
                                       CompPredicate::EQUALS_OP,
                                       ValueExpr::newColumnExpr("db", "tbl", "", "objectId")),
                                       false);
    BOOST_CHECK_EQUAL(restrictor.sqlFragment(), "123456=db.tbl.objectId");
    BOOST_CHECK_EQUAL(restrictor.getSecIdxLookupQuery("db", "tbl", "chunkColumn", "subChunkColumn"),
                      "SELECT `chunkColumn`, `subChunkColumn` "
                      "FROM `db`.`tbl` "
                      "WHERE 123456=objectId");
    auto secIdxColRef = restrictor.getSecIdxColumnRef();
    BOOST_REQUIRE(secIdxColRef != nullptr);
    BOOST_CHECK_EQUAL(*secIdxColRef, ColumnRef("db", "tbl", "", "objectId"));
}


BOOST_AUTO_TEST_CASE(SecIdxBetweenRestrictorTest) {
    auto restrictor = SecIdxBetweenRestrictor(
            make_shared<BetweenPredicate>(ValueExpr::newColumnExpr("db", "tbl", "", "objectId"),
                                          ValueExpr::newSimple(ValueFactor::newConstFactor("0")),
                                          ValueExpr::newSimple(ValueFactor::newConstFactor("100000")),
                                          false));
    BOOST_CHECK_EQUAL(restrictor.sqlFragment(), "db.tbl.objectId BETWEEN 0 AND 100000");
    BOOST_CHECK_EQUAL(restrictor.getSecIdxLookupQuery("db", "tbl", "chunkColumn", "subChunkColumn"),
                      "SELECT `chunkColumn`, `subChunkColumn` "
                      "FROM `db`.`tbl` "
                      "WHERE objectId BETWEEN 0 AND 100000");
    auto secIdxColRef = restrictor.getSecIdxColumnRef();
    BOOST_REQUIRE(secIdxColRef != nullptr);
    BOOST_CHECK_EQUAL(*secIdxColRef, ColumnRef("db", "tbl", "", "objectId"));
}


BOOST_AUTO_TEST_CASE(SecIdxInRestrictorTest) {
    vector<shared_ptr<ValueExpr>> candidates = {ValueExpr::newSimple(ValueFactor::newConstFactor("1")),
                                      ValueExpr::newSimple(ValueFactor::newConstFactor("3")),
                                      ValueExpr::newSimple(ValueFactor::newConstFactor("5")),
                                      ValueExpr::newSimple(ValueFactor::newConstFactor("7")),
                                      ValueExpr::newSimple(ValueFactor::newConstFactor("11"))};
    auto restrictor = SecIdxInRestrictor(
            make_shared<InPredicate>(ValueExpr::newColumnExpr("db", "tbl", "", "objectId"),
                                     candidates, false));
    BOOST_CHECK_EQUAL(restrictor.sqlFragment(), "db.tbl.objectId IN(1,3,5,7,11)");
    BOOST_CHECK_EQUAL(restrictor.getSecIdxLookupQuery("db", "tbl", "chunkColumn", "subChunkColumn"),
                      "SELECT `chunkColumn`, `subChunkColumn` "
                      "FROM `db`.`tbl` "
                      "WHERE objectId IN(1,3,5,7,11)");
    auto secIdxColRef = restrictor.getSecIdxColumnRef();
    BOOST_REQUIRE(secIdxColRef != nullptr);
    BOOST_CHECK_EQUAL(*secIdxColRef, ColumnRef("db", "tbl", "", "objectId"));
}


BOOST_AUTO_TEST_SUITE_END()
