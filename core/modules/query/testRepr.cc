// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
  /**
  *
  * @brief Simple testing for query representation
  *
  */

// System headers
#include <sstream>

// Third-party headers
#include "boost/make_shared.hpp"

// Local headers
#include "query/BoolTerm.h"
#include "query/ColumnRef.h"
#include "query/Predicate.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/SqlSQL2Tokens.h"
#include "query/TestFactory.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"

// Boost unit test header
#define BOOST_TEST_MODULE QueryRepr_1
#include "boost/test/included/unit_test.hpp"

namespace lsst {
namespace qserv {
namespace query {

namespace test = boost::test_tools;

struct TestFixture {
    TestFixture(void) {}
    ~TestFixture(void) {}
};

BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)

BOOST_AUTO_TEST_CASE(Factory) {
    TestFactory tf;
    SelectStmt::Ptr stmt = tf.newStmt();
    QueryContext::Ptr context = tf.newContext();
}

BOOST_AUTO_TEST_CASE(BoolTermRenderParens) {

    PassTerm::Ptr pta = boost::make_shared<PassTerm>();
    pta->_text = "A";
    BoolFactor::Ptr bfa = boost::make_shared<BoolFactor>();
    bfa->_terms.push_back(pta);

    PassTerm::Ptr ptb = boost::make_shared<PassTerm>();
    ptb->_text = "B";
    BoolFactor::Ptr bfb = boost::make_shared<BoolFactor>();
    bfb->_terms.push_back(ptb);

    PassTerm::Ptr ptc = boost::make_shared<PassTerm>();
    ptc->_text = "C";
    BoolFactor::Ptr bfc = boost::make_shared<BoolFactor>();
    bfc->_terms.push_back(ptc);

    // AND
    // +-- AND
    // |   +-- A
    // |   +-- B
    // +-- C
    {
        AndTerm::Ptr t0 = boost::make_shared<AndTerm>();
        t0->_terms.push_back(bfa);
        t0->_terms.push_back(bfb);
        AndTerm::Ptr t1 = boost::make_shared<AndTerm>();
        t1->_terms.push_back(t0);
        t1->_terms.push_back(bfc);
        std::ostringstream str;
        str << *t1;
        BOOST_CHECK_EQUAL(str.str(), "A AND B AND C");
    }

    // AND
    // +-- OR
    // |   +-- A
    // |   +-- B
    // +-- C
    {
        OrTerm::Ptr t0 = boost::make_shared<OrTerm>();
        t0->_terms.push_back(bfa);
        t0->_terms.push_back(bfb);
        AndTerm::Ptr t1 = boost::make_shared<AndTerm>();
        t1->_terms.push_back(t0);
        t1->_terms.push_back(bfc);
        std::ostringstream str;
        str << *t1;
        BOOST_CHECK_EQUAL(str.str(), "(A OR B) AND C");
    }

    // OR
    // +-- AND
    // |   +-- A
    // |   +-- B
    // +-- C
    {
        AndTerm::Ptr t0 = boost::make_shared<AndTerm>();
        t0->_terms.push_back(bfa);
        t0->_terms.push_back(bfb);
        OrTerm::Ptr t1 = boost::make_shared<OrTerm>();
        t1->_terms.push_back(t0);
        t1->_terms.push_back(bfc);
        std::ostringstream str;
        str << *t1;
        BOOST_CHECK_EQUAL(str.str(), "A AND B OR C");
    }

    // OR
    // +-- OR
    // |   +-- A
    // |   +-- B
    // +-- C
    {
        OrTerm::Ptr t0 = boost::make_shared<OrTerm>();
        t0->_terms.push_back(bfa);
        t0->_terms.push_back(bfb);
        OrTerm::Ptr t1 = boost::make_shared<OrTerm>();
        t1->_terms.push_back(t0);
        t1->_terms.push_back(bfc);
        std::ostringstream str;
        str << *t1;
        BOOST_CHECK_EQUAL(str.str(), "A OR B OR C");
    }
}

BOOST_AUTO_TEST_CASE(DM_737_REGRESSION) {

    // Construct "refObjectId IS NULL OR flags<>2"
    ColumnRef::Ptr cr0 = ColumnRef::newShared("", "", "refObjectId");
    boost::shared_ptr<ValueFactor> vf0 = ValueFactor::newColumnRefFactor(cr0);
    boost::shared_ptr<ValueExpr> ve0 = ValueExpr::newSimple(vf0);
    NullPredicate::Ptr np0 = boost::make_shared<NullPredicate>();
    np0->value = ve0;
    BoolFactor::Ptr bf0 = boost::make_shared<BoolFactor>();
    bf0->_terms.push_back(np0);
    ColumnRef::Ptr cr1 = ColumnRef::newShared("", "", "flags");
    boost::shared_ptr<ValueFactor> vf1 = ValueFactor::newColumnRefFactor(cr1);
    boost::shared_ptr<ValueExpr> ve1 = ValueExpr::newSimple(vf1);
    boost::shared_ptr<ValueFactor> vf2 = ValueFactor::newConstFactor("2");
    boost::shared_ptr<ValueExpr> ve2 = ValueExpr::newSimple(vf2);
    CompPredicate::Ptr cp0 = boost::make_shared<CompPredicate>();
    cp0->left = ve1;
    cp0->op = SqlSQL2Tokens::NOT_EQUALS_OP;
    cp0->right = ve2;
    BoolFactor::Ptr bf1 = boost::make_shared<BoolFactor>();
    bf1->_terms.push_back(cp0);
    OrTerm::Ptr ot0 = boost::make_shared<OrTerm>();
    ot0->_terms.push_back(bf0);
    ot0->_terms.push_back(bf1);

    // Construct "WHERE foo!=bar AND baz<3.14159"
    ColumnRef::Ptr cr2 = ColumnRef::newShared("", "", "foo");
    boost::shared_ptr<ValueFactor> vf3 = ValueFactor::newColumnRefFactor(cr2);
    boost::shared_ptr<ValueExpr> ve3 = ValueExpr::newSimple(vf3);
    ColumnRef::Ptr cr3 = ColumnRef::newShared("", "", "bar");
    boost::shared_ptr<ValueFactor> vf4 = ValueFactor::newColumnRefFactor(cr3);
    boost::shared_ptr<ValueExpr> ve4 = ValueExpr::newSimple(vf4);
    CompPredicate::Ptr cp1 = boost::make_shared<CompPredicate>();
    cp1->left = ve3;
    cp1->op = SqlSQL2Tokens::NOT_EQUALS_OP_ALT;
    cp1->right = ve4;
    BoolFactor::Ptr bf2 = boost::make_shared<BoolFactor>();
    bf2->_terms.push_back(cp1);
    ColumnRef::Ptr cr4 = ColumnRef::newShared("", "", "baz");
    boost::shared_ptr<ValueFactor> vf5 = ValueFactor::newColumnRefFactor(cr4);
    boost::shared_ptr<ValueExpr> ve5 = ValueExpr::newSimple(vf5);
    boost::shared_ptr<ValueFactor> vf6 = ValueFactor::newConstFactor("3.14159");
    boost::shared_ptr<ValueExpr> ve6 = ValueExpr::newSimple(vf6);
    CompPredicate::Ptr cp2 = boost::make_shared<CompPredicate>();
    cp2->left = ve5;
    cp2->op = SqlSQL2Tokens::LESS_THAN_OP;
    cp2->right = ve6;
    BoolFactor::Ptr bf3 = boost::make_shared<BoolFactor>();
    bf3->_terms.push_back(cp2);
    AndTerm::Ptr at0 = boost::make_shared<AndTerm>();
    at0->_terms.push_back(bf2);
    at0->_terms.push_back(bf3);
    boost::shared_ptr<WhereClause> wc0 = boost::make_shared<WhereClause>();
    wc0->prependAndTerm(at0);

    // Prepend the OR clause onto the WHERE as an additional AND term,
    // render result, and check.  Should have parens around OR clause.
    boost::shared_ptr<WhereClause> wc1 = wc0->clone();
    wc1->prependAndTerm(ot0);
    std::ostringstream str0;
    str0 << *wc1;
    BOOST_CHECK_EQUAL(str0.str(), "WHERE (refObjectId IS NULL OR flags<>2) AND foo!=bar AND baz<3.14159");
}

BOOST_AUTO_TEST_SUITE_END()

}}} // lsst::qserv::query
