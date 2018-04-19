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
#include <cstddef>
#include <sstream>

// Third-party headers

// Qserv headers
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

    mysql::MySqlConfig schemaCfg;
    std::shared_ptr<lsst::qserv::css::CssAccess> css;
};

BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)

BOOST_AUTO_TEST_CASE(Factory) {
    TestFactory tf;
    SelectStmt::Ptr stmt = tf.newSimpleStmt();
    QueryContext::Ptr context = tf.newContext(css, schemaCfg);
}

// Helper function to construct a BoolTerm tree from a specification
// string and then render it down.  The input specifiation is RPN,
// and the tree is constructed via a push down list.  The aim here
// was to keep the specification parser as simple as possible...

const std::string RenderedBoolTermFromRPN(const char **rpn)
{
    BoolTerm::PtrVector pdl;
    int opcount;

    for(const char **t=rpn; *t; ++t) {
        if (sscanf(*t, "%d", &opcount)==1) {
            ;
        } else if (!strcmp(*t, "AND")) {
            AndTerm::Ptr andt = std::make_shared<AndTerm>();
            for(int i=0; i<opcount; ++i) {
                andt->_terms.push_back(pdl.front());
                assert(!pdl.empty());
                pdl.erase(pdl.begin());
            }
            pdl.insert(pdl.begin(), andt);
        } else if (!strcmp(*t, "OR")) {
            OrTerm::Ptr ort = std::make_shared<OrTerm>();
            for(int i=0; i<opcount; ++i) {
                ort->_terms.push_back(pdl.front());
                pdl.erase(pdl.begin());
            }
            pdl.insert(pdl.begin(),ort);
        } else {
            PassTerm::Ptr pt = std::make_shared<PassTerm>();
            pt->_text = *t;
            BoolFactor::Ptr bf = std::make_shared<BoolFactor>();
            bf->_terms.push_back(pt);
            pdl.insert(pdl.begin(),bf);
        }
    }

    std::ostringstream str;
    pdl.front()->putStream(str);
    return str.str();
}

BOOST_AUTO_TEST_CASE(BoolTermRenderParens) {

    // AND
    // +-- AND
    // |   +-- A
    // |   +-- B
    // +-- C
    const char *test0[] = {"C", "B", "A", "2", "AND", "2", "AND", nullptr};
    BOOST_CHECK_EQUAL(RenderedBoolTermFromRPN(test0), "A AND B AND C");

    // AND
    // +-- OR
    // |   +-- A
    // |   +-- B
    // +-- C
    const char *test1[] = {"C", "B", "A", "2", "OR", "2", "AND", nullptr};
    BOOST_CHECK_EQUAL(RenderedBoolTermFromRPN(test1), "(A OR B) AND C");

    // OR
    // +-- AND
    // |   +-- A
    // |   +-- B
    // +-- C
    const char *test2[] = {"C", "B", "A", "2", "AND", "2", "OR", nullptr};
    BOOST_CHECK_EQUAL(RenderedBoolTermFromRPN(test2), "A AND B OR C");

    // OR
    // +-- OR
    // |   +-- A
    // |   +-- B
    // +-- C
    const char *test3[] = {"C", "B", "A", "2", "OR", "2", "OR", nullptr};
    BOOST_CHECK_EQUAL(RenderedBoolTermFromRPN(test3), "A OR B OR C");

    // AND
    // +-- A
    // +-- OR
    // |   +-- B
    // |   +-- C
    // |   +-- D
    // +-- E
    const char *test4[] = {"E", "D", "C", "B", "3", "OR", "A", "3", "AND", nullptr};
    BOOST_CHECK_EQUAL(RenderedBoolTermFromRPN(test4), "A AND (B OR C OR D) AND E");

    // OR
    // +-- A
    // +-- AND
    // |   +-- B
    // |   +-- C
    // |   +-- D
    // +-- E
    const char *test5[] = {"E", "D", "C", "B", "3", "AND", "A", "3", "OR", nullptr};
    BOOST_CHECK_EQUAL(RenderedBoolTermFromRPN(test5), "A OR B AND C AND D OR E");

}

BOOST_AUTO_TEST_CASE(DM_737_REGRESSION) {

    // Construct "refObjectId IS NULL OR flags<>2"
    ColumnRef::Ptr cr0 = ColumnRef::newShared("", "", "refObjectId");
    std::shared_ptr<ValueFactor> vf0 = ValueFactor::newColumnRefFactor(cr0);
    std::shared_ptr<ValueExpr> ve0 = ValueExpr::newSimple(vf0);
    NullPredicate::Ptr np0 = std::make_shared<NullPredicate>();
    np0->value = ve0;
    BoolFactor::Ptr bf0 = std::make_shared<BoolFactor>();
    bf0->_terms.push_back(np0);
    ColumnRef::Ptr cr1 = ColumnRef::newShared("", "", "flags");
    std::shared_ptr<ValueFactor> vf1 = ValueFactor::newColumnRefFactor(cr1);
    std::shared_ptr<ValueExpr> ve1 = ValueExpr::newSimple(vf1);
    std::shared_ptr<ValueFactor> vf2 = ValueFactor::newConstFactor("2");
    std::shared_ptr<ValueExpr> ve2 = ValueExpr::newSimple(vf2);
    CompPredicate::Ptr cp0 = std::make_shared<CompPredicate>();
    cp0->left = ve1;
    cp0->op = SqlSQL2Tokens::NOT_EQUALS_OP;
    cp0->right = ve2;
    BoolFactor::Ptr bf1 = std::make_shared<BoolFactor>();
    bf1->_terms.push_back(cp0);
    OrTerm::Ptr ot0 = std::make_shared<OrTerm>();
    ot0->_terms.push_back(bf0);
    ot0->_terms.push_back(bf1);

    // Construct "WHERE foo!=bar AND baz<3.14159"
    ColumnRef::Ptr cr2 = ColumnRef::newShared("", "", "foo");
    std::shared_ptr<ValueFactor> vf3 = ValueFactor::newColumnRefFactor(cr2);
    std::shared_ptr<ValueExpr> ve3 = ValueExpr::newSimple(vf3);
    ColumnRef::Ptr cr3 = ColumnRef::newShared("", "", "bar");
    std::shared_ptr<ValueFactor> vf4 = ValueFactor::newColumnRefFactor(cr3);
    std::shared_ptr<ValueExpr> ve4 = ValueExpr::newSimple(vf4);
    CompPredicate::Ptr cp1 = std::make_shared<CompPredicate>();
    cp1->left = ve3;
    cp1->op = SqlSQL2Tokens::NOT_EQUALS_OP_ALT;
    cp1->right = ve4;
    BoolFactor::Ptr bf2 = std::make_shared<BoolFactor>();
    bf2->_terms.push_back(cp1);
    ColumnRef::Ptr cr4 = ColumnRef::newShared("", "", "baz");
    std::shared_ptr<ValueFactor> vf5 = ValueFactor::newColumnRefFactor(cr4);
    std::shared_ptr<ValueExpr> ve5 = ValueExpr::newSimple(vf5);
    std::shared_ptr<ValueFactor> vf6 = ValueFactor::newConstFactor("3.14159");
    std::shared_ptr<ValueExpr> ve6 = ValueExpr::newSimple(vf6);
    CompPredicate::Ptr cp2 = std::make_shared<CompPredicate>();
    cp2->left = ve5;
    cp2->op = SqlSQL2Tokens::LESS_THAN_OP;
    cp2->right = ve6;
    BoolFactor::Ptr bf3 = std::make_shared<BoolFactor>();
    bf3->_terms.push_back(cp2);
    AndTerm::Ptr at0 = std::make_shared<AndTerm>();
    at0->_terms.push_back(bf2);
    at0->_terms.push_back(bf3);
    std::shared_ptr<WhereClause> wc0 = std::make_shared<WhereClause>();
    wc0->prependAndTerm(at0);

    // Prepend the OR clause onto the WHERE as an additional AND term,
    // render result, and check.  Should have parens around OR clause.
    std::shared_ptr<WhereClause> wc1 = wc0->clone();
    wc1->prependAndTerm(ot0);
    auto str0 = wc1->getGenerated();
    BOOST_CHECK_EQUAL(str0, "(refObjectId IS NULL OR flags<>2) AND foo!=bar AND baz<3.14159");
}

BOOST_AUTO_TEST_SUITE_END()

}}} // lsst::qserv::query
