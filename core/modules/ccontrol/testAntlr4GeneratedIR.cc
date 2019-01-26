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

// System headers
#include <array>
#include <memory>
#include <string>

#define BOOST_TEST_MODULE Antlr4GeneratedIR

// Third-party headers
#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

// Qserv headers
#include "ccontrol/UserQueryType.h"
#include "ccontrol/UserQueryFactory.h"
#include "parser/ParseException.h"
#include "parser/SelectParser.h"
#include "qproc/QuerySession.h"
#include "query/AndTerm.h"
#include "query/BoolFactor.h"
#include "query/BoolFactorTerm.h"
#include "query/BoolTerm.h"
#include "query/BoolTermFactor.h"
#include "query/ColumnRef.h"
#include "query/CompPredicate.h"
#include "query/FromList.h"
#include "query/LikePredicate.h"
#include "query/OrTerm.h"
#include "query/PassTerm.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/SqlSQL2Tokens.h"
#include "query/TableRef.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"


using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv;


BOOST_AUTO_TEST_SUITE(Suite)

/**
 * @brief Pusher is a set of recursive variadic functions to receive a variable number of arguments and push
 *        them onto a vector.
 *        This form is the last to be called when a series of args is being recursively pushed.
 * @tparam T The type contained by the vector.
 * @param vec The vector to push onto.
 * @param v The last object to be pushed.
 */
template <typename T>
void pusher(vector<T>& vec, T v) {
    vec.push_back(v);
}


/**
 * @brief Pusher is a set of recursive variadic functions to receive a variable number of arguments and push
 *        them onto a vector.
 *
 *        This form is used when explicitly decalring the template types when there is exactly one arg.
 *
 * @tparam T The type contained by the vector.
 * @tparam U The type of object being pushed.
 * @param vec The vector to push onto.
 * @param v The last object to be pushed.
 */
template <typename T, typename U>
void pusher(vector<T>& vec, U v) {
    vec.push_back(v);
}


/**
 * @brief Pusher is a set of recursive variadic functions to receive a variable number of arguments and push
 *        them onto a vector.
 *
 * @tparam T The type contained by the vector.
 * @tparam Args The variadic list of objects to push onto the vector.
 * @param vec The vector to push onto.
 * @param first The object to push onto. This will be each next object until there is one left, then the
 *              other form of `pusher` will be called.
 * @param args The rest of the object to be pushed onto the vector.
 */
template <typename T, typename...Args>
void pusher(vector<T>& vec, T first, Args... args) {
    vec.push_back(first);
    pusher(vec, args...);
}


/// Create a new AndTerm, with terms. Args should be a comma separated list of BoolTermPtr.
template <typename...Targs>
shared_ptr<query::AndTerm> AndTerm(Targs... args) {
    vector<shared_ptr<query::BoolTerm>> terms;
    pusher<shared_ptr<query::BoolTerm>, shared_ptr<query::BoolTerm>>(terms, args...);
    return make_shared<query::AndTerm>(terms);
}


/// Create a new AndTerm, with terms. Args should be a comma separated list of BoolFactorPtr.
template <typename...Targs>
shared_ptr<query::BoolFactor> BoolFactor(Targs... args) {
    vector<shared_ptr<query::BoolFactorTerm>> terms;
    pusher<shared_ptr<query::BoolFactorTerm>, shared_ptr<query::BoolFactorTerm>>(terms, args...);
    return make_shared<query::BoolFactor>(terms);
}


/// Create a new BoolTermFactor with a BoolTerm member term.
shared_ptr<query::BoolTermFactor> BoolTermFactor(std::shared_ptr<query::BoolTerm> const& term) {
    return make_shared<query::BoolTermFactor>(term);
}


/// Create a new ColumnRef with given database, table, and column names.
shared_ptr<query::ColumnRef> ColumnRef(string const& db, string const& table, string const& column) {
    return make_shared<query::ColumnRef>(db, table, column);
}


/// Create a new CompPredicate, comparising the `left` and `right` ValueExprPtrs, with an operator, which is
/// an int but the caller should use a constant defined in SqlSQL2Tokens.h
shared_ptr<query::CompPredicate> CompPredicate(shared_ptr<query::ValueExpr> const& left, int op,
        shared_ptr<query::ValueExpr> const& right) {
    return make_shared<query::CompPredicate>(left, op, right);
}


/// Create a new FromList. Args should be a comma separated list of TableRefPtr.
template <typename...Targs>
shared_ptr<query::FromList> FromList(Targs... args) {
    auto tableRefs = make_shared<vector<shared_ptr<query::TableRef>>>();
    pusher<shared_ptr<query::TableRef>, shared_ptr<query::TableRef>>(*tableRefs, args...);
    return make_shared<query::FromList>(tableRefs);
}


/// Create a new LikePredicate with ValueExprPtrs, where `left LIKE right`.
shared_ptr<query::LikePredicate> LikePredicate(shared_ptr<query::ValueExpr> const& left,
        shared_ptr<query::ValueExpr> const& right) {
    return make_shared<query::LikePredicate>(left, right);
}


/// Create a new OrderByClause. Args should be a comma separated list of OrderByTerm object instances (not
/// shared_ptr)
template <typename...Targs>
shared_ptr<query::OrderByClause> OrderByClause(Targs... args) {
    auto orderByTerms = make_shared<vector<query::OrderByTerm>>();
    pusher(*orderByTerms, args...);
    return make_shared<query::OrderByClause>(orderByTerms);
}


/// Create an OrderByTerm with a ValueExprPtr term.
/// Note this does not new an object or create a shared_ptr, as dictated by the OrderByClause interface.
query::OrderByTerm OrderByTerm(shared_ptr<query::ValueExpr> const& term) {
    return query::OrderByTerm(term);
}


/// Create a new OrTerm. Args can be a shared_ptr to any kind of object that inherits from BoolTerm.
template <typename...Targs>
shared_ptr<query::OrTerm> OrTerm(Targs... args) {
    vector<shared_ptr<query::BoolTerm>> terms;
    pusher<shared_ptr<query::BoolTerm>, shared_ptr<query::BoolTerm>>(terms, args...);
    return make_shared<query::OrTerm>(terms);
}


/// Create a new PassTerm with given text.
shared_ptr<query::PassTerm> PassTerm(string const& text) {
    return make_shared<query::PassTerm>(text);
}


/// Create a new SelectList. Args should be a comma separated list of shared_ptr to ValueExpr.
template <typename...Targs>
shared_ptr<query::SelectList> SelectList(Targs... args) {
    auto ptr = make_shared<vector<shared_ptr<query::ValueExpr>>>();
    pusher(*ptr, args...);
    return make_shared<query::SelectList>(ptr);
}


/// Create a new SelectList with the given members.
shared_ptr<query::SelectStmt> SelectStmt(shared_ptr<query::SelectList> const& selectList,
        shared_ptr<query::FromList> const& fromList,
        shared_ptr<query::WhereClause> const& whereClause,
        shared_ptr<query::OrderByClause> const& orderByClause) {
    return make_shared<query::SelectStmt>(selectList, fromList, whereClause, orderByClause);
}


/// Create a new TableRef with the given database, table, and alias names.
shared_ptr<query::TableRef> TableRef(string const& db, string const& table,
        const string& alias) {
    return make_shared<query::TableRef>(db, table, alias);
}


/// Create a new ValueExpr with a ValueFactorPtr.
shared_ptr<query::ValueExpr> ValueExpr(shared_ptr<query::ValueFactor> const& valueFactor) {
    // todo, allow pusher to take a func that returns a T, where we can pass the valueFactor
    // to a new FactorOp ptr, and push that ptr into the vector.
    vector<query::ValueExpr::FactorOp> factorOps = {query::ValueExpr::FactorOp(valueFactor)};
    return make_shared<query::ValueExpr>(factorOps);
}


/// Create a ValueFactor with a COLUMNREF value.
shared_ptr<query::ValueFactor> ValueFactor(shared_ptr<query::ColumnRef> const& columnRef) {
    return make_shared<query::ValueFactor>(columnRef);
}


/// Create a ValueFactor with a CONST value.
shared_ptr<query::ValueFactor> ValueFactor(string const& constVal) {
    return make_shared<query::ValueFactor>(constVal);
}


/// Create a new WhereClause with a given OrTerm for its root term.
shared_ptr<query::WhereClause> WhereClause(shared_ptr<query::OrTerm> const& orTerm) {
    return make_shared<query::WhereClause>(orTerm);
}


struct Antlr4TestQueries {
    Antlr4TestQueries(string const& iQuery, shared_ptr<query::SelectStmt> const& iCompareStmt,
            string const& iSerializedQuery)
    : query(iQuery)
    , compareStmt(iCompareStmt)
    , serializedQuery(iSerializedQuery)
    {}

    /// query to test, that will be turned into a SelectStmt by the andlr4-based parser.
    string query;

    /// comparison query, that will be turned into a SelectStmt by the andlr4-based parser and then that will
    /// be modified by modFunc
    shared_ptr<query::SelectStmt> compareStmt;

    /// the query as it should appear after serialization.
    string serializedQuery;
};

ostream& operator<<(ostream& os, Antlr4TestQueries const& i) {
    os << "Antlr4TestQueries(" << i.query << "...)";
    return os;
}


static const vector<Antlr4TestQueries> ANTLR4_TEST_QUERIES = {
    // tests NOT LIKE (which is 'NOT LIKE', different than 'NOT' and 'LIKE' operators separately)
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName "
        "FROM Science_Ccd_Exposure AS sce "
        "WHERE (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%') "
        "ORDER BY filterId", // case01/queries/0012.1_raftAndCcd.sql
        SelectStmt(
            SelectList(
                ValueExpr(ValueFactor(ColumnRef("", "sce", "filterId"))),
                ValueExpr(ValueFactor(ColumnRef("", "sce", "filterName")))
            ),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(
                    PassTerm("("),
                    BoolTermFactor(OrTerm(AndTerm(BoolFactor(CompPredicate(
                        ValueExpr(ValueFactor(ColumnRef("", "sce", "visit"))),
                        SqlSQL2Tokens::EQUALS_OP,
                        ValueExpr(ValueFactor("887404831"))))))),
                    PassTerm(")")),
                BoolFactor(
                    PassTerm("("),
                    BoolTermFactor(OrTerm(AndTerm(BoolFactor(CompPredicate(
                        ValueExpr(ValueFactor(ColumnRef("", "sce", "raftName"))),
                        SqlSQL2Tokens::EQUALS_OP,
                        ValueExpr(ValueFactor("'3,3'"))))))),
                    PassTerm(")")),
                BoolFactor(
                    PassTerm("("),
                    BoolTermFactor(OrTerm(AndTerm(BoolFactor(LikePredicate(
                        ValueExpr(ValueFactor(ColumnRef("", "sce", "ccdName"))),
                        ValueExpr(ValueFactor("'%'"))))))),
                    PassTerm(")"))
            ))),
            OrderByClause(OrderByTerm(ValueExpr(ValueFactor(ColumnRef("", "", "filterId")))))
        ),

        "SELECT sce.filterId,sce.filterName "
            "FROM Science_Ccd_Exposure AS sce "
            "WHERE (sce.visit=887404831) AND (sce.raftName='3,3') AND (sce.ccdName LIKE '%') "
            "ORDER BY filterId"
    )
};


BOOST_DATA_TEST_CASE(antlr4_test, ANTLR4_TEST_QUERIES, queryInfo) {
    query::SelectStmt::Ptr selectStatement;
    BOOST_REQUIRE_NO_THROW(
        selectStatement = parser::SelectParser::makeSelectStmt(queryInfo.query, parser::SelectParser::ANTLR4));
    BOOST_REQUIRE(selectStatement != nullptr);
    BOOST_TEST_MESSAGE("antlr4 selectStmt structure:" << *selectStatement);
    // verify the selectStatements are the same:
    BOOST_REQUIRE_MESSAGE(*selectStatement == *queryInfo.compareStmt, "generated statement:" << *selectStatement <<
        "does not match stmt:" << *queryInfo.compareStmt);
    // verify the selectStatement converted back to sql is the same as the original query:
    BOOST_REQUIRE_EQUAL(selectStatement->getQueryTemplate().sqlFragment(),
             (queryInfo.serializedQuery != "" ? queryInfo.serializedQuery : queryInfo.query));

}


BOOST_AUTO_TEST_SUITE_END()
