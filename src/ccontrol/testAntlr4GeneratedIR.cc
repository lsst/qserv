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
#include <boost/test/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

// Qserv headers
#include "ccontrol/ParseRunner.h"
#include "ccontrol/UserQuerySet.h"
#include "ccontrol/UserQueryType.h"
#include "parser/ParseException.h"
#include "qproc/QuerySession.h"
#include "query/AndTerm.h"
#include "query/BetweenPredicate.h"
#include "query/BoolFactor.h"
#include "query/BoolFactorTerm.h"
#include "query/BoolTerm.h"
#include "query/BoolTermFactor.h"
#include "query/ColumnRef.h"
#include "query/CompPredicate.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/InPredicate.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/LikePredicate.h"
#include "query/NullPredicate.h"
#include "query/OrTerm.h"
#include "query/PassTerm.h"
#include "query/AreaRestrictor.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/TableRef.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv;

BOOST_AUTO_TEST_SUITE(Suite)

/// Negation is used in class constructors where the class may be negated by 'NOT', where IS_NOT == "NOT",
/// and IS is the explicit absence of "NOT".
enum Negation { IS, IS_NOT };

/// InNotIn is used in the case where something may be specified as 'in' or 'not in' another thing (i.e. for
/// the query::InPredicate.)
enum InNotIn { IN, NOT_IN };

/// Star is used to indicate a star value, i.e. "*" as in "SELECT *"
enum Star { STAR };

/// Natural is used to indicate if a join is natural or not natural, in a JoinRef.
enum Natural { NATURAL, NOT_NATURAL };

/// Between is used to indicate if something is between, or not between.
enum Between { BETWEEN, NOT_BETWEEN };

/// Like is used to indicate is something is like, or not like.
enum Like { LIKE, NOT_LIKE };

/// Null is used to indicate IS_NULL or IS_NOT_NULL
enum IsNull { IS_NULL, IS_NOT_NULL };

/**
 * @brief Pusher is a set of recursive variadic functions to receive a variable number of arguments and push
 *        them into a container that has a push_back function.
 *
 *        This form of `pusher` is the last to get called, for the single remaining element.
 *
 * @tparam Container The type of container
 * @tparam Types The template parameters the container accepts (e.g. for a vector it will be the element type
 *               and the allocator type)
 * @tparam T The type of the object to push into the container.
 * @tparam TArgs The type of the remaining objects to push onto the vector (this should match T)
 * @param container The container to push objects into.
 * @param first The object to push into the container.
 */
template <typename Container, typename T>
void pusher(Container& container, T&& first) {
    container.push_back(forward<T>(first));
}

/**
 * @brief Pusher is a set of recursive variadic functions to receive a variable number of arguments and push
 *        them into a container that has a push_back function.
 *
 * @tparam Container The type of container
 * @tparam Types The template parameters the container accepts (e.g. for a vector it will be the element type
 *               and the allocator type)
 * @tparam T The type of the object to push into the container.
 * @tparam TArgs The type of the remaining objects to push onto the vector (this should match T)
 * @param container The container to push objects into.
 * @param first The object to push into the container.
 * @param args The rest of the objects to push into the container.
 */
template <typename Container, typename T, typename... TArgs>
void pusher(Container& container, T&& first, TArgs&&... args) {
    container.push_back(forward<T>(first));
    pusher(container, forward<TArgs>(args)...);
}

/// Create a new AndTerm, with terms. Args should be a comma separated list of BoolTermPtr.
template <typename... Targs>
shared_ptr<query::AndTerm> AndTerm(Targs... args) {
    vector<shared_ptr<query::BoolTerm>> terms;
    pusher(terms, args...);
    return make_shared<query::AndTerm>(terms);
}

/// Create a new BetweenPredicate.
shared_ptr<query::BetweenPredicate> BetweenPredicate(shared_ptr<query::ValueExpr> const& iValue,
                                                     Between between,
                                                     shared_ptr<query::ValueExpr> const& iMinValue,
                                                     shared_ptr<query::ValueExpr> const& iMaxValue) {
    return make_shared<query::BetweenPredicate>(iValue, iMinValue, iMaxValue, (between == NOT_BETWEEN));
}

/// Create a new AndTerm, with terms. Args should be a comma separated list of BoolFactorPtr.
template <typename... Targs>
shared_ptr<query::BoolFactor> BoolFactor(Negation negation, Targs... args) {
    vector<shared_ptr<query::BoolFactorTerm>> terms;
    pusher(terms, args...);
    return make_shared<query::BoolFactor>(terms, negation);
}

/// Create a new BoolTermFactor with a BoolTerm member term.
shared_ptr<query::BoolTermFactor> BoolTermFactor(shared_ptr<query::BoolTerm> const& term) {
    return make_shared<query::BoolTermFactor>(term);
}

/// Create a new ColumnRef with given database, table, and column names.
shared_ptr<query::ColumnRef> ColumnRef(string const& db, string const& table, string const& column) {
    return make_shared<query::ColumnRef>(db, table, column);
}

/// Create a new ColumnRef with given TableRef and column name.
shared_ptr<query::ColumnRef> ColumnRef(shared_ptr<query::TableRef> const& tableRef, string const& column) {
    return make_shared<query::ColumnRef>(tableRef, column);
}

/// Create a new CompPredicate, comparising the `left` and `right` ValueExprPtrs, with an operator
shared_ptr<query::CompPredicate> CompPredicate(shared_ptr<query::ValueExpr> const& left,
                                               query::CompPredicate::OpType op,
                                               shared_ptr<query::ValueExpr> const& right) {
    return make_shared<query::CompPredicate>(left, op, right);
}

/// Create a FactorOp with a ValueFactor
query::ValueExpr::FactorOp FactorOp(shared_ptr<query::ValueFactor> const& factor, query::ValueExpr::Op op) {
    return query::ValueExpr::FactorOp(factor, op);
}

/// Create a FuncExpr
/// args should be instance of shared_ptr to query::ValueExpr.
template <typename... Targs>
shared_ptr<query::FuncExpr> FuncExpr(string const& name, Targs const&... args) {
    vector<shared_ptr<query::ValueExpr>> valueExprVec;
    pusher(valueExprVec, args...);
    return make_shared<query::FuncExpr>(name, valueExprVec);
}

/// Create a new FromList. Args should be a comma separated list of TableRefPtr.
template <typename... Targs>
shared_ptr<query::FromList> FromList(Targs... args) {
    auto tableRefs = make_shared<vector<shared_ptr<query::TableRef>>>();
    pusher(*tableRefs, args...);
    return make_shared<query::FromList>(tableRefs);
}

// No need to write a factory function for GroupByTerm; its cosntructor is named consistently with the
// factory functions here, and its ultimate owner wants an instance, not a shared_ptr.
// The 'using' statement is placed here to put the function (that is, class constructor) in alphabetical
// order with the other factory functions to make it as obvious as possible where the GroupByTerm function
// is coming from.
using query::GroupByTerm;

/// Create a new GroupByClause. Args should be a comma separated list of GroupByTerm.
template <typename... Targs>
shared_ptr<query::GroupByClause> GroupByClause(Targs... args) {
    auto terms = make_shared<deque<query::GroupByTerm>>();
    pusher(*terms, args...);
    return make_shared<query::GroupByClause>(terms);
}

/// Create a new HavingClause
shared_ptr<query::HavingClause> HavingClause(shared_ptr<query::BoolTerm> const& term) {
    return make_shared<query::HavingClause>(term);
}

/// Create a new InPredicate. Args should be a comma separated list of ValueExpr.
template <typename... Targs>
shared_ptr<query::InPredicate> InPredicate(shared_ptr<query::ValueExpr> const& left, InNotIn in,
                                           Targs const&... args) {
    auto valueExprVec = vector<shared_ptr<query::ValueExpr>>();
    pusher(valueExprVec, args...);
    return make_shared<query::InPredicate>(left, valueExprVec, in == NOT_IN);
}

/// Create a new JoinRef
shared_ptr<query::JoinRef> JoinRef(shared_ptr<query::TableRef> right, query::JoinRef::Type joinType,
                                   Natural natural, shared_ptr<query::JoinSpec> joinSpec) {
    bool isNatural = (NATURAL == natural);
    return make_shared<query::JoinRef>(right, joinType, isNatural, joinSpec);
}

/// Create a new JoinSpec
shared_ptr<query::JoinSpec> JoinSpec(shared_ptr<query::ColumnRef> ref,
                                     shared_ptr<query::BoolTerm> const& onTerm) {
    return make_shared<query::JoinSpec>(ref, onTerm);
}

/// Create a new LikePredicate with ValueExprPtrs, where `left LIKE right`.
shared_ptr<query::LikePredicate> LikePredicate(shared_ptr<query::ValueExpr> const& left, Like like,
                                               shared_ptr<query::ValueExpr> const& right) {
    return make_shared<query::LikePredicate>(left, right, NOT_LIKE == like);
}

/// Create a new NullPredicate
shared_ptr<query::NullPredicate> NullPredicate(shared_ptr<query::ValueExpr> const& valueExpr, IsNull isNull) {
    return make_shared<query::NullPredicate>(valueExpr, IS_NOT_NULL == isNull);
}

/// Create a new OrderByClause. Args should be a comma separated list of OrderByTerm object instances (not
/// shared_ptr)
template <typename... Targs>
shared_ptr<query::OrderByClause> OrderByClause(Targs... args) {
    auto orderByTerms = make_shared<vector<query::OrderByTerm>>();
    pusher(*orderByTerms, args...);
    return make_shared<query::OrderByClause>(orderByTerms);
}

/// Create an OrderByTerm with a ValueExprPtr term.
/// Note this does not new an object or create a shared_ptr, as dictated by the OrderByClause interface.
query::OrderByTerm OrderByTerm(shared_ptr<query::ValueExpr> const& term, query::OrderByTerm::Order order,
                               string collate) {
    return query::OrderByTerm(term, order, collate);
}

/// Create a new OrTerm. Args can be a shared_ptr to any kind of object that inherits from BoolTerm.
template <typename... Targs>
shared_ptr<query::OrTerm> OrTerm(Targs... args) {
    vector<shared_ptr<query::BoolTerm>> terms;
    pusher(terms, args...);
    return make_shared<query::OrTerm>(terms);
}

/// Create a new PassTerm with given text.
shared_ptr<query::PassTerm> PassTerm(string const& text) { return make_shared<query::PassTerm>(text); }

shared_ptr<query::AreaRestrictorBox> AreaRestrictorBox(std::string const& lonMinDegree,
                                                       std::string const& latMinDegree,
                                                       std::string const& lonMaxDegree,
                                                       std::string const& latMaxDegree) {
    return make_shared<query::AreaRestrictorBox>(lonMinDegree, latMinDegree, lonMaxDegree, latMaxDegree);
}

shared_ptr<query::AreaRestrictorCircle> AreaRestrictorCircle(std::string const& centerLonDegree,
                                                             std::string const& centerLatDegree,
                                                             std::string const& radiusDegree) {
    return make_shared<query::AreaRestrictorCircle>(centerLonDegree, centerLatDegree, radiusDegree);
}

shared_ptr<query::AreaRestrictorEllipse> AreaRestrictorEllipse(std::string const& centerLonDegree,
                                                               std::string const& centerLatDegree,
                                                               std::string const& semiMajorAxisAngleArcsec,
                                                               std::string const& semiMinorAxisAngleArcsec,
                                                               std::string const& positionAngleDegree) {
    return make_shared<query::AreaRestrictorEllipse>(centerLonDegree, centerLatDegree,
                                                     semiMajorAxisAngleArcsec, semiMinorAxisAngleArcsec,
                                                     positionAngleDegree);
}

shared_ptr<query::AreaRestrictorPoly> AreaRestrictorPoly(std::vector<std::string> const& parameters) {
    return make_shared<query::AreaRestrictorPoly>(parameters);
}

/// Create a new SelectList. Args should be a comma separated list of shared_ptr to ValueExpr.
template <typename... Targs>
shared_ptr<query::SelectList> SelectList(Targs... args) {
    auto ptr = make_shared<vector<shared_ptr<query::ValueExpr>>>();
    pusher(*ptr, args...);
    return make_shared<query::SelectList>(ptr);
}

/// Create a new SelectList with the given members.
shared_ptr<query::SelectStmt> SelectStmt(shared_ptr<query::SelectList> const& selectList,
                                         shared_ptr<query::FromList> const& fromList,
                                         shared_ptr<query::WhereClause> const& whereClause,
                                         shared_ptr<query::OrderByClause> const& orderByClause,
                                         shared_ptr<query::GroupByClause> const& groupByClause,
                                         shared_ptr<query::HavingClause> const& havingClause,
                                         bool hasDistinct, int limit) {
    return make_shared<query::SelectStmt>(selectList, fromList, whereClause, orderByClause, groupByClause,
                                          havingClause, hasDistinct, limit);
}

/// Create a new TableRef with the given database, table, alias name, and JoinRefs. Args should
/// be a comma separated list of shared_ptr to JoinRef.
template <typename... Targs>
shared_ptr<query::TableRef> TableRef(string const& db, string const& table, const string& alias,
                                     Targs const&... args) {
    vector<shared_ptr<query::JoinRef>> joinRefs;
    pusher(joinRefs, args...);
    auto tableRef = make_shared<query::TableRef>(db, table, alias);
    tableRef->addJoins(joinRefs);
    return tableRef;
}

/// Create a new TableRef with the given database, table, and alias name.
shared_ptr<query::TableRef> TableRef(string const& db, string const& table, const string& alias) {
    return make_shared<query::TableRef>(db, table, alias);
}

/// Create a new ValueExpr with a ValueFactorPtr.
template <typename... Targs>
shared_ptr<query::ValueExpr> ValueExpr(string alias, Targs const&... factorOps) {
    vector<query::ValueExpr::FactorOp> factorOpVec;
    pusher(factorOpVec, factorOps...);
    auto valueExpr = make_shared<query::ValueExpr>(factorOpVec);
    if (!alias.empty()) {
        valueExpr->setAlias(alias);
    }
    return valueExpr;
}

/// Create a ValueFactor with a COLUMNREF value.
shared_ptr<query::ValueFactor> ValueFactor(shared_ptr<query::ColumnRef> const& columnRef) {
    return make_shared<query::ValueFactor>(columnRef);
}

/// Create a ValueFactor with a CONST value.
shared_ptr<query::ValueFactor> ValueFactor(string const& constVal) {
    return make_shared<query::ValueFactor>(constVal);
}

/// Create a ValueFactor with a FUNCTION value.
shared_ptr<query::ValueFactor> ValueFactor(query::ValueFactor::Type type,
                                           shared_ptr<query::FuncExpr> const& funcExpr) {
    if (query::ValueFactor::AGGFUNC == type) {
        return query::ValueFactor::newAggFactor(funcExpr);
    } else if (query::ValueFactor::FUNCTION == type) {
        return query::ValueFactor::newFuncFactor(funcExpr);
    }
    BOOST_REQUIRE_MESSAGE(false, "ValueFactor with a FuncExpr may only be of type FUNCTION or AGGFUNC");
    return nullptr;
}

/// Create a ValueFactor with a STAR value.
shared_ptr<query::ValueFactor> ValueFactor(Star star, string const& table) {
    return query::ValueFactor::newStarFactor(table);
}

/// Create a ValueFactor with a ValueExpr value.
shared_ptr<query::ValueFactor> ValueFactor(shared_ptr<query::ValueExpr> valueExpr) {
    return query::ValueFactor::newExprFactor(valueExpr);
}

/// Create a new WhereClause with a given OrTerm for its root term.
shared_ptr<query::WhereClause> WhereClause(
        shared_ptr<query::OrTerm> const& orTerm,
        shared_ptr<query::AreaRestrictor> const& areaRestrictor = nullptr) {
    auto restrictorVec = make_shared<query::AreaRestrictorVec>();
    if (nullptr != areaRestrictor) {
        restrictorVec->push_back(areaRestrictor);
    }
    return make_shared<query::WhereClause>(orTerm, restrictorVec);
}

/**
 * @brief holds related test data.
 *
 */
struct Antlr4TestQueries {
    /**
     * @brief Construct a new Antlr 4 Test Queries object
     *
     * @param iQuery The sql to parse and get generated IR.
     * @param iCompareStmt a function that creates IR that should be equivalent to the parser-generated IR.
     * @param iSerializedQuery The SQL string that should exactly match the string generated by serializing
     *                         the IR.
     */
    Antlr4TestQueries(string const& iQuery, function<shared_ptr<query::SelectStmt>()> const& iCompareStmt,
                      string const& iSerializedQuery)
            : query(iQuery), compareStmt(iCompareStmt), serializedQuery(iSerializedQuery) {}

    /// query to test, that will be turned into a SelectStmt by the andlr4-based parser.
    string query;

    /// comparison query, that will be turned into a SelectStmt by the andlr4-based parser and then that will
    /// be modified by modFunc
    function<shared_ptr<query::SelectStmt>()> compareStmt;

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
                "ORDER BY filterId",  // case01/queries/0012.1_raftAndCcd.sql
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
                            WhereClause(OrTerm(AndTerm(
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "sce", "visit")),
                                                                               query::ValueExpr::NONE)),
                                                            query::CompPredicate::EQUALS_OP,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("887404831"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")")),
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                           "", "sce",
                                                                                           "raftName")),
                                                                                   query::ValueExpr::NONE)),
                                                            query::CompPredicate::EQUALS_OP,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("'3,3'"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")")),
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    LikePredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "sce", "ccdName")),
                                                                               query::ValueExpr::NONE)),
                                                            LIKE,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("'%'"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")"))))),
                            OrderByClause(OrderByTerm(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")),
                                                           query::ValueExpr::NONE)),
                                    query::OrderByTerm::DEFAULT, "")),
                            nullptr,  // Group By Clause
                            nullptr, 0, -1);
                },
                "SELECT `sce`.`filterId`,`sce`.`filterName` "
                "FROM `Science_Ccd_Exposure` AS `sce` "
                "WHERE (`sce`.`visit`=887404831) AND (`sce`.`raftName`='3,3') AND (`sce`.`ccdName` LIKE '%') "
                "ORDER BY `filterId`"),

        // tests a query with 2 items in the GROUP BY expression
        Antlr4TestQueries(
                "SELECT objectId, filterId FROM Source GROUP BY objectId, filterId;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Source", "")),
                            nullptr,  // WhereClause
                            nullptr,  // OrderByClause
                            GroupByClause(
                                    GroupByTerm(
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                                   query::ValueExpr::NONE)),
                                            ""),
                                    GroupByTerm(
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")),
                                                                   query::ValueExpr::NONE)),
                                            "")),
                            nullptr, 0, -1);
                },
                "SELECT `objectId`,`filterId` FROM `Source` GROUP BY `objectId`,`filterId`"),
        // test SELECT MAX...
        Antlr4TestQueries(
                "select max(filterID) from Filter",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr(
                                    "",
                                    FactorOp(ValueFactor(
                                                     query::ValueFactor::AGGFUNC,
                                                     FuncExpr("max",
                                                              ValueExpr("",
                                                                        FactorOp(ValueFactor(ColumnRef(
                                                                                         "", "", "filterID")),
                                                                                 query::ValueExpr::NONE)))),
                                             query::ValueExpr::NONE))),
                            FromList(TableRef("", "Filter", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT max(`filterID`) FROM `Filter`"),
        // test SELECT MIN...
        Antlr4TestQueries(
                "select min(filterID) from Filter",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr(
                                    "",
                                    FactorOp(ValueFactor(
                                                     query::ValueFactor::AGGFUNC,
                                                     FuncExpr("min",
                                                              ValueExpr("",
                                                                        FactorOp(ValueFactor(ColumnRef(
                                                                                         "", "", "filterID")),
                                                                                 query::ValueExpr::NONE)))),
                                             query::ValueExpr::NONE))),
                            FromList(TableRef("", "Filter", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT min(`filterID`) FROM `Filter`"),
        // test WHERE a = b
        Antlr4TestQueries(
                "SELECT objectId,iauId,ra_PS FROM Object WHERE objectId = 430213989148129",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iauId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::EQUALS_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("430213989148129"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId`,`iauId`,`ra_PS` FROM `Object` WHERE `objectId`=430213989148129"),
        // test WHERE a IN (...)
        Antlr4TestQueries(
                "select ra_Ps, decl_PS FROM Object WHERE objectId IN (390034570102582, 396210733076852, "
                "393126946553816, 390030275138483)",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_Ps")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "",
                                                                                                 "objectId")),
                                                                           query::ValueExpr::NONE)),
                                                    IN,
                                                    ValueExpr("", FactorOp(ValueFactor("390034570102582"),
                                                                           query::ValueExpr::NONE)),
                                                    ValueExpr("", FactorOp(ValueFactor("396210733076852"),
                                                                           query::ValueExpr::NONE)),
                                                    ValueExpr("", FactorOp(ValueFactor("393126946553816"),
                                                                           query::ValueExpr::NONE)),
                                                    ValueExpr("", FactorOp(ValueFactor("390030275138483"),
                                                                           query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `ra_Ps`,`decl_PS` FROM `Object` WHERE `objectId` "
                "IN(390034570102582,396210733076852,393126946553816,390030275138483)"),
        // test SELECT *
        Antlr4TestQueries(
                "SELECT * FROM Object WHERE objectId = 430213989000",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::EQUALS_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("430213989000"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT * FROM `Object` WHERE `objectId`=430213989000"),
        // test SELECT a.b
        // test JOIN tablename tablealias
        // test USING (a)
        // test WHERE a.b ...
        Antlr4TestQueries(
                "SELECT s.ra, s.decl, o.raRange, o.declRange FROM Object o JOIN Source s USING (objectId) "
                "WHERE o.objectId = 390034570102582 AND o.latestObsTime = s.taiMidPoint",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "raRange")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "declRange")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef(
                                    "", "Object", "o",
                                    JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL,
                                            JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
                            WhereClause(OrTerm(AndTerm(
                                    BoolFactor(IS,
                                               CompPredicate(
                                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                      "", "o", "objectId")),
                                                                              query::ValueExpr::NONE)),
                                                       query::CompPredicate::EQUALS_OP,
                                                       ValueExpr("", FactorOp(ValueFactor("390034570102582"),
                                                                              query::ValueExpr::NONE)))),
                                    BoolFactor(
                                            IS,
                                            CompPredicate(
                                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                   "", "o", "latestObsTime")),
                                                                           query::ValueExpr::NONE)),
                                                    query::CompPredicate::EQUALS_OP,
                                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                   "", "s", "taiMidPoint")),
                                                                           query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `s`.`ra`,`s`.`decl`,`o`.`raRange`,`o`.`declRange` FROM `Object` AS `o` JOIN `Source` "
                "AS `s` USING(`objectId`) "
                "WHERE `o`.`objectId`=390034570102582 AND `o`.`latestObsTime`=`s`.`taiMidPoint`"),
        // test ORDER BY
        Antlr4TestQueries(
                "SELECT sourceId, objectId FROM Source WHERE objectId = 386942193651348 ORDER BY sourceId;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Source", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::EQUALS_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("386942193651348"),
                                                                             query::ValueExpr::NONE))))))),
                            OrderByClause(OrderByTerm(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")),
                                                           query::ValueExpr::NONE)),
                                    query::OrderByTerm::DEFAULT, "")),
                            nullptr, nullptr, 0, -1);
                },
                "SELECT `sourceId`,`objectId` FROM `Source` WHERE `objectId`=386942193651348 ORDER BY "
                "`sourceId`"),
        // test COUNT(*) AS alias
        Antlr4TestQueries(
                "select COUNT(*) AS N FROM Source WHERE objectId IN (386950783579546, 386942193651348)",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr(
                                    "N",
                                    FactorOp(ValueFactor(
                                                     query::ValueFactor::AGGFUNC,
                                                     FuncExpr("COUNT",
                                                              ValueExpr("",
                                                                        FactorOp(ValueFactor(STAR, ""),
                                                                                 query::ValueExpr::NONE)))),
                                             query::ValueExpr::NONE))),
                            FromList(TableRef("", "Source", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "",
                                                                                                 "objectId")),
                                                                           query::ValueExpr::NONE)),
                                                    IN,
                                                    ValueExpr("", FactorOp(ValueFactor("386950783579546"),
                                                                           query::ValueExpr::NONE)),
                                                    ValueExpr("", FactorOp(ValueFactor("386942193651348"),
                                                                           query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT COUNT(*) AS `N` FROM `Source` WHERE `objectId` IN(386950783579546,386942193651348)"),
        // test LIKE
        // test WHERE a and b
        Antlr4TestQueries(
                "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce "
                "WHERE  (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%') ORDER "
                "BY filterId",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
                            WhereClause(OrTerm(AndTerm(
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "sce", "visit")),
                                                                               query::ValueExpr::NONE)),
                                                            query::CompPredicate::EQUALS_OP,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("887404831"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")")),
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                           "", "sce",
                                                                                           "raftName")),
                                                                                   query::ValueExpr::NONE)),
                                                            query::CompPredicate::EQUALS_OP,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("'3,3'"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")")),
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    LikePredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "sce", "ccdName")),
                                                                               query::ValueExpr::NONE)),
                                                            LIKE,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("'%'"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")"))))),
                            OrderByClause(OrderByTerm(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")),
                                                           query::ValueExpr::NONE)),
                                    query::OrderByTerm::DEFAULT, "")),
                            nullptr, nullptr, 0, -1);
                },
                "SELECT `sce`.`filterId`,`sce`.`filterName` FROM `Science_Ccd_Exposure` AS `sce` "
                "WHERE (`sce`.`visit`=887404831) AND (`sce`.`raftName`='3,3') AND (`sce`.`ccdName` LIKE '%') "
                "ORDER BY `filterId`"),
        // test LIMIT
        Antlr4TestQueries(
                "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce "
                "WHERE  (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%') ORDER "
                "BY filterId LIMIT 5",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
                            WhereClause(OrTerm(AndTerm(
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "sce", "visit")),
                                                                               query::ValueExpr::NONE)),
                                                            query::CompPredicate::EQUALS_OP,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("887404831"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")")),
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                           "", "sce",
                                                                                           "raftName")),
                                                                                   query::ValueExpr::NONE)),
                                                            query::CompPredicate::EQUALS_OP,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("'3,3'"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")")),
                                    BoolFactor(
                                            IS, PassTerm("("),
                                            BoolTermFactor(OrTerm(AndTerm(BoolFactor(
                                                    IS,
                                                    LikePredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "sce", "ccdName")),
                                                                               query::ValueExpr::NONE)),
                                                            LIKE,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("'%'"),
                                                                               query::ValueExpr::NONE))))))),
                                            PassTerm(")"))))),
                            OrderByClause(OrderByTerm(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")),
                                                           query::ValueExpr::NONE)),
                                    query::OrderByTerm::DEFAULT, "")),
                            nullptr, nullptr, 0, 5);
                },
                "SELECT `sce`.`filterId`,`sce`.`filterName` "
                "FROM `Science_Ccd_Exposure` AS `sce` "
                "WHERE (`sce`.`visit`=887404831) AND (`sce`.`raftName`='3,3') AND (`sce`.`ccdName` LIKE '%') "
                "ORDER BY `filterId` LIMIT 5"),
        // test qserv_areaspec_box
        // test scisql UDF
        // test BETWEEN a and b
        Antlr4TestQueries(
                "SELECT COUNT(*) as OBJ_COUNT FROM   Object "
                "WHERE qserv_areaspec_box(0.1, -6, 4, 6) "
                "AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 "
                "AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 "
                "AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr(
                                    "OBJ_COUNT",
                                    FactorOp(ValueFactor(
                                                     query::ValueFactor::AGGFUNC,
                                                     FuncExpr("COUNT",
                                                              ValueExpr("",
                                                                        FactorOp(ValueFactor(STAR, ""),
                                                                                 query::ValueExpr::NONE)))),
                                             query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(
                                    OrTerm(AndTerm(
                                            BoolFactor(
                                                    IS,
                                                    BetweenPredicate(
                                                            ValueExpr(
                                                                    "",
                                                                    FactorOp(
                                                                            ValueFactor(
                                                                                    query::ValueFactor::
                                                                                            FUNCTION,
                                                                                    FuncExpr(
                                                                                            "scisql_"
                                                                                            "fluxToAbMag",
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "",
                                                                                                                    "zFlux_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)))),
                                                                            query::ValueExpr::NONE)),
                                                            BETWEEN,
                                                            ValueExpr("", FactorOp(ValueFactor("20"),
                                                                                   query::ValueExpr::NONE)),
                                                            ValueExpr("", FactorOp(ValueFactor("24"),
                                                                                   query::ValueExpr::NONE)))),
                                            BoolFactor(
                                                    IS,
                                                    BetweenPredicate(
                                                            ValueExpr(
                                                                    "",
                                                                    FactorOp(
                                                                            ValueFactor(
                                                                                    query::ValueFactor::
                                                                                            FUNCTION,
                                                                                    FuncExpr(
                                                                                            "scisql_"
                                                                                            "fluxToAbMag",
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "",
                                                                                                                    "gFlux_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)))),
                                                                            query::ValueExpr::MINUS),
                                                                    FactorOp(
                                                                            ValueFactor(
                                                                                    query::ValueFactor::
                                                                                            FUNCTION,
                                                                                    FuncExpr(
                                                                                            "scisql_"
                                                                                            "fluxToAbMag",
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "",
                                                                                                                    "rFlux_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)))),
                                                                            query::ValueExpr::NONE)),
                                                            BETWEEN,
                                                            ValueExpr("", FactorOp(ValueFactor("0.1"),
                                                                                   query::ValueExpr::NONE)),
                                                            ValueExpr("", FactorOp(ValueFactor("0.9"),
                                                                                   query::ValueExpr::NONE)))),
                                            BoolFactor(
                                                    IS,
                                                    BetweenPredicate(
                                                            ValueExpr(
                                                                    "",
                                                                    FactorOp(
                                                                            ValueFactor(
                                                                                    query::ValueFactor::
                                                                                            FUNCTION,
                                                                                    FuncExpr(
                                                                                            "scisql_"
                                                                                            "fluxToAbMag",
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "",
                                                                                                                    "iFlux_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)))),
                                                                            query::ValueExpr::MINUS),
                                                                    FactorOp(
                                                                            ValueFactor(
                                                                                    query::ValueFactor::
                                                                                            FUNCTION,
                                                                                    FuncExpr(
                                                                                            "scisql_"
                                                                                            "fluxToAbMag",
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "",
                                                                                                                    "zFlux_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)))),
                                                                            query::ValueExpr::NONE)),
                                                            BETWEEN,
                                                            ValueExpr("", FactorOp(ValueFactor("0.1"),
                                                                                   query::ValueExpr::NONE)),
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("1.0"),
                                                                               query::ValueExpr::NONE)))))),
                                    AreaRestrictorBox("0.1", "-6", "4", "6")),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT COUNT(*) AS `OBJ_COUNT` "
                "FROM `Object` WHERE qserv_areaspec_box(0.1,-6,4,6) scisql_fluxToAbMag(`zFlux_PS`) BETWEEN "
                "20 AND 24 "
                "AND (scisql_fluxToAbMag(`gFlux_PS`)-scisql_fluxToAbMag(`rFlux_PS`)) BETWEEN 0.1 AND 0.9 "
                "AND (scisql_fluxToAbMag(`iFlux_PS`)-scisql_fluxToAbMag(`zFlux_PS`)) BETWEEN 0.1 AND 1.0"),
        // test AVG
        Antlr4TestQueries(
                "SELECT objectId, AVG(ra_PS) as ra FROM Object WHERE qserv_areaspec_box(0, 0, 3, 10) GROUP "
                "BY objectId ORDER BY ra",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                           query::ValueExpr::NONE)),
                                    ValueExpr(
                                            "ra",
                                            FactorOp(
                                                    ValueFactor(
                                                            query::ValueFactor::AGGFUNC,
                                                            FuncExpr("AVG",
                                                                     ValueExpr("",
                                                                               FactorOp(ValueFactor(ColumnRef(
                                                                                                "", "",
                                                                                                "ra_PS")),
                                                                                        query::ValueExpr::
                                                                                                NONE)))),
                                                    query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(nullptr, AreaRestrictorBox("0", "0", "3", "10")),
                            OrderByClause(
                                    OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra")),
                                                                       query::ValueExpr::NONE)),
                                                query::OrderByTerm::DEFAULT, "")),
                            GroupByClause(GroupByTerm(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                           query::ValueExpr::NONE)),
                                    "")),
                            nullptr, 0, -1);
                },
                "SELECT `objectId`,AVG(`ra_PS`) AS `ra` FROM `Object` WHERE qserv_areaspec_box(0,0,3,10) "
                "GROUP BY `objectId` ORDER BY `ra`"),
        // test multiple JOIN
        // test ASC
        Antlr4TestQueries(
                "SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) "
                "FROM Source JOIN Object USING(objectId) JOIN Filter USING(filterId) "
                "WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' ORDER BY objectId, "
                "taiMidPoint ASC",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                           query::ValueExpr::NONE)),
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "taiMidPoint")),
                                                           query::ValueExpr::NONE)),
                                    ValueExpr(
                                            "",
                                            FactorOp(
                                                    ValueFactor(
                                                            query::ValueFactor::FUNCTION,
                                                            FuncExpr("scisql_fluxToAbMag",
                                                                     ValueExpr("",
                                                                               FactorOp(ValueFactor(ColumnRef(
                                                                                                "", "",
                                                                                                "psfFlux")),
                                                                                        query::ValueExpr::
                                                                                                NONE)))),
                                                    query::ValueExpr::NONE))),
                            FromList(TableRef(
                                    "", "Source", "",
                                    JoinRef(TableRef("", "Object", ""), query::JoinRef::DEFAULT, NOT_NATURAL,
                                            JoinSpec(ColumnRef("", "", "objectId"), nullptr)),
                                    JoinRef(TableRef("", "Filter", ""), query::JoinRef::DEFAULT, NOT_NATURAL,
                                            JoinSpec(ColumnRef("", "", "filterId"), nullptr)))),
                            WhereClause(
                                    OrTerm(AndTerm(BoolFactor(
                                            IS, CompPredicate(
                                                        ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "", "filterName")),
                                                                               query::ValueExpr::NONE)),
                                                        query::CompPredicate::EQUALS_OP,
                                                        ValueExpr("", FactorOp(ValueFactor("'g'"),
                                                                               query::ValueExpr::NONE)))))),
                                    AreaRestrictorBox("355", "0", "360", "20")),
                            OrderByClause(
                                    OrderByTerm(
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                                   query::ValueExpr::NONE)),
                                            query::OrderByTerm::DEFAULT, ""),
                                    OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "",
                                                                                             "taiMidPoint")),
                                                                       query::ValueExpr::NONE)),
                                                query::OrderByTerm::ASC, "")),
                            nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId`,`taiMidPoint`,scisql_fluxToAbMag(`psfFlux`) "
                "FROM `Source` JOIN `Object` USING(`objectId`) JOIN `Filter` USING(`filterId`) WHERE "
                "qserv_areaspec_box(355,0,360,20)`filterName`='g' ORDER BY `objectId`, `taiMidPoint` ASC"),
        // test hex
        Antlr4TestQueries(
                "SELECT scienceCcdExposureId, hex(poly) as hexPoly FROM Science_Ccd_Exposure;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("",
                                              FactorOp(ValueFactor(ColumnRef("", "", "scienceCcdExposureId")),
                                                       query::ValueExpr::NONE)),
                                    ValueExpr(
                                            "hexPoly",
                                            FactorOp(
                                                    ValueFactor(
                                                            query::ValueFactor::FUNCTION,
                                                            FuncExpr("hex",
                                                                     ValueExpr("",
                                                                               FactorOp(ValueFactor(ColumnRef(
                                                                                                "", "",
                                                                                                "poly")),
                                                                                        query::ValueExpr::
                                                                                                NONE)))),
                                                    query::ValueExpr::NONE))),
                            FromList(TableRef("", "Science_Ccd_Exposure", "")), nullptr, nullptr, nullptr,
                            nullptr, 0, -1);
                },
                "SELECT `scienceCcdExposureId`,hex(`poly`) AS `hexPoly` FROM `Science_Ccd_Exposure`"),
        // test case insensitivity
        Antlr4TestQueries(
                "SELECT objectId FROM   Object WHERE QsErV_ArEaSpEc_BoX(0, 0, 3, 10) ORDER BY objectId",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(nullptr, AreaRestrictorBox("0", "0", "3", "10")),
                            OrderByClause(OrderByTerm(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                           query::ValueExpr::NONE)),
                                    query::OrderByTerm::DEFAULT, "")),
                            nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE qserv_areaspec_box(0,0,3,10) ORDER BY `objectId`"),
        // test null-safe equals operator <>
        Antlr4TestQueries(
                "SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, "
                "o2.ra_PS, o2.decl_PS) AS distance FROM Object o1, Object o2 WHERE qserv_areaspec_box(1.2, "
                "3.3, 1.3, 3.4) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.016 AND "
                "o1.objectId <> o2.objectId",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("objId1", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")),
                                                                 query::ValueExpr::NONE)),
                                    ValueExpr("objId2", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")),
                                                                 query::ValueExpr::NONE)),
                                    ValueExpr(
                                            "distance",
                                            FactorOp(
                                                    ValueFactor(
                                                            query::ValueFactor::FUNCTION,
                                                            FuncExpr(
                                                                    "scisql_angSep",
                                                                    ValueExpr(
                                                                            "",
                                                                            FactorOp(ValueFactor(ColumnRef(
                                                                                             "", "o1",
                                                                                             "ra_PS")),
                                                                                     query::ValueExpr::NONE)),
                                                                    ValueExpr(
                                                                            "",
                                                                            FactorOp(ValueFactor(ColumnRef(
                                                                                             "", "o1",
                                                                                             "decl_PS")),
                                                                                     query::ValueExpr::NONE)),
                                                                    ValueExpr(
                                                                            "",
                                                                            FactorOp(ValueFactor(ColumnRef(
                                                                                             "", "o2",
                                                                                             "ra_PS")),
                                                                                     query::ValueExpr::NONE)),
                                                                    ValueExpr("",
                                                                              FactorOp(ValueFactor(ColumnRef(
                                                                                               "", "o2",
                                                                                               "decl_PS")),
                                                                                       query::ValueExpr::
                                                                                               NONE)))),
                                                    query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
                            WhereClause(
                                    OrTerm(AndTerm(
                                            BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr(
                                                                    "",
                                                                    FactorOp(
                                                                            ValueFactor(
                                                                                    query::ValueFactor::
                                                                                            FUNCTION,
                                                                                    FuncExpr(
                                                                                            "scisql_angSep",
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "o1",
                                                                                                                    "ra_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)),
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "o1",
                                                                                                                    "decl_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)),
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "o2",
                                                                                                                    "ra_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)),
                                                                                            ValueExpr(
                                                                                                    "",
                                                                                                    FactorOp(
                                                                                                            ValueFactor(ColumnRef(
                                                                                                                    "",
                                                                                                                    "o2",
                                                                                                                    "decl_PS")),
                                                                                                            query::ValueExpr::
                                                                                                                    NONE)))),
                                                                            query::ValueExpr::NONE)),
                                                            query::CompPredicate::LESS_THAN_OP,
                                                            ValueExpr("", FactorOp(ValueFactor("0.016"),
                                                                                   query::ValueExpr::NONE)))),
                                            BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "o1", "objectId")),
                                                                               query::ValueExpr::NONE)),
                                                            query::CompPredicate::NOT_EQUALS_OP,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "o2", "objectId")),
                                                                               query::ValueExpr::NONE)))))),
                                    AreaRestrictorBox("1.2", "3.3", "1.3", "3.4")),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `o1`.`objectId` AS `objId1`,`o2`.`objectId` AS "
                "`objId2`,scisql_angSep(`o1`.`ra_PS`,`o1`.`decl_PS`,`o2`.`ra_PS`,`o2`.`decl_PS`) AS "
                "`distance` "
                "FROM `Object` AS `o1`,`Object` AS `o2` WHERE qserv_areaspec_box(1.2,3.3,1.3,3.4) "
                "scisql_angSep(`o1`.`ra_PS`,`o1`.`decl_PS`,`o2`.`ra_PS`,`o2`.`decl_PS`)<0.016 "
                "AND `o1`.`objectId`<>`o2`.`objectId`"),
        // test less-than operator
        Antlr4TestQueries(
                "SELECT  objectId FROM    Object WHERE   "
                "scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS) <  2.0 AND  "
                "scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) <  0.1 AND  "
                "scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) > -0.8 AND  "
                "scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) <  1.4",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(
                                    BoolFactor(
                                            IS,
                                            CompPredicate(
                                                    ValueExpr(
                                                            "",
                                                            FactorOp(
                                                                    ValueFactor(
                                                                            query::ValueFactor::FUNCTION,
                                                                            FuncExpr(
                                                                                    "scisql_fluxToAbMag",
                                                                                    ValueExpr(
                                                                                            "",
                                                                                            FactorOp(
                                                                                                    ValueFactor(ColumnRef(
                                                                                                            "",
                                                                                                            "",
                                                                                                            "uFlux_PS")),
                                                                                                    query::ValueExpr::
                                                                                                            NONE)))),
                                                                    query::ValueExpr::MINUS),
                                                            FactorOp(
                                                                    ValueFactor(
                                                                            query::ValueFactor::FUNCTION,
                                                                            FuncExpr(
                                                                                    "scisql_fluxToAbMag",
                                                                                    ValueExpr(
                                                                                            "",
                                                                                            FactorOp(
                                                                                                    ValueFactor(ColumnRef(
                                                                                                            "",
                                                                                                            "",
                                                                                                            "gFlux_PS")),
                                                                                                    query::ValueExpr::
                                                                                                            NONE)))),
                                                                    query::ValueExpr::NONE)),
                                                    query::CompPredicate::LESS_THAN_OP,
                                                    ValueExpr("", FactorOp(ValueFactor("2.0"),
                                                                           query::ValueExpr::NONE)))),
                                    BoolFactor(
                                            IS,
                                            CompPredicate(
                                                    ValueExpr(
                                                            "",
                                                            FactorOp(
                                                                    ValueFactor(
                                                                            query::ValueFactor::FUNCTION,
                                                                            FuncExpr(
                                                                                    "scisql_fluxToAbMag",
                                                                                    ValueExpr(
                                                                                            "",
                                                                                            FactorOp(
                                                                                                    ValueFactor(ColumnRef(
                                                                                                            "",
                                                                                                            "",
                                                                                                            "gFlux_PS")),
                                                                                                    query::ValueExpr::
                                                                                                            NONE)))),
                                                                    query::ValueExpr::MINUS),
                                                            FactorOp(
                                                                    ValueFactor(
                                                                            query::ValueFactor::FUNCTION,
                                                                            FuncExpr(
                                                                                    "scisql_fluxToAbMag",
                                                                                    ValueExpr(
                                                                                            "",
                                                                                            FactorOp(
                                                                                                    ValueFactor(ColumnRef(
                                                                                                            "",
                                                                                                            "",
                                                                                                            "rFlux_PS")),
                                                                                                    query::ValueExpr::
                                                                                                            NONE)))),
                                                                    query::ValueExpr::NONE)),
                                                    query::CompPredicate::LESS_THAN_OP,
                                                    ValueExpr("", FactorOp(ValueFactor("0.1"),
                                                                           query::ValueExpr::NONE)))),
                                    BoolFactor(
                                            IS,
                                            CompPredicate(
                                                    ValueExpr(
                                                            "",
                                                            FactorOp(
                                                                    ValueFactor(
                                                                            query::ValueFactor::FUNCTION,
                                                                            FuncExpr(
                                                                                    "scisql_fluxToAbMag",
                                                                                    ValueExpr(
                                                                                            "",
                                                                                            FactorOp(
                                                                                                    ValueFactor(ColumnRef(
                                                                                                            "",
                                                                                                            "",
                                                                                                            "rFlux_PS")),
                                                                                                    query::ValueExpr::
                                                                                                            NONE)))),
                                                                    query::ValueExpr::MINUS),
                                                            FactorOp(
                                                                    ValueFactor(
                                                                            query::ValueFactor::FUNCTION,
                                                                            FuncExpr(
                                                                                    "scisql_fluxToAbMag",
                                                                                    ValueExpr(
                                                                                            "",
                                                                                            FactorOp(
                                                                                                    ValueFactor(ColumnRef(
                                                                                                            "",
                                                                                                            "",
                                                                                                            "iFlux_PS")),
                                                                                                    query::ValueExpr::
                                                                                                            NONE)))),
                                                                    query::ValueExpr::NONE)),
                                                    query::CompPredicate::GREATER_THAN_OP,
                                                    ValueExpr("", FactorOp(ValueFactor("-0.8"),
                                                                           query::ValueExpr::NONE)))),
                                    BoolFactor(
                                            IS,
                                            CompPredicate(
                                                    ValueExpr(
                                                            "",
                                                            FactorOp(
                                                                    ValueFactor(
                                                                            query::ValueFactor::FUNCTION,
                                                                            FuncExpr(
                                                                                    "scisql_fluxToAbMag",
                                                                                    ValueExpr(
                                                                                            "",
                                                                                            FactorOp(
                                                                                                    ValueFactor(ColumnRef(
                                                                                                            "",
                                                                                                            "",
                                                                                                            "iFlux_PS")),
                                                                                                    query::ValueExpr::
                                                                                                            NONE)))),
                                                                    query::ValueExpr::MINUS),
                                                            FactorOp(
                                                                    ValueFactor(
                                                                            query::ValueFactor::FUNCTION,
                                                                            FuncExpr(
                                                                                    "scisql_fluxToAbMag",
                                                                                    ValueExpr(
                                                                                            "",
                                                                                            FactorOp(
                                                                                                    ValueFactor(ColumnRef(
                                                                                                            "",
                                                                                                            "",
                                                                                                            "zFlux_PS")),
                                                                                                    query::ValueExpr::
                                                                                                            NONE)))),
                                                                    query::ValueExpr::NONE)),
                                                    query::CompPredicate::LESS_THAN_OP,
                                                    ValueExpr("", FactorOp(ValueFactor("1.4"),
                                                                           query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` "
                "WHERE (scisql_fluxToAbMag(`uFlux_PS`)-scisql_fluxToAbMag(`gFlux_PS`))<2.0 "
                "AND (scisql_fluxToAbMag(`gFlux_PS`)-scisql_fluxToAbMag(`rFlux_PS`))<0.1 "
                "AND (scisql_fluxToAbMag(`rFlux_PS`)-scisql_fluxToAbMag(`iFlux_PS`))>-0.8 "
                "AND (scisql_fluxToAbMag(`iFlux_PS`)-scisql_fluxToAbMag(`zFlux_PS`))<1.4"),
        // test greater-than operator
        Antlr4TestQueries(
                "SELECT COUNT(*) AS OBJ_COUNT FROM Object WHERE gFlux_PS>1e-25",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr(
                                    "OBJ_COUNT",
                                    FactorOp(ValueFactor(
                                                     query::ValueFactor::AGGFUNC,
                                                     FuncExpr("COUNT",
                                                              ValueExpr("",
                                                                        FactorOp(ValueFactor(STAR, ""),
                                                                                 query::ValueExpr::NONE)))),
                                             query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "gFlux_PS")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::GREATER_THAN_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("1e-25"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT COUNT(*) AS `OBJ_COUNT` FROM `Object` WHERE `gFlux_PS`>1e-25"),
        // test DISTINCT
        Antlr4TestQueries(
                "SELECT DISTINCT tract,patch,filterName FROM DeepCoadd ;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "tract")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "patch")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "DeepCoadd", "")), nullptr, nullptr, nullptr, nullptr, 1,
                            -1);
                },
                "SELECT DISTINCT `tract`,`patch`,`filterName` FROM `DeepCoadd`"),
        // test value + int
        Antlr4TestQueries(
                "SELECT s.ra, s.decl FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = "
                "433327840429024 AND    o.latestObsTime BETWEEN s.taiMidPoint - 300 AND s.taiMidPoint + 300",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef(
                                    "", "Object", "o",
                                    JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL,
                                            JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
                            WhereClause(OrTerm(AndTerm(
                                    BoolFactor(IS,
                                               CompPredicate(
                                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                      "", "o", "objectId")),
                                                                              query::ValueExpr::NONE)),
                                                       query::CompPredicate::EQUALS_OP,
                                                       ValueExpr("", FactorOp(ValueFactor("433327840429024"),
                                                                              query::ValueExpr::NONE)))),
                                    BoolFactor(
                                            IS,
                                            BetweenPredicate(
                                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                   "", "o", "latestObsTime")),
                                                                           query::ValueExpr::NONE)),
                                                    BETWEEN,
                                                    ValueExpr("",
                                                              FactorOp(ValueFactor(ColumnRef("", "s",
                                                                                             "taiMidPoint")),
                                                                       query::ValueExpr::MINUS),
                                                              FactorOp(ValueFactor("300"),
                                                                       query::ValueExpr::NONE)),
                                                    ValueExpr("",
                                                              FactorOp(ValueFactor(ColumnRef("", "s",
                                                                                             "taiMidPoint")),
                                                                       query::ValueExpr::PLUS),
                                                              FactorOp(ValueFactor("300"),
                                                                       query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `s`.`ra`,`s`.`decl` "
                "FROM `Object` AS `o` JOIN `Source` AS `s` USING(`objectId`) "
                "WHERE `o`.`objectId`=433327840429024 AND `o`.`latestObsTime` BETWEEN(`s`.`taiMidPoint`-300) "
                "AND (`s`.`taiMidPoint`+300)"),
        // test function in select list
        Antlr4TestQueries(
                "SELECT f(one)/f2(two) FROM  Object where qserv_areaspec_box(0,0,1,1);",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr(
                                    "",
                                    FactorOp(ValueFactor(
                                                     query::ValueFactor::FUNCTION,
                                                     FuncExpr("f",
                                                              ValueExpr("",
                                                                        FactorOp(ValueFactor(ColumnRef(
                                                                                         "", "", "one")),
                                                                                 query::ValueExpr::NONE)))),
                                             query::ValueExpr::DIVIDE),
                                    FactorOp(ValueFactor(
                                                     query::ValueFactor::FUNCTION,
                                                     FuncExpr("f2",
                                                              ValueExpr("",
                                                                        FactorOp(ValueFactor(ColumnRef(
                                                                                         "", "", "two")),
                                                                                 query::ValueExpr::NONE)))),
                                             query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(nullptr, AreaRestrictorBox("0", "0", "1", "1")), nullptr, nullptr,
                            nullptr, 0, -1);
                },
                "SELECT (f(`one`)/f2(`two`)) FROM `Object` WHERE qserv_areaspec_box(0,0,1,1)"),
        // test NATURAL LEFT JOIN
        Antlr4TestQueries(
                "SELECT s1.foo, s2.foo AS s2_foo FROM Source s1 NATURAL LEFT JOIN Source s2 WHERE s1.bar = "
                "s2.bar;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "foo")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("s2_foo", FactorOp(ValueFactor(ColumnRef("", "s2", "foo")),
                                                                    query::ValueExpr::NONE))),
                            FromList(TableRef("", "Source", "s1",
                                              JoinRef(TableRef("", "Source", "s2"), query::JoinRef::LEFT,
                                                      NATURAL, nullptr))),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS,
                                    CompPredicate(
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")),
                                                                   query::ValueExpr::NONE)),
                                            query::CompPredicate::EQUALS_OP,
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")),
                                                                   query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `s1`.`foo`,`s2`.`foo` AS `s2_foo` FROM `Source` AS `s1` NATURAL LEFT OUTER JOIN "
                "`Source` AS `s2` WHERE `s1`.`bar`=`s2`.`bar`"),
        // test NATURAL RIGHT JOIN
        Antlr4TestQueries(
                "SELECT s1.foo, s2.foo AS s2_foo FROM Source s1 NATURAL RIGHT JOIN Source s2 WHERE s1.bar = "
                "s2.bar;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "foo")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("s2_foo", FactorOp(ValueFactor(ColumnRef("", "s2", "foo")),
                                                                    query::ValueExpr::NONE))),
                            FromList(TableRef("", "Source", "s1",
                                              JoinRef(TableRef("", "Source", "s2"), query::JoinRef::RIGHT,
                                                      NATURAL, nullptr))),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS,
                                    CompPredicate(
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")),
                                                                   query::ValueExpr::NONE)),
                                            query::CompPredicate::EQUALS_OP,
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")),
                                                                   query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `s1`.`foo`,`s2`.`foo` AS `s2_foo` FROM `Source` AS `s1` NATURAL RIGHT OUTER JOIN "
                "`Source` AS `s2` WHERE `s1`.`bar`=`s2`.`bar`"),
        // test NATURAL JOIN
        Antlr4TestQueries(
                "SELECT s1.foo, s2.foo AS s2_foo FROM Source s1 NATURAL JOIN Source s2 WHERE s1.bar = "
                "s2.bar;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "foo")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("s2_foo", FactorOp(ValueFactor(ColumnRef("", "s2", "foo")),
                                                                    query::ValueExpr::NONE))),
                            FromList(TableRef("", "Source", "s1",
                                              JoinRef(TableRef("", "Source", "s2"), query::JoinRef::DEFAULT,
                                                      NATURAL, nullptr))),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS,
                                    CompPredicate(
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")),
                                                                   query::ValueExpr::NONE)),
                                            query::CompPredicate::EQUALS_OP,
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")),
                                                                   query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `s1`.`foo`,`s2`.`foo` AS `s2_foo` FROM `Source` AS `s1` NATURAL JOIN `Source` AS "
                "`s2` WHERE `s1`.`bar`=`s2`.`bar`"),
        // test CROSS JOIN
        Antlr4TestQueries(
                "SELECT * FROM Source s1 CROSS JOIN Source s2 WHERE s1.bar = s2.bar;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
                            FromList(TableRef("", "Source", "s1",
                                              JoinRef(TableRef("", "Source", "s2"), query::JoinRef::CROSS,
                                                      NOT_NATURAL, nullptr))),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS,
                                    CompPredicate(
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")),
                                                                   query::ValueExpr::NONE)),
                                            query::CompPredicate::EQUALS_OP,
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")),
                                                                   query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT * FROM `Source` AS `s1` CROSS JOIN `Source` AS `s2` WHERE `s1`.`bar`=`s2`.`bar`"),
        // test = operator
        Antlr4TestQueries(
                "SELECT ra_PS FROM Object WHERE objectId = 417857368235490;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::EQUALS_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `ra_PS` FROM `Object` WHERE `objectId`=417857368235490"),
        // test <> operator
        Antlr4TestQueries(
                "SELECT ra_PS FROM Object WHERE objectId <> 417857368235490;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::NOT_EQUALS_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `ra_PS` FROM `Object` WHERE `objectId`<>417857368235490"),
        // test != operator
        Antlr4TestQueries(
                "SELECT ra_PS FROM Object WHERE objectId != 417857368235490;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::NOT_EQUALS_OP_ALT,
                                                      ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `ra_PS` FROM `Object` WHERE `objectId`!=417857368235490"),
        // test < operator
        Antlr4TestQueries(
                "SELECT ra_PS FROM Object WHERE objectId < 417857368235490;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::LESS_THAN_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `ra_PS` FROM `Object` WHERE `objectId`<417857368235490"),
        // test <= operator
        Antlr4TestQueries(
                "SELECT ra_PS FROM Object WHERE objectId <= 417857368235490;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::LESS_THAN_OR_EQUALS_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `ra_PS` FROM `Object` WHERE `objectId`<=417857368235490"),
        // test >= operator
        Antlr4TestQueries(
                "SELECT ra_PS FROM Object WHERE objectId >= 417857368235490;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::GREATER_THAN_OR_EQUALS_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `ra_PS` FROM `Object` WHERE `objectId`>=417857368235490"),
        // test >= operator
        Antlr4TestQueries(
                "SELECT ra_PS FROM Object WHERE objectId > 417857368235490;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "objectId")),
                                                                             query::ValueExpr::NONE)),
                                                      query::CompPredicate::GREATER_THAN_OP,
                                                      ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `ra_PS` FROM `Object` WHERE `objectId`>417857368235490"),
        // test IS NULL
        Antlr4TestQueries(
                "select objectId from Object where zFlags is NULL;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, NullPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "",
                                                                                                   "zFlags")),
                                                                             query::ValueExpr::NONE)),
                                                      IS_NULL))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE `zFlags` IS NULL"),
        // test IS NOT NULL
        Antlr4TestQueries(
                "select objectId from Object where zFlags is NOT NULL;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, NullPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "",
                                                                                                   "zFlags")),
                                                                             query::ValueExpr::NONE)),
                                                      IS_NOT_NULL))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE `zFlags` IS NOT NULL"),
        // tests NOT LIKE (which is 'NOT LIKE', different than 'NOT' and 'LIKE' operators separately)
        Antlr4TestQueries(
                "SELECT filterId FROM Filter WHERE filterName NOT LIKE 'Z'",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Filter", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                     "", "", "filterName")),
                                                                             query::ValueExpr::NONE)),
                                                      NOT_LIKE,
                                                      ValueExpr("", FactorOp(ValueFactor("'Z'"),
                                                                             query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `filterId` FROM `Filter` WHERE `filterName` NOT LIKE 'Z'"),
        // tests quoted IDs
        Antlr4TestQueries(
                "SELECT `Source`.`sourceId`, `Source`.`objectId` From Source WHERE `Source`.`objectId` IN "
                "(386942193651348) ORDER BY `Source`.`sourceId`",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "sourceId")),
                                                           query::ValueExpr::NONE)),
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "objectId")),
                                                           query::ValueExpr::NONE))),
                            FromList(TableRef("", "Source", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source",
                                                                                                 "objectId")),
                                                                           query::ValueExpr::NONE)),
                                                    IN,
                                                    ValueExpr("", FactorOp(ValueFactor("386942193651348"),
                                                                           query::ValueExpr::NONE))))))),
                            OrderByClause(OrderByTerm(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "sourceId")),
                                                           query::ValueExpr::NONE)),
                                    query::OrderByTerm::DEFAULT, "")),
                            nullptr, nullptr, 0, -1);
                },
                "SELECT `Source`.`sourceId`,`Source`.`objectId` FROM `Source` WHERE `Source`.`objectId` "
                "IN(386942193651348) ORDER BY `Source`.`sourceId`"),

        // tests the NOT BETWEEN operator
        Antlr4TestQueries(
                "SELECT objectId,ra_PS FROM Object WHERE objectId NOT BETWEEN 417857368235490 AND "
                "420949744686724",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS,
                                    BetweenPredicate(
                                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                                   query::ValueExpr::NONE)),
                                            NOT_BETWEEN,
                                            ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                   query::ValueExpr::NONE)),
                                            ValueExpr("", FactorOp(ValueFactor("420949744686724"),
                                                                   query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId`,`ra_PS` FROM `Object` WHERE `objectId` NOT BETWEEN 417857368235490 AND "
                "420949744686724"),

        // tests the && operator.
        // The Qserv IR converts && to AND as a result of the IR structure and how it serializes it to string.
        Antlr4TestQueries(
                "select objectId, iRadius_SG, ra_PS, decl_PS from Object where iRadius_SG > .5 && ra_PS < 2 "
                "&& decl_PS < 3;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iRadius_SG")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(
                                    BoolFactor(IS,
                                               CompPredicate(
                                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                      "", "", "iRadius_SG")),
                                                                              query::ValueExpr::NONE)),
                                                       query::CompPredicate::GREATER_THAN_OP,
                                                       ValueExpr("", FactorOp(ValueFactor(".5"),
                                                                              query::ValueExpr::NONE)))),
                                    BoolFactor(IS, CompPredicate(
                                                           ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                          "", "", "ra_PS")),
                                                                                  query::ValueExpr::NONE)),
                                                           query::CompPredicate::LESS_THAN_OP,
                                                           ValueExpr("", FactorOp(ValueFactor("2"),
                                                                                  query::ValueExpr::NONE)))),
                                    BoolFactor(IS,
                                               CompPredicate(
                                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                      "", "", "decl_PS")),
                                                                              query::ValueExpr::NONE)),
                                                       query::CompPredicate::LESS_THAN_OP,
                                                       ValueExpr("", FactorOp(ValueFactor("3"),
                                                                              query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId`,`iRadius_SG`,`ra_PS`,`decl_PS` FROM `Object` WHERE `iRadius_SG`>.5 AND "
                "`ra_PS`<2 AND `decl_PS`<3"),

        // tests the || operator.
        // The Qserv IR converts || to OR as a result of the IR structure and how it serializes it to string.
        Antlr4TestQueries(
                "select objectId from Object where objectId < 400000000000000 || objectId > 430000000000000 "
                "ORDER BY objectId;",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(
                                    AndTerm(BoolFactor(
                                            IS, CompPredicate(
                                                        ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "", "objectId")),
                                                                               query::ValueExpr::NONE)),
                                                        query::CompPredicate::LESS_THAN_OP,
                                                        ValueExpr("", FactorOp(ValueFactor("400000000000000"),
                                                                               query::ValueExpr::NONE))))),
                                    AndTerm(BoolFactor(
                                            IS, CompPredicate(
                                                        ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "", "objectId")),
                                                                               query::ValueExpr::NONE)),
                                                        query::CompPredicate::GREATER_THAN_OP,
                                                        ValueExpr("", FactorOp(ValueFactor("430000000000000"),
                                                                               query::ValueExpr::NONE))))))),
                            OrderByClause(OrderByTerm(
                                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                           query::ValueExpr::NONE)),
                                    query::OrderByTerm::DEFAULT, "")),
                            nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE `objectId`<400000000000000 OR "
                "`objectId`>430000000000000 ORDER BY `objectId`"),

        // tests NOT IN in the InPredicate
        Antlr4TestQueries(
                "SELECT objectId, ra_PS FROM Object WHERE objectId NOT IN (417857368235490, 420949744686724, "
                "420954039650823);",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "",
                                                                                                 "objectId")),
                                                                           query::ValueExpr::NONE)),
                                                    NOT_IN,
                                                    ValueExpr("", FactorOp(ValueFactor("417857368235490"),
                                                                           query::ValueExpr::NONE)),
                                                    ValueExpr("", FactorOp(ValueFactor("420949744686724"),
                                                                           query::ValueExpr::NONE)),
                                                    ValueExpr("", FactorOp(ValueFactor("420954039650823"),
                                                                           query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId`,`ra_PS` FROM `Object` WHERE `objectId` NOT "
                "IN(417857368235490,420949744686724,420954039650823)"),

        // tests the modulo operator
        Antlr4TestQueries(
                "select objectId, ra_PS % 3, decl_PS from Object where ra_PS % 3 > 1.5",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("",
                                                 FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                          query::ValueExpr::MODULO),
                                                 FactorOp(ValueFactor("3"), query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(
                                                ValueExpr("",
                                                          FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                                   query::ValueExpr::MODULO),
                                                          FactorOp(ValueFactor("3"), query::ValueExpr::NONE)),
                                                query::CompPredicate::GREATER_THAN_OP,
                                                ValueExpr("", FactorOp(ValueFactor("1.5"),
                                                                       query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId`,(`ra_PS`% 3),`decl_PS` FROM `Object` WHERE (`ra_PS`% 3)>1.5"),

        // tests the MOD operator
        Antlr4TestQueries(
                "select objectId, ra_PS MOD 3, decl_PS from Object where ra_PS MOD 3 > 1.5",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE)),
                                       ValueExpr("",
                                                 FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                          query::ValueExpr::MOD),
                                                 FactorOp(ValueFactor("3"), query::ValueExpr::NONE)),
                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(
                                                ValueExpr("",
                                                          FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                                   query::ValueExpr::MOD),
                                                          FactorOp(ValueFactor("3"), query::ValueExpr::NONE)),
                                                query::CompPredicate::GREATER_THAN_OP,
                                                ValueExpr("", FactorOp(ValueFactor("1.5"),
                                                                       query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId`,(`ra_PS` MOD 3),`decl_PS` FROM `Object` WHERE (`ra_PS` MOD 3)>1.5"),

        // tests the DIV operator
        Antlr4TestQueries(
                "SELECT objectId from Object where ra_PS DIV 2 > 1",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(
                                                ValueExpr("",
                                                          FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")),
                                                                   query::ValueExpr::DIV),
                                                          FactorOp(ValueFactor("2"), query::ValueExpr::NONE)),
                                                query::CompPredicate::GREATER_THAN_OP,
                                                ValueExpr("", FactorOp(ValueFactor("1"),
                                                                       query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE (`ra_PS` DIV 2)>1"),

        // tests the & operator
        Antlr4TestQueries(
                "SELECT objectId from Object where objectID & 1 = 1",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(
                                                ValueExpr("",
                                                          FactorOp(ValueFactor(ColumnRef("", "", "objectID")),
                                                                   query::ValueExpr::BIT_AND),
                                                          FactorOp(ValueFactor("1"), query::ValueExpr::NONE)),
                                                query::CompPredicate::EQUALS_OP,
                                                ValueExpr("", FactorOp(ValueFactor("1"),
                                                                       query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE (`objectID`&1)=1"),

        // tests the | operator
        Antlr4TestQueries(
                "SELECT objectId from Object where objectID | 1 = 1",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(
                                                ValueExpr("",
                                                          FactorOp(ValueFactor(ColumnRef("", "", "objectID")),
                                                                   query::ValueExpr::BIT_OR),
                                                          FactorOp(ValueFactor("1"), query::ValueExpr::NONE)),
                                                query::CompPredicate::EQUALS_OP,
                                                ValueExpr("", FactorOp(ValueFactor("1"),
                                                                       query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE (`objectID`|1)=1"),

        // tests the << operator
        Antlr4TestQueries(
                "SELECT objectId from Object where objectID << 10 = 1",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS,
                                    CompPredicate(
                                            ValueExpr("",
                                                      FactorOp(ValueFactor(ColumnRef("", "", "objectID")),
                                                               query::ValueExpr::BIT_SHIFT_LEFT),
                                                      FactorOp(ValueFactor("10"), query::ValueExpr::NONE)),
                                            query::CompPredicate::EQUALS_OP,
                                            ValueExpr("", FactorOp(ValueFactor("1"),
                                                                   query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE (`objectID`<<10)=1"),

        // tests the >> operator
        Antlr4TestQueries(
                "SELECT objectId from Object where objectID >> 10 = 1",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS,
                                    CompPredicate(
                                            ValueExpr("",
                                                      FactorOp(ValueFactor(ColumnRef("", "", "objectID")),
                                                               query::ValueExpr::BIT_SHIFT_RIGHT),
                                                      FactorOp(ValueFactor("10"), query::ValueExpr::NONE)),
                                            query::CompPredicate::EQUALS_OP,
                                            ValueExpr("", FactorOp(ValueFactor("1"),
                                                                   query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE (`objectID`>>10)=1"),

        // tests the ^ operator
        Antlr4TestQueries(
                "SELECT objectId from Object where objectID ^ 1 = 1",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")),
                                                              query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS, CompPredicate(
                                                ValueExpr("",
                                                          FactorOp(ValueFactor(ColumnRef("", "", "objectID")),
                                                                   query::ValueExpr::BIT_XOR),
                                                          FactorOp(ValueFactor("1"), query::ValueExpr::NONE)),
                                                query::CompPredicate::EQUALS_OP,
                                                ValueExpr("", FactorOp(ValueFactor("1"),
                                                                       query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT `objectId` FROM `Object` WHERE (`objectID`^1)=1"),

        // tests NOT with a BoolFactor
        Antlr4TestQueries(
                "select * from Filter where NOT filterId > 1 AND filterId < 6",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
                            FromList(TableRef("", "Filter", "")),
                            WhereClause(OrTerm(AndTerm(
                                    BoolFactor(
                                            IS_NOT,
                                            CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                         "", "", "filterId")),
                                                                                 query::ValueExpr::NONE)),
                                                          query::CompPredicate::GREATER_THAN_OP,
                                                          ValueExpr("", FactorOp(ValueFactor("1"),
                                                                                 query::ValueExpr::NONE)))),
                                    BoolFactor(IS,
                                               CompPredicate(
                                                       ValueExpr("", FactorOp(ValueFactor(ColumnRef(
                                                                                      "", "", "filterId")),
                                                                              query::ValueExpr::NONE)),
                                                       query::CompPredicate::LESS_THAN_OP,
                                                       ValueExpr("", FactorOp(ValueFactor("6"),
                                                                              query::ValueExpr::NONE))))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT * FROM `Filter` WHERE NOT `filterId`>1 AND `filterId`<6"),

        // tests NOT with an AND term
        Antlr4TestQueries(
                "select * from Filter where NOT (filterId > 1 AND filterId < 6)",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(
                                    ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
                            FromList(TableRef("", "Filter", "")),
                            WhereClause(OrTerm(AndTerm(BoolFactor(
                                    IS_NOT, PassTerm("("),
                                    BoolTermFactor(AndTerm(
                                            BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "", "filterId")),
                                                                               query::ValueExpr::NONE)),
                                                            query::CompPredicate::GREATER_THAN_OP,
                                                            ValueExpr("", FactorOp(ValueFactor("1"),
                                                                                   query::ValueExpr::NONE)))),
                                            BoolFactor(
                                                    IS,
                                                    CompPredicate(
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor(ColumnRef(
                                                                                       "", "", "filterId")),
                                                                               query::ValueExpr::NONE)),
                                                            query::CompPredicate::LESS_THAN_OP,
                                                            ValueExpr("",
                                                                      FactorOp(ValueFactor("6"),
                                                                               query::ValueExpr::NONE)))))),
                                    PassTerm(")"))))),
                            nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT * FROM `Filter` WHERE NOT(`filterId`>1 AND `filterId`<6)"),

        // tests expression with alias in select list
        Antlr4TestQueries(
                "SELECT objectId - 1 AS o FROM Object",
                []() -> shared_ptr<query::SelectStmt> {
                    return SelectStmt(
                            SelectList(ValueExpr(
                                    "o",
                                    FactorOp(ValueFactor(ColumnRef(TableRef("", "", ""), "objectId")),
                                             query::ValueExpr::MINUS),
                                    FactorOp(ValueFactor("1"), query::ValueExpr::NONE))),
                            FromList(TableRef("", "Object", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);
                },
                "SELECT (`objectId`-1) AS `o` FROM `Object`"),
};

BOOST_DATA_TEST_CASE(antlr4_test, ANTLR4_TEST_QUERIES, queryInfo) {
    query::SelectStmt::Ptr selectStatement;
    BOOST_REQUIRE_NO_THROW(selectStatement = ccontrol::ParseRunner::makeSelectStmt(queryInfo.query));
    BOOST_REQUIRE(selectStatement != nullptr);
    BOOST_TEST_MESSAGE("antlr4 selectStmt structure:" << *selectStatement);
    // verify the selectStatements are the same:
    auto compareStatement = queryInfo.compareStmt();
    BOOST_REQUIRE_MESSAGE(
            *selectStatement == *compareStatement,
            "parser-generated statement:" << *selectStatement
                                          << "does not match compare statement:" << *compareStatement);
    // verify the selectStatement converted back to sql is the same as the original query:
    std::string serializedQuery;
    BOOST_REQUIRE_NO_THROW(serializedQuery = selectStatement->getQueryTemplate().sqlFragment());
    BOOST_REQUIRE_EQUAL(serializedQuery,
                        (queryInfo.serializedQuery != "" ? queryInfo.serializedQuery : queryInfo.query));
}

BOOST_AUTO_TEST_CASE(set_session_var_test) {
    auto parser = ccontrol::ParseRunner("SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = 0;");
    auto uq = parser.getUserQuery();
    auto setQuery = std::static_pointer_cast<ccontrol::UserQuerySet>(uq);
    BOOST_REQUIRE_EQUAL(setQuery->varName(), "QSERV_ROW_COUNTER_OPTIMIZATION");
    BOOST_REQUIRE_EQUAL(setQuery->varValue(), "0");

    auto parser1 = ccontrol::ParseRunner("SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = 1;");
    uq = parser1.getUserQuery();
    setQuery = std::static_pointer_cast<ccontrol::UserQuerySet>(uq);
    BOOST_REQUIRE_EQUAL(setQuery->varName(), "QSERV_ROW_COUNTER_OPTIMIZATION");
    BOOST_REQUIRE_EQUAL(setQuery->varValue(), "1");

    // Verify that bool vals (not handled) are explicity rejected (to prevent a case where a
    // non-zero value "FALSE" evaluates to ON)
    BOOST_CHECK_THROW(ccontrol::ParseRunner("SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = FALSE;"),
                      parser::adapter_order_error);
    BOOST_CHECK_THROW(ccontrol::ParseRunner("SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = TRUE;"),
                      parser::adapter_order_error);
}

BOOST_AUTO_TEST_SUITE_END()
