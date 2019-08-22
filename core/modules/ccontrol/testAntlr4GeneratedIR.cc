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
#include <boost/test/included/unit_test.hpp> // it seems this must be included before boost/test/data/test_case.hpp
#include <boost/test/data/test_case.hpp>

// Qserv headers
#include "ccontrol/UserQueryType.h"
#include "parser/ParseException.h"
#include "parser/SelectParser.h"
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
#include "query/QsRestrictor.h"
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
template <typename...Targs>
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
template <typename...Targs>
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
        query::CompPredicate::OpType op, shared_ptr<query::ValueExpr> const& right) {
    return make_shared<query::CompPredicate>(left, op, right);
}


/// Create a FactorOp with a ValueFactor
query::ValueExpr::FactorOp FactorOp(shared_ptr<query::ValueFactor> const& factor, query::ValueExpr::Op op) {
    return query::ValueExpr::FactorOp(factor, op);
}


/// Create a FuncExpr
/// args should be instance of shared_ptr to query::ValueExpr.
template <typename...Targs>
shared_ptr<query::FuncExpr> FuncExpr(string const& name, Targs const&... args) {
    vector<shared_ptr<query::ValueExpr>> valueExprVec;
    pusher(valueExprVec, args...);
    return make_shared<query::FuncExpr>(name, valueExprVec);
}


/// Create a new FromList. Args should be a comma separated list of TableRefPtr.
template <typename...Targs>
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
template <typename...Targs>
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
template <typename...Targs>
shared_ptr<query::InPredicate> InPredicate(shared_ptr<query::ValueExpr> const& left,
                                           InNotIn in, Targs const&... args) {
    auto valueExprVec = vector<shared_ptr<query::ValueExpr>>();
    pusher(valueExprVec, args...);
    return make_shared<query::InPredicate>(left, valueExprVec, in == NOT_IN);
}


/// Create a new JoinRef
shared_ptr<query::JoinRef> JoinRef(shared_ptr<query::TableRef> right, query::JoinRef::Type joinType, Natural natural,
                                   shared_ptr<query::JoinSpec> joinSpec) {
    bool isNatural = (NATURAL == natural);
    return make_shared<query::JoinRef>(right, joinType, isNatural, joinSpec);
}


/// Create a new JoinSpec
shared_ptr<query::JoinSpec> JoinSpec(shared_ptr<query::ColumnRef> ref, shared_ptr<query::BoolTerm> const& onTerm) {
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
template <typename...Targs>
shared_ptr<query::OrderByClause> OrderByClause(Targs... args) {
    auto orderByTerms = make_shared<vector<query::OrderByTerm>>();
    pusher(*orderByTerms, args...);
    return make_shared<query::OrderByClause>(orderByTerms);
}


/// Create an OrderByTerm with a ValueExprPtr term.
/// Note this does not new an object or create a shared_ptr, as dictated by the OrderByClause interface.
query::OrderByTerm OrderByTerm(shared_ptr<query::ValueExpr> const& term, query::OrderByTerm::Order order, string collate) {
    return query::OrderByTerm(term, order, collate);
}



/// Create a new OrTerm. Args can be a shared_ptr to any kind of object that inherits from BoolTerm.
template <typename...Targs>
shared_ptr<query::OrTerm> OrTerm(Targs... args) {
    vector<shared_ptr<query::BoolTerm>> terms;
    pusher(terms, args...);
    return make_shared<query::OrTerm>(terms);
}


/// Create a new PassTerm with given text.
shared_ptr<query::PassTerm> PassTerm(string const& text) {
    return make_shared<query::PassTerm>(text);
}


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
    return make_shared<query::AreaRestrictorEllipse>(centerLonDegree, centerLatDegree, semiMajorAxisAngleArcsec,
                                                     semiMinorAxisAngleArcsec, positionAngleDegree);
}


shared_ptr<query::AreaRestrictorPoly> AreaRestrictorPoly(std::vector<std::string> const& parameters) {
    return make_shared<query::AreaRestrictorPoly>(parameters);
}


/// Create a new SelectList. Args should be a comma separated list of shared_ptr to ValueExpr.
template <typename...Targs>
shared_ptr<query::SelectList> SelectList(Targs... args) {
    auto ptr = make_shared<vector<shared_ptr<query::ValueExpr>>>();
    pusher(*ptr, args...);
    return make_shared<query::SelectList>(ptr);
}


/// Create a new SelectList with the given members.
shared_ptr<query::SelectStmt> SelectStmt(
        shared_ptr<query::SelectList> const& selectList,
        shared_ptr<query::FromList> const& fromList,
        shared_ptr<query::WhereClause> const& whereClause,
        shared_ptr<query::OrderByClause> const& orderByClause,
        shared_ptr<query::GroupByClause> const& groupByClause,
        shared_ptr<query::HavingClause> const& havingClause,
        bool hasDistinct,
        int limit) {
    return make_shared<query::SelectStmt>(selectList, fromList, whereClause, orderByClause, groupByClause, havingClause, hasDistinct, limit);
}


/// Create a new TableRef with the given database, table, alias name, and JoinRefs. Args should
/// be a comma separated list of shared_ptr to JoinRef.
template <typename...Targs>
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
template <typename...Targs>
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
shared_ptr<query::WhereClause> WhereClause(shared_ptr<query::OrTerm> const& orTerm,
                                           shared_ptr<query::AreaRestrictor> const& areaRestrictor=nullptr) {
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
    Antlr4TestQueries(string const& iQuery,
                      function<shared_ptr<query::SelectStmt>()> const& iCompareStmt,
                      string const& iSerializedQuery)
    : query(iQuery)
    , compareStmt(iCompareStmt)
    , serializedQuery(iSerializedQuery)
    {}

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
        "ORDER BY filterId", // case01/queries/0012.1_raftAndCcd.sql
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))
            ),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS,
                    PassTerm("("),
                    BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(
                        ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)),
                        query::CompPredicate::EQUALS_OP,
                        ValueExpr("", FactorOp(ValueFactor("887404831"), query::ValueExpr::NONE))))))),
                    PassTerm(")")),
                BoolFactor(IS,
                    PassTerm("("),
                    BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(
                        ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "raftName")), query::ValueExpr::NONE)),
                        query::CompPredicate::EQUALS_OP,
                        ValueExpr("", FactorOp(ValueFactor("'3,3'"), query::ValueExpr::NONE))))))),
                    PassTerm(")")),
                BoolFactor(IS,
                    PassTerm("("),
                    BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(
                        ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ccdName")), query::ValueExpr::NONE)),
                        LIKE,
                        ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE))))))),
                    PassTerm(")"))
            ))),
            OrderByClause(
                OrderByTerm(
                    ValueExpr(
                        "",
                        FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)
                    ),
                    query::OrderByTerm::DEFAULT,
                    "")),
            nullptr, // Group By Clause
            nullptr,
            0,
            -1
        );},
        "SELECT sce.filterId,sce.filterName "
            "FROM Science_Ccd_Exposure AS `sce` "
            "WHERE (sce.visit=887404831) AND (sce.raftName='3,3') AND (sce.ccdName LIKE '%') "
            "ORDER BY filterId"
     ),

    // tests a query with 2 items in the GROUP BY expression
    Antlr4TestQueries(
        "SELECT objectId, filterId FROM Source GROUP BY objectId, filterId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE))
            ),
            FromList(TableRef("", "Source", "")),
            nullptr, // WhereClause
            nullptr, // OrderByClause
            GroupByClause(
                GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), ""),
                GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)), "")
            ),
            nullptr,
            0,
            -1
        );},
        "SELECT objectId,filterId FROM Source GROUP BY objectId,filterId"
    ),


    /// Queries below here come from integration tests and other unit tests to sanity check that they generate correct IR and reserialzie to a query string correctly.
    Antlr4TestQueries(
        "select max(filterID) from Filter",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("max", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterID")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT max(filterID) FROM Filter"
    ),
    Antlr4TestQueries(
        "select min(filterID) from Filter",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("min", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterID")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT min(filterID) FROM Filter"
    ),
    Antlr4TestQueries(
        "SELECT objectId,iauId,ra_PS FROM   Object WHERE  objectId = 430213989148129",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iauId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430213989148129"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,iauId,ra_PS FROM Object WHERE objectId=430213989148129"
    ),
    Antlr4TestQueries(
        "select ra_Ps, decl_PS FROM Object WHERE objectId IN (390034570102582, 396210733076852, 393126946553816, 390030275138483)",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_Ps")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("390034570102582"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("396210733076852"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("393126946553816"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("390030275138483"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_Ps,decl_PS FROM Object WHERE objectId IN(390034570102582,396210733076852,393126946553816,390030275138483)"
    ),
    Antlr4TestQueries(
        "SELECT objectId,iauId,ra_PS,ra_PS_Sigma FROM   Object WHERE  objectId = 430213989148129",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iauId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS_Sigma")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430213989148129"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,iauId,ra_PS,ra_PS_Sigma FROM Object WHERE objectId=430213989148129"
    ),
    Antlr4TestQueries(
        "SELECT * FROM   Object WHERE  objectId = 430213989000",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430213989000"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE objectId=430213989000"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 390034570102582 AND    o.latestObsTime = s.taiMidPoint",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "raRange")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "declRange")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("390034570102582"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "latestObsTime")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "taiMidPoint")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.raRange,o.declRange FROM Object AS `o` JOIN Source AS `s` USING(objectId) WHERE o.objectId=390034570102582 AND o.latestObsTime=s.taiMidPoint"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.raRange, o.declRange FROM Object o, Source s WHERE o.objectId = 390034570102582 AND o.objectId = s.objectId AND o.latestObsTime = s.taiMidPoint;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "raRange")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "declRange")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o"), TableRef("", "Source", "s")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("390034570102582"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "objectId")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "latestObsTime")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "taiMidPoint")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.raRange,o.declRange FROM Object AS `o`,Source AS `s` WHERE o.objectId=390034570102582 AND o.objectId=s.objectId AND o.latestObsTime=s.taiMidPoint"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 390034570102582 AND    o.latestObsTime = s.taiMidPoint",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "raRange")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "declRange")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("390034570102582"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "latestObsTime")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "taiMidPoint")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.raRange,o.declRange FROM Object AS `o` JOIN Source AS `s` USING(objectId) WHERE o.objectId=390034570102582 AND o.latestObsTime=s.taiMidPoint"
    ),
    Antlr4TestQueries(
        "SELECT offset, mjdRef, drift FROM LeapSeconds where offset = 10",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "offset")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "mjdRef")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "drift")), query::ValueExpr::NONE))),
            FromList(TableRef("", "LeapSeconds", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "offset")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("10"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT offset,mjdRef,drift FROM LeapSeconds WHERE offset=10"
    ),
    Antlr4TestQueries(
        "SELECT sourceId, objectId FROM Source WHERE objectId = 386942193651348 ORDER BY sourceId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sourceId,objectId FROM Source WHERE objectId=386942193651348 ORDER BY sourceId"
    ),
    Antlr4TestQueries(
        "SELECT sourceId, objectId FROM Source WHERE objectId = 386942193651348 ORDER BY sourceId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sourceId,objectId FROM Source WHERE objectId=386942193651348 ORDER BY sourceId"
    ),
    Antlr4TestQueries(
        "SELECT sourceId, objectId FROM Source WHERE objectId IN (1234) ORDER BY sourceId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("1234"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sourceId,objectId FROM Source WHERE objectId IN(1234) ORDER BY sourceId"
    ),
    Antlr4TestQueries(
        "SELECT sourceId, objectId FROM Source WHERE objectId IN (386942193651348) ORDER BY sourceId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sourceId,objectId FROM Source WHERE objectId IN(386942193651348) ORDER BY sourceId"
    ),
    Antlr4TestQueries(
        "select COUNT(*) AS N FROM Source WHERE objectId IN (386950783579546, 386942193651348)",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("N", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("386950783579546"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `N` FROM Source WHERE objectId IN(386950783579546,386942193651348)"
    ),
    Antlr4TestQueries(
        "select COUNT(*) AS N FROM Source WHERE objectId BETWEEN 386942193651348 AND 386950783579546",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("N", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("386950783579546"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `N` FROM Source WHERE objectId BETWEEN 386942193651348 AND 386950783579546"
    ),
    Antlr4TestQueries(
        "SELECT sourceId, objectId FROM Source WHERE objectId IN (386942193651348) ORDER BY sourceId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sourceId,objectId FROM Source WHERE objectId IN(386942193651348) ORDER BY sourceId"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%') ORDER BY filterId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("887404831"), query::ValueExpr::NONE))))))), PassTerm(")")), BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "raftName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'3,3'"), query::ValueExpr::NONE))))))), PassTerm(")")), BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ccdName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE))))))), PassTerm(")"))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM Science_Ccd_Exposure AS `sce` WHERE (sce.visit=887404831) AND (sce.raftName='3,3') AND (sce.ccdName LIKE '%') ORDER BY filterId"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%') ORDER BY filterId LIMIT 5",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("887404831"), query::ValueExpr::NONE))))))), PassTerm(")")), BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "raftName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'3,3'"), query::ValueExpr::NONE))))))), PassTerm(")")), BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ccdName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE))))))), PassTerm(")"))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, 5);},
        "SELECT sce.filterId,sce.filterName FROM Science_Ccd_Exposure AS `sce` WHERE (sce.visit=887404831) AND (sce.raftName='3,3') AND (sce.ccdName LIKE '%') ORDER BY filterId LIMIT 5"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%')",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("887404831"), query::ValueExpr::NONE))))))), PassTerm(")")), BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "raftName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'3,3'"), query::ValueExpr::NONE))))))), PassTerm(")")), BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ccdName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM Science_Ccd_Exposure AS `sce` WHERE (sce.visit=887404831) AND (sce.raftName='3,3') AND (sce.ccdName LIKE '%')"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) as OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0.1, -6, 4, 6) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.9"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.0"), query::ValueExpr::NONE)))))), AreaRestrictorBox("0.1", "-6", "4", "6")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_box(0.1,-6,4,6) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.9 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 1.0"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) FROM   Object WHERE qserv_areaspec_box(0.1, -6, 4, 6) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.9"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.0"), query::ValueExpr::NONE)))))), AreaRestrictorBox("0.1", "-6", "4", "6")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) FROM Object WHERE qserv_areaspec_box(0.1,-6,4,6) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.9 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 1.0"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) as OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0, -6, 4, -5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.2 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.2",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE)))))), AreaRestrictorBox("0", "-6", "4", "-5")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_box(0,-6,4,-5) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.2 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 0.2"
    ),
    Antlr4TestQueries(
        "SELECT objectId, AVG(ra_PS) as ra FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) GROUP BY objectId ORDER BY ra",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("ra", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("AVG", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "3", "10")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")),
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT objectId,AVG(ra_PS) AS `ra` FROM Object WHERE qserv_areaspec_box(0,0,3,10) GROUP BY objectId ORDER BY ra"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) ORDER BY objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "3", "10")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE qserv_areaspec_box(0,0,3,10) ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Source s JOIN   Science_Ccd_Exposure sce USING (scienceCcdExposureId) WHERE  sce.visit IN (885449631,886257441,886472151) ORDER BY objectId LIMIT 10",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s", JoinRef(TableRef("", "Science_Ccd_Exposure", "sce"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "scienceCcdExposureId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("885449631"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("886257441"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("886472151"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, 10);},
        "SELECT objectId FROM Source AS `s` JOIN Science_Ccd_Exposure AS `sce` USING(scienceCcdExposureId) WHERE sce.visit IN(885449631,886257441,886472151) ORDER BY objectId LIMIT 10"
    ),
    Antlr4TestQueries(
        "SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) FROM   Source JOIN   Object USING(objectId) JOIN   Filter USING(filterId) WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' ORDER BY objectId, taiMidPoint ASC",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "taiMidPoint")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "psfFlux")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "", JoinRef(TableRef("", "Object", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)), JoinRef(TableRef("", "Filter", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "filterId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))))), AreaRestrictorBox("355", "0", "360", "20")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, ""), OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "taiMidPoint")), query::ValueExpr::NONE)), query::OrderByTerm::ASC, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId,taiMidPoint,scisql_fluxToAbMag(psfFlux) FROM Source JOIN Object USING(objectId) JOIN Filter USING(filterId) WHERE qserv_areaspec_box(355,0,360,20) filterName='g' ORDER BY objectId, taiMidPoint ASC"
    ),
    Antlr4TestQueries(
        "SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM Object o1, Object o2 WHERE qserv_areaspec_box(0, 0, 0.2, 1) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.016 AND o1.objectId <> o2.objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("objId1", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("objId2", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("distance", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.016"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)))))), AreaRestrictorBox("0", "0", "0.2", "1")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId AS `objId1`,o2.objectId AS `objId2`,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS `distance` FROM Object AS `o1`,Object AS `o2` WHERE qserv_areaspec_box(0,0,0.2,1) scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS)<0.016 AND o1.objectId<>o2.objectId"
    ),
    Antlr4TestQueries(
        "SELECT scienceCcdExposureId, hex(poly) as hexPoly FROM Science_Ccd_Exposure;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "scienceCcdExposureId")), query::ValueExpr::NONE)),
                ValueExpr("hexPoly", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("hex", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "poly")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT scienceCcdExposureId,hex(poly) AS `hexPoly` FROM Science_Ccd_Exposure"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS AS ra, decl_PS AS decl FROM Object WHERE ra_PS BETWEEN 0. AND 1. AND decl_PS BETWEEN 0. AND 1. ORDER BY ra, decl;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("ra", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("decl", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0."), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1."), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0."), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1."), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, ""), OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT ra_PS AS `ra`,decl_PS AS `decl` FROM Object WHERE ra_PS BETWEEN 0.AND 1.AND decl_PS BETWEEN 0.AND 1.ORDER BY ra, decl"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS AS ra FROM Object WHERE ra_PS BETWEEN 0. AND 1. AND decl_PS BETWEEN 0. AND 1. ORDER BY ra;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("ra", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0."), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1."), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0."), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1."), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT ra_PS AS `ra` FROM Object WHERE ra_PS BETWEEN 0.AND 1.AND decl_PS BETWEEN 0.AND 1.ORDER BY ra"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Object WHERE QsErV_ArEaSpEc_BoX(0, 0, 3, 10) ORDER BY objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "3", "10")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE qserv_areaspec_box(0,0,3,10) ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "SELECT objectId, iauId, ra_PS FROM   Object WHERE  objectId = 433327840428032",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iauId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("433327840428032"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,iauId,ra_PS FROM Object WHERE objectId=433327840428032"
    ),
    Antlr4TestQueries(
        "SELECT * FROM   Object WHERE  objectId = 430213989000",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430213989000"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE objectId=430213989000"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 433327840428032",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "raRange")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "declRange")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("433327840428032"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.raRange,o.declRange FROM Object AS `o` JOIN Source AS `s` USING(objectId) WHERE o.objectId=433327840428032"
    ),
    Antlr4TestQueries(
        "SELECT sourceId, scienceCcdExposureId, filterId FROM   Source WHERE  sourceId = 2867930096075697",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "scienceCcdExposureId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("2867930096075697"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sourceId,scienceCcdExposureId,filterId FROM Source WHERE sourceId=2867930096075697"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0.1, -6, 4, 6) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.9"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.0"), query::ValueExpr::NONE)))))), AreaRestrictorBox("0.1", "-6", "4", "6")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_box(0.1,-6,4,6) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.9 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 1.0"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_circle(1.2, 3.2, 0.5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.6 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.6",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.6"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.6"), query::ValueExpr::NONE)))))), AreaRestrictorCircle("1.2", "3.2", "0.5")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_circle(1.2,3.2,0.5) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.6 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 0.6"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_ellipse(1.2, 3.2, 6000, 5000, 0.2) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.6 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.6",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.6"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.6"), query::ValueExpr::NONE)))))), AreaRestrictorEllipse("1.2", "3.2", "6000", "5000", "0.2")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_ellipse(1.2,3.2,6000,5000,0.2) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.6 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 0.6"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_poly(1.0, 3.0, 1.5, 2.0, 2.0, 4.0) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.6 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.6",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.6"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.6"), query::ValueExpr::NONE)))))), AreaRestrictorPoly({"1.0", "3.0", "1.5", "2.0", "2.0", "4.0"})), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_poly(1.0,3.0,1.5,2.0,2.0,4.0) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.6 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 0.6"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0, -6, 4, -5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.2 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.2",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE)))))), AreaRestrictorBox("0", "-6", "4", "-5")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_box(0,-6,4,-5) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.2 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 0.2"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS, decl_PS FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) ORDER BY objectId, ra_PS, decl_PS",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "3", "10")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, ""), OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, ""), OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS,decl_PS FROM Object WHERE qserv_areaspec_box(0,0,3,10) ORDER BY objectId, ra_PS, decl_PS"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Object WHERE qserv_areaspec_circle(1.5, 3, 1) ORDER BY objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorCircle("1.5", "3", "1")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE qserv_areaspec_circle(1.5,3,1) ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Object WHERE qserv_areaspec_ellipse(1.5, 3, 3500, 200, 0.5) ORDER BY objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorEllipse("1.5", "3", "3500", "200", "0.5")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE qserv_areaspec_ellipse(1.5,3,3500,200,0.5) ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Object WHERE qserv_areaspec_poly(0, 0, 3, 10, 0, 5, 3, 1) ORDER BY objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorPoly({"0", "0", "3", "10", "0", "5", "3", "1"})),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE qserv_areaspec_poly(0,0,3,10,0,5,3,1) ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) ORDER BY objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "3", "10")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE qserv_areaspec_box(0,0,3,10) ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM Object o1, Object o2 WHERE qserv_areaspec_box(1.2, 3.3, 1.3, 3.4) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.016 AND o1.objectId <> o2.objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("objId1", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("objId2", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("distance", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.016"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)))))), AreaRestrictorBox("1.2", "3.3", "1.3", "3.4")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId AS `objId1`,o2.objectId AS `objId2`,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS `distance` FROM Object AS `o1`,Object AS `o2` WHERE qserv_areaspec_box(1.2,3.3,1.3,3.4) scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS)<0.016 AND o1.objectId<>o2.objectId"
    ),
    Antlr4TestQueries(
        "SELECT  objectId FROM    Object WHERE   scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS) <  2.0 AND  scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) <  0.1 AND  scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) > -0.8 AND  scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) <  1.4",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "uFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("2.0"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("-0.8"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1.4"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS))<2.0 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))<0.1 AND (scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS))>-0.8 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS))<1.4"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS OBJ_COUNT FROM Object",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) AS `OBJ_COUNT` FROM Object"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS OBJ_COUNT FROM   Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5 AND scisql_fluxToAbMag(gFlux_PS) - scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.3 AND 0.4 AND scisql_fluxToAbMag(iFlux_PS) - scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.12",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("21"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("21.5"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.3"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.4"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.12"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) AS `OBJ_COUNT` FROM Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.3 AND 0.4 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 0.12"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM Object WHERE gFlux_PS>1e-25",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1e-25"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE gFlux_PS>1e-25"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS, decl_PS, uFlux_PS, gFlux_PS, rFlux_PS, iFlux_PS, zFlux_PS, yFlux_PS FROM Object WHERE scisql_fluxToAbMag(iFlux_PS) - scisql_fluxToAbMag(zFlux_PS) > 0.08",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "uFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "yFlux_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.08"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS,decl_PS,uFlux_PS,gFlux_PS,rFlux_PS,iFlux_PS,zFlux_PS,yFlux_PS FROM Object WHERE (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS))>0.08"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS OBJ_COUNT FROM Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND  decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 and 21.5",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("21"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("21.5"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) AS `OBJ_COUNT` FROM Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS, decl_PS, scisql_fluxToAbMag(zFlux_PS) AS fluxToAbMag FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("fluxToAbMag", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS,decl_PS,scisql_fluxToAbMag(zFlux_PS) AS `fluxToAbMag` FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS, decl_PS, scisql_fluxToAbMag(zFlux_PS) FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS,decl_PS,scisql_fluxToAbMag(zFlux_PS) FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS OBJ_COUNT FROM Object WHERE scisql_angSep(ra_PS, decl_PS, 1.2, 3.2) < 0.2",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.2"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.2"), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) AS `OBJ_COUNT` FROM Object WHERE scisql_angSep(ra_PS,decl_PS,1.2,3.2)<0.2"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM Source JOIN Object USING(objectId) WHERE ra_PS BETWEEN 1.28 AND 1.38 AND  decl_PS BETWEEN 3.18 AND 3.34",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "", JoinRef(TableRef("", "Object", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Source JOIN Object USING(objectId) WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 433327840429024 AND    o.latestObsTime BETWEEN s.taiMidPoint - 300 AND s.taiMidPoint + 300",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("433327840429024"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "latestObsTime")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "taiMidPoint")), query::ValueExpr::MINUS), FactorOp(ValueFactor("300"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "taiMidPoint")), query::ValueExpr::PLUS), FactorOp(ValueFactor("300"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl FROM Object AS `o` JOIN Source AS `s` USING(objectId) WHERE o.objectId=433327840429024 AND o.latestObsTime BETWEEN(s.taiMidPoint-300) AND (s.taiMidPoint+300)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName like '%' AND sce.field = 535 AND sce.camcol like '%' AND sce.run = 94;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE)))),
                BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run FROM Science_Ccd_Exposure AS `sce` WHERE sce.filterName LIKE '%' AND sce.field=535 AND sce.camcol LIKE '%' AND sce.run=94"
    ),
    Antlr4TestQueries(
        "SELECT sce.scienceCcdExposureId, sce.filterName, sce.field, sce.camcol, sce.run, sce.filterId, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 535 AND sce.camcol = 1 AND sce.run = 94;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fwhm")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.scienceCcdExposureId,sce.filterName,sce.field,sce.camcol,sce.run,sce.filterId,sce.ra,sce.decl,sce.crpix1,sce.crpix2,sce.crval1,sce.crval2,sce.cd1_1,sce.cd1_2,sce.cd2_1,sce.cd2_2,sce.fluxMag0,sce.fluxMag0Sigma,sce.fwhm FROM Science_Ccd_Exposure AS `sce` WHERE sce.filterName='g' AND sce.field=535 AND sce.camcol=1 AND sce.run=94"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 670 AND sce.camcol = 2 AND sce.run = 7202 ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("670"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("2"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("7202"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run FROM Science_Ccd_Exposure AS `sce` WHERE sce.filterName='g' AND sce.field=670 AND sce.camcol=2 AND sce.run=7202"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 670 AND sce.camcol = 2 AND sce.run = 7202 ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("670"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("2"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("7202"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM Science_Ccd_Exposure AS `sce` WHERE sce.filterName='g' AND sce.field=670 AND sce.camcol=2 AND sce.run=7202"
    ),
    Antlr4TestQueries(
        "SELECT DISTINCT tract,patch,filterName FROM DeepCoadd ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "")), nullptr, nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT tract,patch,filterName FROM DeepCoadd"
    ),
    Antlr4TestQueries(
        "SELECT DISTINCT tract, patch, filterName FROM   DeepCoadd WHERE  tract = 0 AND patch = '159,2' AND filterName = 'r';",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,2'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT tract,patch,filterName FROM DeepCoadd WHERE tract=0 AND patch='159,2' AND filterName='r'"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.tract, sce.patch FROM   DeepCoadd AS sce WHERE  sce.filterName = 'r' AND sce.tract = 0 AND sce.patch = '159,3';",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,3'"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.tract,sce.patch FROM DeepCoadd AS `sce` WHERE sce.filterName='r' AND sce.tract=0 AND sce.patch='159,3'"
    ),
    Antlr4TestQueries(
        "SELECT sce.DeepCoaddId, sce.filterName, sce.tract, sce.patch, sce.filterId, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.measuredFwhm FROM   DeepCoadd AS sce WHERE  sce.filterName = 'r' AND sce.tract = 0 AND sce.patch = '159,2';",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "DeepCoaddId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "measuredFwhm")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,2'"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.DeepCoaddId,sce.filterName,sce.tract,sce.patch,sce.filterId,sce.ra,sce.decl,sce.crpix1,sce.crpix2,sce.crval1,sce.crval2,sce.cd1_1,sce.cd1_2,sce.cd2_1,sce.cd2_2,sce.fluxMag0,sce.fluxMag0Sigma,sce.measuredFwhm FROM DeepCoadd AS `sce` WHERE sce.filterName='r' AND sce.tract=0 AND sce.patch='159,2'"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   DeepCoadd AS sce WHERE  sce.filterName = 'r' AND sce.tract = 0 AND sce.patch = '159,1';",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,1'"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM DeepCoadd AS `sce` WHERE sce.filterName='r' AND sce.tract=0 AND sce.patch='159,1'"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.id,  rom.nSrcMatches, s.flags_pixel_interpolated_center, s.flags_negative, s.flags_pixel_edge, s.centroid_sdss_flags, s.flags_pixel_saturated_center FROM   RunDeepSource AS s, DeepCoadd AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.coadd_id = sce.deepCoaddId) AND (s.id = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3') AND (s.id IN (1398582280195495, 1398582280195498, 1398582280195256))",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "gMag")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "isStar")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "nSrcMatches")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_interpolated_center")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_negative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_edge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "centroid_sdss_flags")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_saturated_center")), query::ValueExpr::NONE))),
            FromList(TableRef("", "RunDeepSource", "s"), TableRef("", "DeepCoadd", "sce"), TableRef("", "RefDeepSrcMatch", "rom"), TableRef("", "RefObject", "sro")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "coadd_id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "deepCoaddId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "deepSourceId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "refObjectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,3'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("1398582280195495"), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor("1398582280195498"), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor("1398582280195256"), query::ValueExpr::NONE))))))), PassTerm(")"))))),
                nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.tract,sce.patch,sro.gMag,sro.ra,sro.decl,sro.isStar,sro.refObjectId,s.id,rom.nSrcMatches,s.flags_pixel_interpolated_center,s.flags_negative,s.flags_pixel_edge,s.centroid_sdss_flags,s.flags_pixel_saturated_center FROM RunDeepSource AS `s`,DeepCoadd AS `sce`,RefDeepSrcMatch AS `rom`,RefObject AS `sro` WHERE (s.coadd_id=sce.deepCoaddId) AND (s.id=rom.deepSourceId) AND (rom.refObjectId=sro.refObjectId) AND (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,3') AND (s.id IN(1398582280195495,1398582280195498,1398582280195256))"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.id as sourceId,  rom.nSrcMatches, s.flags_pixel_interpolated_center, s.flags_negative, s.flags_pixel_edge, s.centroid_sdss_flags, s.flags_pixel_saturated_center FROM   RunDeepSource AS s, DeepCoadd AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.coadd_id = sce.deepCoaddId) AND (s.id = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3') AND (s.id = 1398582280194457) ORDER BY sourceId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "gMag")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "isStar")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE)),
                ValueExpr("sourceId", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "nSrcMatches")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_interpolated_center")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_negative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_edge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "centroid_sdss_flags")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_saturated_center")), query::ValueExpr::NONE))),
            FromList(TableRef("", "RunDeepSource", "s"), TableRef("", "DeepCoadd", "sce"), TableRef("", "RefDeepSrcMatch", "rom"), TableRef("", "RefObject", "sro")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "coadd_id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "deepCoaddId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "deepSourceId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "refObjectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,3'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1398582280194457"), query::ValueExpr::NONE))))))), PassTerm(")"))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.tract,sce.patch,sro.gMag,sro.ra,sro.decl,sro.isStar,sro.refObjectId,s.id AS `sourceId`,rom.nSrcMatches,s.flags_pixel_interpolated_center,s.flags_negative,s.flags_pixel_edge,s.centroid_sdss_flags,s.flags_pixel_saturated_center FROM RunDeepSource AS `s`,DeepCoadd AS `sce`,RefDeepSrcMatch AS `rom`,RefObject AS `sro` WHERE (s.coadd_id=sce.deepCoaddId) AND (s.id=rom.deepSourceId) AND (rom.refObjectId=sro.refObjectId) AND (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,3') AND (s.id=1398582280194457) ORDER BY sourceId"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName like '%' AND sce.field = 535 AND sce.camcol like '%' AND sce.run = 94;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE)))), BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run FROM Science_Ccd_Exposure AS `sce` WHERE sce.filterName LIKE '%' AND sce.field=535 AND sce.camcol LIKE '%' AND sce.run=94"
    ),
    Antlr4TestQueries(
        "SELECT sce.scienceCcdExposureId, sce.field, sce.camcol, sce.run, sce.filterId, sce.filterName, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 535 AND sce.camcol = 1 AND sce.run = 94;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fwhm")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.scienceCcdExposureId,sce.field,sce.camcol,sce.run,sce.filterId,sce.filterName,sce.ra,sce.decl,sce.crpix1,sce.crpix2,sce.crval1,sce.crval2,sce.cd1_1,sce.cd1_2,sce.cd2_1,sce.cd2_2,sce.fluxMag0,sce.fluxMag0Sigma,sce.fwhm FROM Science_Ccd_Exposure AS `sce` WHERE sce.filterName='g' AND sce.field=535 AND sce.camcol=1 AND sce.run=94"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 535 AND sce.camcol = 1 AND sce.run = 94;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run FROM Science_Ccd_Exposure AS `sce` WHERE sce.filterName='g' AND sce.field=535 AND sce.camcol=1 AND sce.run=94"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  sce.filterName = 'g' AND sce.field = 535 AND sce.camcol = 1 AND sce.run = 94;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM Science_Ccd_Exposure AS `sce` WHERE sce.filterName='g' AND sce.field=535 AND sce.camcol=1 AND sce.run=94"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Science_Ccd_Exposure_Metadata WHERE scienceCcdExposureId=7202320671 AND stringValue=''",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure_Metadata", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "scienceCcdExposureId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("7202320671"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "stringValue")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("''"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Science_Ccd_Exposure_Metadata WHERE scienceCcdExposureId=7202320671 AND stringValue=''"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepForcedSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepForcedSource", "s"), TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "scienceCcdExposureId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run,s.deepForcedSourceId,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepForcedSource AS `s`,Science_Ccd_Exposure AS `sce` WHERE (s.scienceCcdExposureId=sce.scienceCcdExposureId) AND (sce.filterName='g') AND (sce.field=535) AND (sce.camcol=1) AND (sce.run=94)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.tract, sce.patch, s.deepSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepSource AS s, DeepCoadd AS sce WHERE  (s.deepCoaddId = sce.deepCoaddId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,2');",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepSource", "s"), TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepCoaddId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "deepCoaddId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,2'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.tract,sce.patch,s.deepSourceId,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepSource AS `s`,DeepCoadd AS `sce` WHERE (s.deepCoaddId=sce.deepCoaddId) AND (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,2')"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   DeepCoadd AS sce WHERE  sce.filterName = 'r' AND sce.tract = 0 AND sce.patch = '159,1';",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,1'"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM DeepCoadd AS `sce` WHERE sce.filterName='r' AND sce.tract=0 AND sce.patch='159,1'"
    ),
    Antlr4TestQueries(
        "SELECT deepForcedSourceId, scienceCcdExposureId, filterId FROM DeepForcedSource ORDER BY deepForcedSourceId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "deepForcedSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "scienceCcdExposureId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepForcedSource", "")), nullptr,
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "deepForcedSourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT deepForcedSourceId,scienceCcdExposureId,filterId FROM DeepForcedSource ORDER BY deepForcedSourceId"
    ),
    Antlr4TestQueries(
        "SELECT objectId, iauId, ra_PS FROM   Object WHERE  objectId = 433327840428032",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iauId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("433327840428032"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,iauId,ra_PS FROM Object WHERE objectId=433327840428032"
    ),
    Antlr4TestQueries(
        "SELECT * FROM   Object WHERE  objectId = 430213989000",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430213989000"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE objectId=430213989000"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 433327840428032",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "raRange")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "declRange")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("433327840428032"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.raRange,o.declRange FROM Object AS `o` JOIN Source AS `s` USING(objectId) WHERE o.objectId=433327840428032"
    ),
    Antlr4TestQueries(
        "SELECT sourceId, scienceCcdExposureId, filterId FROM   Source WHERE  sourceId = 2867930096075697",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "scienceCcdExposureId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("2867930096075697"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sourceId,scienceCcdExposureId,filterId FROM Source WHERE sourceId=2867930096075697"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(70, 3, 75, 3.5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.9"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.0"), query::ValueExpr::NONE)))))), AreaRestrictorBox("70", "3", "75", "3.5")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_box(70,3,75,3.5) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.9 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 1.0"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_circle(72.5, 3.25, 0.6) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.9"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.0"), query::ValueExpr::NONE)))))), AreaRestrictorCircle("72.5", "3.25", "0.6")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_circle(72.5,3.25,0.6) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.9 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 1.0"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_ellipse(72.5, 3.25, 6000, 1700, 0.2) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.9"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.0"), query::ValueExpr::NONE)))))), AreaRestrictorEllipse("72.5", "3.25", "6000", "1700", "0.2")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_ellipse(72.5,3.25,6000,1700,0.2) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.9 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 1.0"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_poly( 70, 3, 75, 3.5, 70, 4.0) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.9"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.0"), query::ValueExpr::NONE)))))), AreaRestrictorPoly({"70", "3", "75", "3.5", "70", "4.0"})), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_poly(70,3,75,3.5,70,4.0) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.9 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 1.0"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM   Object WHERE qserv_areaspec_box(0, -6, 4, -5) AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.2 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.2",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE)))))), AreaRestrictorBox("0", "-6", "4", "-5")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE qserv_areaspec_box(0,-6,4,-5) scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.1 AND 0.2 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 0.2"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Object WHERE qserv_areaspec_box(0, 0, 3, 10) ORDER BY objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "3", "10")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE qserv_areaspec_box(0,0,3,10) ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "SELECT  objectId FROM    Object WHERE   scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS) <  2.0 AND  scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) <  0.1 AND  scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) > -0.8 AND  scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) <  1.4",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "uFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("2.0"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("-0.8"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1.4"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS))<2.0 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))<0.1 AND (scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS))>-0.8 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS))<1.4"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS OBJ_COUNT FROM Object",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) AS `OBJ_COUNT` FROM Object"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS OBJ_COUNT FROM   Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5 AND scisql_fluxToAbMag(gFlux_PS) - scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.3 AND 0.4 AND scisql_fluxToAbMag(iFlux_PS) - scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 0.12",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("21"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("21.5"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.3"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.4"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.12"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) AS `OBJ_COUNT` FROM Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)) BETWEEN 0.3 AND 0.4 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS)) BETWEEN 0.1 AND 0.12"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) AS OBJ_COUNT FROM Object WHERE gFlux_PS>1e-25",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1e-25"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT COUNT(*) AS `OBJ_COUNT` FROM Object WHERE gFlux_PS>1e-25"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS, decl_PS, uFlux_PS, gFlux_PS, rFlux_PS, iFlux_PS, zFlux_PS, yFlux_PS FROM Object WHERE scisql_fluxToAbMag(iFlux_PS) - scisql_fluxToAbMag(zFlux_PS) > 0.08",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "uFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "yFlux_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.08"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS,decl_PS,uFlux_PS,gFlux_PS,rFlux_PS,iFlux_PS,zFlux_PS,yFlux_PS FROM Object WHERE (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS))>0.08"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS OBJ_COUNT FROM Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND  decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 and 21.5",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("21"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("21.5"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) AS `OBJ_COUNT` FROM Object WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS, decl_PS, scisql_fluxToAbMag(zFlux_PS) AS fluxToAbMag FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("fluxToAbMag", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS,decl_PS,scisql_fluxToAbMag(zFlux_PS) AS `fluxToAbMag` FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS, decl_PS, scisql_fluxToAbMag(zFlux_PS) FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("20"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("24"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS,decl_PS,scisql_fluxToAbMag(zFlux_PS) FROM Object WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS OBJ_COUNT FROM Object WHERE scisql_angSep(ra_PS, decl_PS, 0., 0.) < 0.2",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("OBJ_COUNT", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0."), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0."), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) AS `OBJ_COUNT` FROM Object WHERE scisql_angSep(ra_PS,decl_PS,0.,0.)<0.2"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM Source JOIN Object USING(objectId) WHERE ra_PS BETWEEN 1.28 AND 1.38 AND  decl_PS BETWEEN 3.18 AND 3.34",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "", JoinRef(TableRef("", "Object", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Source JOIN Object USING(objectId) WHERE ra_PS BETWEEN 1.28 AND 1.38 AND decl_PS BETWEEN 3.18 AND 3.34"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl FROM   Object o JOIN   Source s USING (objectId) WHERE  o.objectId = 433327840429024 AND    o.latestObsTime BETWEEN s.taiMidPoint - 300 AND s.taiMidPoint + 300",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("433327840429024"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "latestObsTime")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "taiMidPoint")), query::ValueExpr::MINUS), FactorOp(ValueFactor("300"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "taiMidPoint")), query::ValueExpr::PLUS), FactorOp(ValueFactor("300"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl FROM Object AS `o` JOIN Source AS `s` USING(objectId) WHERE o.objectId=433327840429024 AND o.latestObsTime BETWEEN(s.taiMidPoint-300) AND (s.taiMidPoint+300)"
    ),
    Antlr4TestQueries(
        "SELECT taiMidPoint, psfFlux, psfFluxSigma, ra, decl FROM   Source JOIN   Filter USING (filterId) WHERE  objectId = 402412665835716 AND filterName = 'r'",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "taiMidPoint")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "", JoinRef(TableRef("", "Filter", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "filterId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("402412665835716"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT taiMidPoint,psfFlux,psfFluxSigma,ra,decl FROM Source JOIN Filter USING(filterId) WHERE objectId=402412665835716 AND filterName='r'"
    ),
    Antlr4TestQueries(
        "SELECT sourceId, objectId, blobField FROM Source WHERE objectId = 386942193651348 ORDER BY sourceId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "blobField")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sourceId,objectId,blobField FROM Source WHERE objectId=386942193651348 ORDER BY sourceId"
    ),
    Antlr4TestQueries(
        "SELECT sce.visit, sce.raftName, sce.ccdName, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, rom.nSrcMatches, s.sourceId,s.ra,s.decl,s.xAstrom,s.yAstrom,s.psfFlux,s.psfFluxSigma, s.apFlux,s.apFluxSigma,s.flux_ESG,s.flux_ESG_Sigma,s.flux_Gaussian, s.flux_Gaussian_Sigma,s.ixx,s.iyy,s.ixy,s.psfIxx,s.psfIxxSigma, s.psfIyy,s.psfIyySigma,s.psfIxy,s.psfIxySigma,s.resolution_SG, s.e1_SG,s.e1_SG_Sigma,s.e2_SG,s.e2_SG_Sigma,s.shear1_SG,s.shear1_SG_Sigma, s.shear2_SG,s.shear2_SG_Sigma,s.sourceWidth_SG,s.sourceWidth_SG_Sigma, s.flagForDetection FROM Source AS s, Science_Ccd_Exposure AS sce, RefSrcMatch AS rom, SimRefObject AS sro WHERE (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (s.sourceId = rom.sourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.visit = 888241840) AND (sce.raftName = '1,0') AND (sce.ccdName like '%')",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "raftName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ccdName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "gMag")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "isStar")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "nSrcMatches")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "xAstrom")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "yAstrom")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flux_ESG")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flux_ESG_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flux_Gaussian")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flux_Gaussian_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ixx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "iyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ixy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfIxxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfIyySigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfIxySigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "resolution_SG")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "e1_SG")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "e1_SG_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "e2_SG")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "e2_SG_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shear1_SG")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shear1_SG_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shear2_SG")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shear2_SG_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "sourceWidth_SG")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "sourceWidth_SG_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagForDetection")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s"), TableRef("", "Science_Ccd_Exposure", "sce"), TableRef("", "RefSrcMatch", "rom"), TableRef("", "SimRefObject", "sro")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "scienceCcdExposureId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "sourceId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "sourceId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "refObjectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("888241840"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "raftName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'1,0'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ccdName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.visit,sce.raftName,sce.ccdName,sro.gMag,sro.ra,sro.decl,sro.isStar,sro.refObjectId,rom.nSrcMatches,s.sourceId,s.ra,s.decl,s.xAstrom,s.yAstrom,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.flux_ESG,s.flux_ESG_Sigma,s.flux_Gaussian,s.flux_Gaussian_Sigma,s.ixx,s.iyy,s.ixy,s.psfIxx,s.psfIxxSigma,s.psfIyy,s.psfIyySigma,s.psfIxy,s.psfIxySigma,s.resolution_SG,s.e1_SG,s.e1_SG_Sigma,s.e2_SG,s.e2_SG_Sigma,s.shear1_SG,s.shear1_SG_Sigma,s.shear2_SG,s.shear2_SG_Sigma,s.sourceWidth_SG,s.sourceWidth_SG_Sigma,s.flagForDetection FROM Source AS `s`,Science_Ccd_Exposure AS `sce`,RefSrcMatch AS `rom`,SimRefObject AS `sro` WHERE (s.scienceCcdExposureId=sce.scienceCcdExposureId) AND (s.sourceId=rom.sourceId) AND (rom.refObjectId=sro.refObjectId) AND (sce.visit=888241840) AND (sce.raftName='1,0') AND (sce.ccdName LIKE '%')"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), chunkId FROM Object GROUP BY chunkId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("n", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("AVG", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("AVG", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT count(*) AS `n`,AVG(ra_PS),AVG(decl_PS),chunkId FROM Object GROUP BY chunkId"
    ),
    Antlr4TestQueries(
        "SELECT o1.ra_PS,o2.ra_PS FROM Object o1, Object o2 WHERE o1.objectid = 402391191015221 AND o2.objectid = 390030275138483 ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectid")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("402391191015221"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectid")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("390030275138483"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.ra_PS,o2.ra_PS FROM Object AS `o1`,Object AS `o2` WHERE o1.objectid=402391191015221 AND o2.objectid=390030275138483"
    ),
    Antlr4TestQueries(
        "SELECT o.ra_PS,o.decl_PS,o.ra_PS FROM Object o WHERE o.objectid = 402391191015221 ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectid")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("402391191015221"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o.ra_PS,o.decl_PS,o.ra_PS FROM Object AS `o` WHERE o.objectid=402391191015221"
    ),
    Antlr4TestQueries(
        "SELECT o.foobar FROM Object o WHERE o.objectid = 402391191015221 ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "foobar")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectid")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("402391191015221"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o.foobar FROM Object AS `o` WHERE o.objectid=402391191015221"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Object WHERE qserv_areaspec_box(0.,1.,0.,1.) ORDER BY ra_PS",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0.", "1.", "0.", "1.")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE qserv_areaspec_box(0.,1.,0.,1.) ORDER BY ra_PS"
    ),
    Antlr4TestQueries(
        "select count(*) from Sources;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Sources", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Sources"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM Object WHERE qserv_areaspec_box(0.1, -6, 4, 6) LIMIT 10",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0.1", "-6", "4", "6")), nullptr, nullptr, nullptr, 0, 10);},
        "SELECT objectId FROM Object WHERE qserv_areaspec_box(0.1,-6,4,6) LIMIT 10"
    ),
    Antlr4TestQueries(
        "SELECT COUNT(*) FROM   Object WHERE qserv_areaspec_box(355, 0, 356, 1) LIMIT 10",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("355", "0", "356", "1")), nullptr, nullptr, nullptr, 0, 10);},
        "SELECT COUNT(*) FROM Object WHERE qserv_areaspec_box(355,0,356,1) LIMIT 10"
    ),
    Antlr4TestQueries(
        "SELECT objectId FROM   Source s JOIN   Science_Ccd_Exposure sce USING (scienceCcdExposureId) WHERE  sce.visit IN (885449631,886257441,886472151) ORDER BY objectId LIMIT 10",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s", JoinRef(TableRef("", "Science_Ccd_Exposure", "sce"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "scienceCcdExposureId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("885449631"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("886257441"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("886472151"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, 10);},
        "SELECT objectId FROM Source AS `s` JOIN Science_Ccd_Exposure AS `sce` USING(scienceCcdExposureId) WHERE sce.visit IN(885449631,886257441,886472151) ORDER BY objectId LIMIT 10"
    ),
    Antlr4TestQueries(
        "SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) FROM   Source JOIN   Object USING(objectId) JOIN   Filter USING(filterId) WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' ORDER BY objectId, taiMidPoint ASC",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "taiMidPoint")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "psfFlux")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "", JoinRef(TableRef("", "Object", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)), JoinRef(TableRef("", "Filter", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "filterId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))))), AreaRestrictorBox("355", "0", "360", "20")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, ""), OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "taiMidPoint")), query::ValueExpr::NONE)), query::OrderByTerm::ASC, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId,taiMidPoint,scisql_fluxToAbMag(psfFlux) FROM Source JOIN Object USING(objectId) JOIN Filter USING(filterId) WHERE qserv_areaspec_box(355,0,360,20) filterName='g' ORDER BY objectId, taiMidPoint ASC"
    ),
    Antlr4TestQueries(
        "SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM   Object o1, Object o2 WHERE  o1.ra_PS BETWEEN 1.28 AND 1.38 AND  o1.decl_PS BETWEEN 3.18 AND 3.34 AND  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND  o1.objectId <> o2.objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("objId1", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("objId2", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("distance", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId AS `objId1`,o2.objectId AS `objId2`,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS `distance` FROM Object AS `o1`,Object AS `o2` WHERE o1.ra_PS BETWEEN 1.28 AND 1.38 AND o1.decl_PS BETWEEN 3.18 AND 3.34 AND scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS)<1 AND o1.objectId<>o2.objectId"
    ),
    Antlr4TestQueries(
        "SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), objectId, chunkId FROM Object GROUP BY chunkId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("n", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("AVG", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("AVG", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT count(*) AS `n`,AVG(ra_PS),AVG(decl_PS),objectId,chunkId FROM Object GROUP BY chunkId"
    ),
    Antlr4TestQueries(
        "SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM   Object o1, Object o2 WHERE o1.ra_PS BETWEEN 1.28 AND 1.38 AND o1.decl_PS BETWEEN 3.18 AND 3.34 AND o2.ra_PS BETWEEN 1.28 AND 1.38 AND o2.decl_PS BETWEEN 3.18 AND 3.34 AND o1.objectId <> o2.objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("objId1", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("objId2", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("distance", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.28"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.38"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3.18"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("3.34"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId AS `objId1`,o2.objectId AS `objId2`,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS `distance` FROM Object AS `o1`,Object AS `o2` WHERE o1.ra_PS BETWEEN 1.28 AND 1.38 AND o1.decl_PS BETWEEN 3.18 AND 3.34 AND o2.ra_PS BETWEEN 1.28 AND 1.38 AND o2.decl_PS BETWEEN 3.18 AND 3.34 AND o1.objectId<>o2.objectId"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Object WHERE qserv_areaspec_box(1.28,1.38,3.18,3.34) ORDER BY ra_PS",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("1.28", "1.38", "3.18", "3.34")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE qserv_areaspec_box(1.28,1.38,3.18,3.34) ORDER BY ra_PS"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName like '%') AND (sce.field = 535) AND (sce.camcol like '%') AND (sce.run = 94);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run FROM Science_Ccd_Exposure AS `sce` WHERE (sce.filterName LIKE '%') AND (sce.field=535) AND (sce.camcol LIKE '%') AND (sce.run=94)"
    ),
    Antlr4TestQueries(
        "SELECT sce.scienceCcdExposureId, sce.filterName, sce.field, sce.camcol, sce.run, sce.filterId, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fwhm")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.scienceCcdExposureId,sce.filterName,sce.field,sce.camcol,sce.run,sce.filterId,sce.ra,sce.decl,sce.crpix1,sce.crpix2,sce.crval1,sce.crval2,sce.cd1_1,sce.cd1_2,sce.cd2_1,sce.cd2_2,sce.fluxMag0,sce.fluxMag0Sigma,sce.fwhm FROM Science_Ccd_Exposure AS `sce` WHERE (sce.filterName='g') AND (sce.field=535) AND (sce.camcol=1) AND (sce.run=94)"
    ),
    Antlr4TestQueries(
        "SELECT distinct run, field FROM   Science_Ccd_Exposure WHERE  (run = 94) AND (field = 535);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "field")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT run,field FROM Science_Ccd_Exposure WHERE (run=94) AND (field=535)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId)",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepForcedSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepForcedSource", "s"), TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "scienceCcdExposureId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run,s.deepForcedSourceId,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepForcedSource AS `s`,Science_Ccd_Exposure AS `sce` WHERE (s.scienceCcdExposureId=sce.scienceCcdExposureId)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepForcedSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepForcedSource", "s"), TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "scienceCcdExposureId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run,s.deepForcedSourceId,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepForcedSource AS `s`,Science_Ccd_Exposure AS `sce` WHERE (s.scienceCcdExposureId=sce.scienceCcdExposureId) AND (sce.filterName='g') AND (sce.field=535) AND (sce.camcol=1) AND (sce.run=94)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepForcedSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (sce.filterName = 'g') AND (sce.field = 793) AND (sce.camcol = 1) AND (sce.run = 5924) ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepForcedSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepForcedSource", "s"), TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "scienceCcdExposureId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("793"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("5924"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run,s.deepForcedSourceId,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepForcedSource AS `s`,Science_Ccd_Exposure AS `sce` WHERE (s.scienceCcdExposureId=sce.scienceCcdExposureId) AND (sce.filterName='g') AND (sce.field=793) AND (sce.camcol=1) AND (sce.run=5924)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 670) AND (sce.camcol = 2) AND (sce.run = 7202) ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("670"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("2"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("7202"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run FROM Science_Ccd_Exposure AS `sce` WHERE (sce.filterName='g') AND (sce.field=670) AND (sce.camcol=2) AND (sce.run=7202)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 670) AND (sce.camcol = 2) AND (sce.run = 7202) ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("670"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("2"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("7202"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM Science_Ccd_Exposure AS `sce` WHERE (sce.filterName='g') AND (sce.field=670) AND (sce.camcol=2) AND (sce.run=7202)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run, sro.gMag, sro.isStar, sro.refObjectId, s.deepForcedSourceId,  rom.nSrcMatches,s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (s.deepForcedSourceId = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'g') AND (sce.field = 670) AND (sce.camcol = 2) AND (sce.run = 7202) ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "gMag")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "isStar")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepForcedSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "nSrcMatches")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepForcedSource", "s"), TableRef("", "Science_Ccd_Exposure", "sce"), TableRef("", "RefDeepSrcMatch", "rom"), TableRef("", "RefObject", "sro")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "scienceCcdExposureId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepForcedSourceId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "deepSourceId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "refObjectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("670"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("2"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("7202"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run,sro.gMag,sro.isStar,sro.refObjectId,s.deepForcedSourceId,rom.nSrcMatches,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepForcedSource AS `s`,Science_Ccd_Exposure AS `sce`,RefDeepSrcMatch AS `rom`,RefObject AS `sro` WHERE (s.scienceCcdExposureId=sce.scienceCcdExposureId) AND (s.deepForcedSourceId=rom.deepSourceId) AND (rom.refObjectId=sro.refObjectId) AND (sce.filterName='g') AND (sce.field=670) AND (sce.camcol=2) AND (sce.run=7202)"
    ),
    Antlr4TestQueries(
        "SELECT DISTINCT tract, patch, filterName FROM   DeepCoadd WHERE  (tract = 0) AND (patch = '159,2') AND (filterName = 'r');",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,2'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT tract,patch,filterName FROM DeepCoadd WHERE (tract=0) AND (patch='159,2') AND (filterName='r')"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.tract, sce.patch FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3');",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,3'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.tract,sce.patch FROM DeepCoadd AS `sce` WHERE (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,3')"
    ),
    Antlr4TestQueries(
        "SELECT sce.DeepCoaddId, sce.filterName, sce.tract, sce.patch, sce.filterId, sce.filterName, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.measuredFwhm FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,2');",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "DeepCoaddId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "measuredFwhm")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,2'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.DeepCoaddId,sce.filterName,sce.tract,sce.patch,sce.filterId,sce.filterName,sce.ra,sce.decl,sce.crpix1,sce.crpix2,sce.crval1,sce.crval2,sce.cd1_1,sce.cd1_2,sce.cd2_1,sce.cd2_2,sce.fluxMag0,sce.fluxMag0Sigma,sce.measuredFwhm FROM DeepCoadd AS `sce` WHERE (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,2')"
    ),
    Antlr4TestQueries(
        "SELECT sce.DeepCoaddId, sce.filterName, sce.tract, sce.patch, sce.filterId, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.measuredFwhm FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,2');",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "DeepCoaddId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "measuredFwhm")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,2'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.DeepCoaddId,sce.filterName,sce.tract,sce.patch,sce.filterId,sce.ra,sce.decl,sce.crpix1,sce.crpix2,sce.crval1,sce.crval2,sce.cd1_1,sce.cd1_2,sce.cd2_1,sce.cd2_2,sce.fluxMag0,sce.fluxMag0Sigma,sce.measuredFwhm FROM DeepCoadd AS `sce` WHERE (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,2')"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.tract, sce.patch, s.deepSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepSource AS s, DeepCoadd AS sce WHERE  (s.deepCoaddId = sce.deepCoaddId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,2');",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepSource", "s"), TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepCoaddId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "deepCoaddId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,2'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.tract,sce.patch,s.deepSourceId,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepSource AS `s`,DeepCoadd AS `sce` WHERE (s.deepCoaddId=sce.deepCoaddId) AND (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,2')"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   DeepCoadd AS sce WHERE  (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,1');",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,1'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM DeepCoadd AS `sce` WHERE (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,1')"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.id,  rom.nSrcMatches, s.flags_pixel_interpolated_center, s.flags_negative, s.flags_pixel_edge, s.centroid_sdss_flags, s.flags_pixel_saturated_center FROM   RunDeepSource AS s, DeepCoadd AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.coadd_id = sce.deepCoaddId) AND (s.id = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3') AND (s.id = 1398582280194457) ORDER BY s.id",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "gMag")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "isStar")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "nSrcMatches")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_interpolated_center")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_negative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_edge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "centroid_sdss_flags")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flags_pixel_saturated_center")), query::ValueExpr::NONE))),
            FromList(TableRef("", "RunDeepSource", "s"), TableRef("", "DeepCoadd", "sce"), TableRef("", "RefDeepSrcMatch", "rom"), TableRef("", "RefObject", "sro")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "coadd_id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "deepCoaddId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "deepSourceId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "refObjectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,3'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1398582280194457"), query::ValueExpr::NONE))))))), PassTerm(")"))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "id")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.tract,sce.patch,sro.gMag,sro.ra,sro.decl,sro.isStar,sro.refObjectId,s.id,rom.nSrcMatches,s.flags_pixel_interpolated_center,s.flags_negative,s.flags_pixel_edge,s.centroid_sdss_flags,s.flags_pixel_saturated_center FROM RunDeepSource AS `s`,DeepCoadd AS `sce`,RefDeepSrcMatch AS `rom`,RefObject AS `sro` WHERE (s.coadd_id=sce.deepCoaddId) AND (s.id=rom.deepSourceId) AND (rom.refObjectId=sro.refObjectId) AND (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,3') AND (s.id=1398582280194457) ORDER BY s.id"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.tract, sce.patch, sro.gMag, sro.ra, sro.decl, sro.isStar, sro.refObjectId, s.deepSourceId,  rom.nSrcMatches,s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepSource AS s, DeepCoadd AS sce, RefDeepSrcMatch AS rom, RefObject AS sro WHERE  (s.deepCoaddId = sce.deepCoaddId) AND (s.deepSourceId = rom.deepSourceId) AND (rom.refObjectId = sro.refObjectId) AND (sce.filterName = 'r') AND (sce.tract = 0) AND (sce.patch = '159,3');",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "gMag")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "isStar")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "nSrcMatches")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepSource", "s"), TableRef("", "DeepCoadd", "sce"), TableRef("", "RefDeepSrcMatch", "rom"), TableRef("", "RefObject", "sro")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepCoaddId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "deepCoaddId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepSourceId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "deepSourceId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "rom", "refObjectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sro", "refObjectId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'r'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "tract")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "patch")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'159,3'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.tract,sce.patch,sro.gMag,sro.ra,sro.decl,sro.isStar,sro.refObjectId,s.deepSourceId,rom.nSrcMatches,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepSource AS `s`,DeepCoadd AS `sce`,RefDeepSrcMatch AS `rom`,RefObject AS `sro` WHERE (s.deepCoaddId=sce.deepCoaddId) AND (s.deepSourceId=rom.deepSourceId) AND (rom.refObjectId=sro.refObjectId) AND (sce.filterName='r') AND (sce.tract=0) AND (sce.patch='159,3')"
    ),
    Antlr4TestQueries(
        "SELECT sce.scienceCcdExposureId, sce.field, sce.camcol, sce.run, sce.filterId, sce.filterName, sce.ra, sce.decl, sce.crpix1, sce.crpix2, sce.crval1, sce.crval2, sce.cd1_1, sce.cd1_2, sce.cd2_1, sce.cd2_2, sce.fluxMag0, sce.fluxMag0Sigma, sce.fwhm FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crpix2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "crval2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd1_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_1")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "cd2_2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fluxMag0Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "fwhm")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.scienceCcdExposureId,sce.field,sce.camcol,sce.run,sce.filterId,sce.filterName,sce.ra,sce.decl,sce.crpix1,sce.crpix2,sce.crval1,sce.crval2,sce.cd1_1,sce.cd1_2,sce.cd2_1,sce.cd2_2,sce.fluxMag0,sce.fluxMag0Sigma,sce.fwhm FROM Science_Ccd_Exposure AS `sce` WHERE (sce.filterName='g') AND (sce.field=535) AND (sce.camcol=1) AND (sce.run=94)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run, s.deepSourceId, s.ra, s.decl, s.x, s.y, s.psfFlux, s.psfFluxSigma, s.apFlux, s.apFluxSigma, s.modelFlux, s.modelFluxSigma, s.instFlux, s.instFluxSigma, s.shapeIxx, s.shapeIyy, s.shapeIxy, s.flagPixInterpCen, s.flagNegative, s.flagPixEdge, s.flagBadCentroid, s.flagPixSaturCen, s.extendedness FROM   DeepForcedSource AS s, Science_Ccd_Exposure AS sce WHERE  (s.scienceCcdExposureId = sce.scienceCcdExposureId) AND (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "deepSourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "x")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "y")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "psfFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "apFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "modelFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFlux")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "instFluxSigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxx")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIyy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "shapeIxy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixInterpCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagNegative")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixEdge")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagBadCentroid")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "flagPixSaturCen")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "extendedness")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepForcedSource", "s"), TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "scienceCcdExposureId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "scienceCcdExposureId")), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run,s.deepSourceId,s.ra,s.decl,s.x,s.y,s.psfFlux,s.psfFluxSigma,s.apFlux,s.apFluxSigma,s.modelFlux,s.modelFluxSigma,s.instFlux,s.instFluxSigma,s.shapeIxx,s.shapeIyy,s.shapeIxy,s.flagPixInterpCen,s.flagNegative,s.flagPixEdge,s.flagBadCentroid,s.flagPixSaturCen,s.extendedness FROM DeepForcedSource AS `s`,Science_Ccd_Exposure AS `sce` WHERE (s.scienceCcdExposureId=sce.scienceCcdExposureId) AND (sce.filterName='g') AND (sce.field=535) AND (sce.camcol=1) AND (sce.run=94)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterName, sce.field, sce.camcol, sce.run FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterName,sce.field,sce.camcol,sce.run FROM Science_Ccd_Exposure AS `sce` WHERE (sce.filterName='g') AND (sce.field=535) AND (sce.camcol=1) AND (sce.run=94)"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM   Science_Ccd_Exposure AS sce WHERE  (sce.filterName = 'g') AND (sce.field = 535) AND (sce.camcol = 1) AND (sce.run = 94);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("535"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "camcol")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM Science_Ccd_Exposure AS `sce` WHERE (sce.filterName='g') AND (sce.field=535) AND (sce.camcol=1) AND (sce.run=94)"
    ),
    Antlr4TestQueries(
        "SELECT distinct run, field FROM   Science_Ccd_Exposure WHERE  (run = 94) AND (field = 536);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "run")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "field")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "run")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("94"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "field")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("536"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT run,field FROM Science_Ccd_Exposure WHERE (run=94) AND (field=536)"
    ),
    Antlr4TestQueries(
        "SELECT DISTINCT tract,patch,filterName FROM DeepCoadd ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "tract")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "patch")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "DeepCoadd", "")), nullptr, nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT tract,patch,filterName FROM DeepCoadd"
    ),
    Antlr4TestQueries(
        "SELECT o1.objectId AS objId1, o2.objectId AS objId2, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM Object o1, Object o2 WHERE qserv_areaspec_box(0, 0, 0.2, 1) AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1 AND o1.objectId <> o2.objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("objId1", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("objId2", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("distance", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS,
                    CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION,
                        FuncExpr("scisql_angSep",
                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                            ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                        query::CompPredicate::LESS_THAN_OP,
                        ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE)))),
                BoolFactor(IS, CompPredicate(
                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                    query::CompPredicate::NOT_EQUALS_OP,
                    ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE))))
                )),
                AreaRestrictorBox("0", "0", "0.2", "1")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId AS `objId1`,o2.objectId AS `objId2`,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS `distance` FROM Object AS `o1`,Object AS `o2` WHERE qserv_areaspec_box(0,0,0.2,1) scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS)<1 AND o1.objectId<>o2.objectId"
    ),
    Antlr4TestQueries(
        "select sum(pm_declErr),chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0 GROUP BY chunkId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("sum", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "pm_declErr")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)),
                ValueExpr("bmf2", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("avg", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bMagF2")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bMagF")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("20.0"), query::ValueExpr::NONE))))))), nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT sum(pm_declErr),chunkId,avg(bMagF2) AS `bmf2` FROM LSST.Object WHERE bMagF>20.0 GROUP BY chunkId"
    ),
    Antlr4TestQueries(
        "select chunkId, avg(bMagF2) bmf2 from LSST.Object where bMagF > 20.0;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)),
                ValueExpr("bmf2", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("avg", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bMagF2")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bMagF")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("20.0"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT chunkId,avg(bMagF2) AS `bmf2` FROM LSST.Object WHERE bMagF>20.0"
    ),
    Antlr4TestQueries(
        "select * from Object where objectIdObjTest between 386942193651347 and 386942193651349;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectIdObjTest")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("386942193651347"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("386942193651349"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE objectIdObjTest BETWEEN 386942193651347 AND 386942193651349"
    ),
    Antlr4TestQueries(
        "select * from Object where someField between 386942193651347 and 386942193651349;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "someField")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("386942193651347"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("386942193651349"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE someField BETWEEN 386942193651347 AND 386942193651349"
    ),
    Antlr4TestQueries(
        "select * from Object where objectIdObjTest between 38 and 40 and objectIdObjTest IN (10, 30, 70);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectIdObjTest")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("38"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("40"), query::ValueExpr::NONE)))), BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectIdObjTest")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("10"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("30"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("70"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE objectIdObjTest BETWEEN 38 AND 40 AND objectIdObjTest IN(10,30,70)"
    ),
    Antlr4TestQueries(
        "select * from Object o, Source s where o.objectIdObjTest between 38 and 40 AND s.objectIdSourceTest IN (10, 30, 70);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o"), TableRef("", "Source", "s")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectIdObjTest")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("38"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("40"), query::ValueExpr::NONE)))), BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "objectIdSourceTest")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("10"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("30"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("70"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object AS `o`,Source AS `s` WHERE o.objectIdObjTest BETWEEN 38 AND 40 AND s.objectIdSourceTest IN(10,30,70)"
    ),
    Antlr4TestQueries(
        "select chunkId as f1, pm_declErr AS f1 from LSST.Object where bMagF > 20.0 GROUP BY chunkId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("f1", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)),
                ValueExpr("f1", FactorOp(ValueFactor(ColumnRef("", "", "pm_declErr")), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bMagF")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("20.0"), query::ValueExpr::NONE))))))), nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT chunkId AS `f1`,pm_declErr AS `f1` FROM LSST.Object WHERE bMagF>20.0 GROUP BY chunkId"
    ),
    Antlr4TestQueries(
        "select chunkId, CHUNKID from LSST.Object where bMagF > 20.0 GROUP BY chunkId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "CHUNKID")), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bMagF")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("20.0"), query::ValueExpr::NONE))))))), nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT chunkId,CHUNKID FROM LSST.Object WHERE bMagF>20.0 GROUP BY chunkId"
    ),
    Antlr4TestQueries(
        "select sum(pm_declErr), chunkId as f1, chunkId AS f1, avg(pm_declErr) from LSST.Object where bMagF > 20.0 GROUP BY chunkId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("sum", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "pm_declErr")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("f1", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)),
                ValueExpr("f1", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("avg", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "pm_declErr")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bMagF")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("20.0"), query::ValueExpr::NONE))))))), nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT sum(pm_declErr),chunkId AS `f1`,chunkId AS `f1`,avg(pm_declErr) FROM LSST.Object WHERE bMagF>20.0 GROUP BY chunkId"
    ),
    Antlr4TestQueries(
        "select pm_declErr, chunkId, ra_Test from LSST.Object where bMagF > 20.0 GROUP BY chunkId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "pm_declErr")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_Test")), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bMagF")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("20.0"), query::ValueExpr::NONE))))))), nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "chunkId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT pm_declErr,chunkId,ra_Test FROM LSST.Object WHERE bMagF>20.0 GROUP BY chunkId"
    ),
    Antlr4TestQueries(
        "SELECT o1.objectId, o2.objectId, scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance FROM Object o1, Object o2 WHERE scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.05 AND  o1.objectId <> o2.objectId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("distance", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.05"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId,o2.objectId,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS `distance` FROM Object AS `o1`,Object AS `o2` WHERE scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS)<0.05 AND o1.objectId<>o2.objectId"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Object WHERE someField > 5.0;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "someField")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("5.0"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE someField>5.0"
    ),
    Antlr4TestQueries(
        "SELECT * FROM LSST.Object WHERE someField > 5.0;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "someField")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("5.0"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM LSST.Object WHERE someField>5.0"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Filter WHERE filterId=4;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("4"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Filter WHERE filterId=4"
    ),
    Antlr4TestQueries(
        "select * from LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 and decl_PS between 1.6 and 1.7 limit 2;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("150"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("150.2"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.6"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.7"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, 2);},
        "SELECT * FROM LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 AND decl_PS BETWEEN 1.6 AND 1.7 LIMIT 2"
    ),
    Antlr4TestQueries(
        "select * from LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 and decl_PS between 1.6 and 1.7 ORDER BY objectId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("150"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("150.2"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("1.6"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("1.7"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT * FROM LSST.Object WHERE ra_PS BETWEEN 150 AND 150.2 AND decl_PS BETWEEN 1.6 AND 1.7 ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "select * from Object where qserv_areaspec_box(0,0,1,1);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "1", "1")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE qserv_areaspec_box(0,0,1,1)"
    ),
    Antlr4TestQueries(
        "select count(*) from Object as o1, Object as o2 where qserv_areaspec_box(6,6,7,7) AND rFlux_PS<0.005 AND scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test) < 0.001;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.005"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_Test")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.001"), query::ValueExpr::NONE)))))), AreaRestrictorBox("6", "6", "7", "7")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object AS `o1`,Object AS `o2` WHERE qserv_areaspec_box(6,6,7,7) rFlux_PS<0.005 AND scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test)<0.001"
    ),
    Antlr4TestQueries(
        "select * from LSST.Object as o1, LSST.Object as o2, LSST.Source where o1.id <> o2.id and 0.024 > scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test) and Source.objectIdSourceTest=o2.objectIdObjTest;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "o1"), TableRef("LSST", "Object", "o2"), TableRef("LSST", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "id")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "id")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor("0.024"), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_Test")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "objectIdSourceTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectIdObjTest")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM LSST.Object AS `o1`,LSST.Object AS `o2`,LSST.Source WHERE o1.id<>o2.id AND 0.024>scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test) AND Source.objectIdSourceTest=o2.objectIdObjTest"
    ),
    Antlr4TestQueries(
        "select count(*) from Bad.Object as o1, Object o2 where qserv_areaspec_box(6,6,7,7) AND o1.ra_PS between 6 and 7 and o1.decl_PS between 6 and 7 ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("Bad", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("6"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("7"), query::ValueExpr::NONE)))), BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("6"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("7"), query::ValueExpr::NONE)))))), AreaRestrictorBox("6", "6", "7", "7")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Bad.Object AS `o1`,Object AS `o2` WHERE qserv_areaspec_box(6,6,7,7) o1.ra_PS BETWEEN 6 AND 7 AND o1.decl_PS BETWEEN 6 AND 7"
    ),
    Antlr4TestQueries(
        "select * from LSST.Object o, Source s WHERE qserv_areaspec_box(2,2,3,3) AND o.objectIdObjTest = s.objectIdSourceTest;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "o"), TableRef("", "Source", "s")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "objectIdSourceTest")), query::ValueExpr::NONE)))))), AreaRestrictorBox("2", "2", "3", "3")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM LSST.Object AS `o`,Source AS `s` WHERE qserv_areaspec_box(2,2,3,3) o.objectIdObjTest=s.objectIdSourceTest"
    ),
    Antlr4TestQueries(
        "select count(*) from Object as o1, Object as o2;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object AS `o1`,Object AS `o2`"
    ),
    Antlr4TestQueries(
        "select count(*) from LSST.Object as o1, LSST.Object as o2 WHERE o1.objectIdObjTest = o2.objectIdObjTest and o1.iFlux > 0.4 and o2.gFlux > 0.4;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "o1"), TableRef("LSST", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectIdObjTest")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "iFlux")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.4"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "gFlux")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.4"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM LSST.Object AS `o1`,LSST.Object AS `o2` WHERE o1.objectIdObjTest=o2.objectIdObjTest AND o1.iFlux>0.4 AND o2.gFlux>0.4"
    ),
    Antlr4TestQueries(
        "select o1.objectId, o2.objectI2, scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS distance from LSST.Object as o1, LSST.Object as o2 where o1.foo <> o2.foo and o1.objectIdObjTest = o2.objectIdObjTest;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectI2")), query::ValueExpr::NONE)),
                ValueExpr("distance", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "o1"), TableRef("LSST", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "foo")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "foo")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectIdObjTest")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId,o2.objectI2,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS `distance` FROM LSST.Object AS `o1`,LSST.Object AS `o2` WHERE o1.foo<>o2.foo AND o1.objectIdObjTest=o2.objectIdObjTest"
    ),
    Antlr4TestQueries(
        "select o1.objectId, o2.objectI2, scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS distance from LSST.Object as o1, LSST.Object as o2 where o1.foo != o2.foo and o1.objectIdObjTest = o2.objectIdObjTest;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectI2")), query::ValueExpr::NONE)),
                ValueExpr("distance", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "o1"), TableRef("LSST", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "foo")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP_ALT, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "foo")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectIdObjTest")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId,o2.objectI2,scisql_angSep(o1.ra_PS,o1.decl_PS,o2.ra_PS,o2.decl_PS) AS `distance` FROM LSST.Object AS `o1`,LSST.Object AS `o2` WHERE o1.foo!=o2.foo AND o1.objectIdObjTest=o2.objectIdObjTest"
    ),
    Antlr4TestQueries(
        "select count(*) from LSST.Object as o1, LSST.Object as o2;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "o1"), TableRef("LSST", "Object", "o2")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM LSST.Object AS `o1`,LSST.Object AS `o2`"
    ),
    Antlr4TestQueries(
        "select count(*) from LSST.Object o1,LSST.Object o2 WHERE qserv_areaspec_box(5.5, 5.5, 6.1, 6.1) AND scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test) < 0.02",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "o1"), TableRef("LSST", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_Test")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.02"), query::ValueExpr::NONE)))))), AreaRestrictorBox("5.5", "5.5", "6.1", "6.1")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM LSST.Object AS `o1`,LSST.Object AS `o2` WHERE qserv_areaspec_box(5.5,5.5,6.1,6.1) scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test)<0.02"
    ),
    Antlr4TestQueries(
        "select o1.ra_PS, o1.ra_PS_Sigma, o2.ra_PS ra_PS2, o2.ra_PS_Sigma ra_PS_Sigma2 from Object o1, Object o2 where o1.ra_PS_Sigma < 4e-7 and o2.ra_PS_Sigma < 4e-7;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("ra_PS2", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("ra_PS_Sigma2", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS_Sigma")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS_Sigma")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("4e-7"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_PS_Sigma")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("4e-7"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.ra_PS,o1.ra_PS_Sigma,o2.ra_PS AS `ra_PS2`,o2.ra_PS_Sigma AS `ra_PS_Sigma2` FROM Object AS `o1`,Object AS `o2` WHERE o1.ra_PS_Sigma<4e-7 AND o2.ra_PS_Sigma<4e-7"
    ),
    Antlr4TestQueries(
        "select o1.ra_PS, o1.ra_PS_Sigma, s.dummy, Exposure.exposureTime from LSST.Object o1,  Source s, Exposure WHERE o1.objectIdObjTest = s.objectIdSourceTest AND Exposure.id = o1.exposureId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_PS_Sigma")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "dummy")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Exposure", "exposureTime")), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "o1"), TableRef("", "Source", "s"), TableRef("", "Exposure", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "objectIdSourceTest")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Exposure", "id")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "exposureId")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.ra_PS,o1.ra_PS_Sigma,s.dummy,Exposure.exposureTime FROM LSST.Object AS `o1`,Source AS `s`,Exposure WHERE o1.objectIdObjTest=s.objectIdSourceTest AND Exposure.id=o1.exposureId"
    ),
    Antlr4TestQueries(
        "select count(*) from Object where qserv_areaspec_box(359.1, 3.16, 359.2,3.17);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("359.1", "3.16", "359.2", "3.17")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object WHERE qserv_areaspec_box(359.1,3.16,359.2,3.17)"
    ),
    Antlr4TestQueries(
        "select count(*) from LSST.Object where qserv_areaspec_box(359.1, 3.16, 359.2,3.17);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("359.1", "3.16", "359.2", "3.17")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM LSST.Object WHERE qserv_areaspec_box(359.1,3.16,359.2,3.17)"
    ),
    Antlr4TestQueries(
        " SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), x_chunkId FROM Object GROUP BY x_chunkId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("n", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("AVG", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("AVG", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "x_chunkId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "x_chunkId")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT count(*) AS `n`,AVG(ra_PS),AVG(decl_PS),x_chunkId FROM Object GROUP BY x_chunkId"
    ),
    Antlr4TestQueries(
        "select count(*) from Object where qserv_areaspec_box(359.1, 3.16, 359.2, 3.17);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("359.1", "3.16", "359.2", "3.17")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object WHERE qserv_areaspec_box(359.1,3.16,359.2,3.17)"
    ),
    Antlr4TestQueries(
        "SELECT offset, mjdRef, drift FROM LeapSeconds where offset = 10",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "offset")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "mjdRef")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "drift")), query::ValueExpr::NONE))),
            FromList(TableRef("", "LeapSeconds", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "offset")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("10"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT offset,mjdRef,drift FROM LeapSeconds WHERE offset=10"
    ),
    Antlr4TestQueries(
        "SELECT count(*) from Object;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object"
    ),
    Antlr4TestQueries(
        "SELECT count(*) from LSST.Source;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Source", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM LSST.Source"
    ),
    Antlr4TestQueries(
        "SELECT count(*) FROM Object WHERE iFlux < 0.4;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.4"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object WHERE iFlux<0.4"
    ),
    Antlr4TestQueries(
        "SELECT rFlux FROM Object WHERE iFlux < 0.4 ;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.4"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT rFlux FROM Object WHERE iFlux<0.4"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Object WHERE iRadius_SG between 0.02 AND 0.021 LIMIT 3;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iRadius_SG")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("0.02"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0.021"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, 3);},
        "SELECT * FROM Object WHERE iRadius_SG BETWEEN 0.02 AND 0.021 LIMIT 3"
    ),
    Antlr4TestQueries(
        "SELECT * from Science_Ccd_Exposure limit 3;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "")), nullptr, nullptr, nullptr, nullptr, 0, 3);},
        "SELECT * FROM Science_Ccd_Exposure LIMIT 3"
    ),
    Antlr4TestQueries(
        "SELECT table1.* from Science_Ccd_Exposure limit 3;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, "table1"), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "")), nullptr, nullptr, nullptr, nullptr, 0, 3);},
        "SELECT table1.* FROM Science_Ccd_Exposure LIMIT 3"
    ),
    Antlr4TestQueries(
        "SELECT * from Science_Ccd_Exposure limit 1;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "")), nullptr, nullptr, nullptr, nullptr, 0, 1);},
        "SELECT * FROM Science_Ccd_Exposure LIMIT 1"
    ),
    Antlr4TestQueries(
        "select ra_PS ra1,decl_PS as dec1 from Object order by dec1;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("ra1", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("dec1", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr,
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "dec1")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT ra_PS AS `ra1`,decl_PS AS `dec1` FROM Object ORDER BY dec1"
    ),
    Antlr4TestQueries(
        "select o1.iflux_PS o1ps, o2.iFlux_PS o2ps, computeX(o1.one, o2.one) from Object o1, Object o2 order by o1.objectId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("o1ps", FactorOp(ValueFactor(ColumnRef("", "o1", "iflux_PS")), query::ValueExpr::NONE)),
                ValueExpr("o2ps", FactorOp(ValueFactor(ColumnRef("", "o2", "iFlux_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("computeX", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "one")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "one")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")), nullptr,
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT o1.iflux_PS AS `o1ps`,o2.iFlux_PS AS `o2ps`,computeX(o1.one,o2.one) FROM Object AS `o1`,Object AS `o2` ORDER BY o1.objectId"
    ),
    Antlr4TestQueries(
        "select ra_PS from LSST.Object where ra_PS between 3 and 4;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("3"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("4"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM LSST.Object WHERE ra_PS BETWEEN 3 AND 4"
    ),
    Antlr4TestQueries(
        "select count(*) from LSST.Object_3840, usnob.Object_3840 where LSST.Object_3840.objectId > usnob.Object_3840.objectId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object_3840", ""), TableRef("usnob", "Object_3840", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("LSST", "Object_3840", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("usnob", "Object_3840", "objectId")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM LSST.Object_3840,usnob.Object_3840 WHERE LSST.Object_3840.objectId>usnob.Object_3840.objectId"
    ),
    Antlr4TestQueries(
        "select count(*), max(iFlux_PS) from LSST.Object where iFlux_PS > 100 and col1=col2;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("max", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("100"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "col1")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "col2")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*),max(iFlux_PS) FROM LSST.Object WHERE iFlux_PS>100 AND col1=col2"
    ),
    Antlr4TestQueries(
        "select count(*), max(iFlux_PS) from LSST.Object where qserv_areaspec_box(0,0,1,1) and iFlux_PS > 100 and col1=col2 and col3=4;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("max", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("100"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "col1")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "col2")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "col3")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("4"), query::ValueExpr::NONE)))))), AreaRestrictorBox("0", "0", "1", "1")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*),max(iFlux_PS) FROM LSST.Object WHERE qserv_areaspec_box(0,0,1,1) iFlux_PS>100 AND col1=col2 AND col3=4"
    ),
    Antlr4TestQueries(
        "SELECT * from Object order by ra_PS limit 3;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr,
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, 3);},
        "SELECT * FROM Object ORDER BY ra_PS LIMIT 3"
    ),
    Antlr4TestQueries(
        "SELECT run FROM LSST.Science_Ccd_Exposure order by field limit 2;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "run")), query::ValueExpr::NONE))),
            FromList(TableRef("LSST", "Science_Ccd_Exposure", "")), nullptr,
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "field")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, 2);},
        "SELECT run FROM LSST.Science_Ccd_Exposure ORDER BY field LIMIT 2"
    ),
    Antlr4TestQueries(
        "SELECT count(*) from Science_Ccd_Exposure group by visit;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "")), nullptr, nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "visit")), query::ValueExpr::NONE)), "")), nullptr, 0, -1);},
        "SELECT count(*) FROM Science_Ccd_Exposure GROUP BY visit"
    ),
    Antlr4TestQueries(
        "select count(*) from Object group by flags having count(*) > 3;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "flags")), query::ValueExpr::NONE)), "")),
            HavingClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("3"), query::ValueExpr::NONE))))))), 0, -1);},
        "SELECT count(*) FROM Object GROUP BY flags HAVING count(*)>3"
    ),
    Antlr4TestQueries(
        "SELECT count(*), sum(Source.flux), flux2, Source.flux3 from Source where qserv_areaspec_box(0,0,1,1) and flux4=2 and Source.flux5=3;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("sum", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "flux")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "flux2")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "flux3")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "flux4")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("2"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "flux5")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("3"), query::ValueExpr::NONE)))))), AreaRestrictorBox("0", "0", "1", "1")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*),sum(Source.flux),flux2,Source.flux3 FROM Source WHERE qserv_areaspec_box(0,0,1,1) flux4=2 AND Source.flux5=3"
    ),
    Antlr4TestQueries(
        "SELECT count(*) FROM Object WHERE  qserv_areaspec_box(1,3,2,4) AND  scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("21"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("21.5"), query::ValueExpr::NONE)))))), AreaRestrictorBox("1", "3", "2", "4")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object WHERE qserv_areaspec_box(1,3,2,4) scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 AND 21.5"
    ),
    Antlr4TestQueries(
        "SELECT f(one)/f2(two) FROM  Object where qserv_areaspec_box(0,0,1,1);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("f", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "one")), query::ValueExpr::NONE)))), query::ValueExpr::DIVIDE), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("f2", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "two")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "1", "1")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT (f(one)/f2(two)) FROM Object WHERE qserv_areaspec_box(0,0,1,1)"
    ),
    Antlr4TestQueries(
        "SELECT (1+f(one))/f2(two) FROM  Object where qserv_areaspec_box(0,0,1,1);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::PLUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("f", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "one")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))), query::ValueExpr::DIVIDE), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("f2", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "two")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(nullptr, AreaRestrictorBox("0", "0", "1", "1")), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ((1+f(one))/f2(two)) FROM Object WHERE qserv_areaspec_box(0,0,1,1)"
    ),
    Antlr4TestQueries(
        "SELECT objectId as id, COUNT(sourceId) AS c FROM Source GROUP BY objectId HAVING  c > 1000 LIMIT 10;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("id", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("c", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("COUNT", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "sourceId")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")), nullptr, nullptr,
            GroupByClause(GroupByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), "")),
            HavingClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "c")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1000"), query::ValueExpr::NONE))))))), 0, 10);},
        "SELECT objectId AS `id`,COUNT(sourceId) AS `c` FROM Source GROUP BY objectId HAVING c>1000 LIMIT 10"
    ),
    Antlr4TestQueries(
        "SELECT ROUND(scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS), 0) AS UG, ROUND(scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS), 0) AS GR FROM Object WHERE scisql_fluxToAbMag(gFlux_PS) < 0.2 AND scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS) >=-0.27 AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) >=-0.24 AND scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) >=-0.27 AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) >=-0.35 AND scisql_fluxToAbMag(zFlux_PS)-scisql_fluxToAbMag(yFlux_PS) >=-0.40;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("UG", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("ROUND", ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "uFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("GR", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("ROUND", ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("0"), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.2"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "uFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OR_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("-0.27"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OR_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("-0.24"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OR_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("-0.27"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OR_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("-0.35"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "yFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OR_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("-0.40"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ROUND(scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS),0) AS `UG`,ROUND(scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS),0) AS `GR` FROM Object WHERE scisql_fluxToAbMag(gFlux_PS)<0.2 AND (scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS))>=-0.27 AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))>=-0.24 AND (scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS))>=-0.27 AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS))>=-0.35 AND (scisql_fluxToAbMag(zFlux_PS)-scisql_fluxToAbMag(yFlux_PS))>=-0.40"
    ),
    Antlr4TestQueries(
        "SELECT DISTINCT foo FROM Filter f;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "f")), nullptr, nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT foo FROM Filter AS `f`"
    ),
    Antlr4TestQueries(
        "SELECT DISTINCT zNumObs FROM Object;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zNumObs")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT zNumObs FROM Object"
    ),
    Antlr4TestQueries(
        "SELECT foo FROM Filter f limit 5",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "f")), nullptr, nullptr, nullptr, nullptr, 0, 5);},
        "SELECT foo FROM Filter AS `f` LIMIT 5"
    ),
    Antlr4TestQueries(
        "SELECT foo FROM Filter f limit 5;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "f")), nullptr, nullptr, nullptr, nullptr, 0, 5);},
        "SELECT foo FROM Filter AS `f` LIMIT 5"
    ),
    Antlr4TestQueries(
        "SELECT foo FROM Filter f limit 5;; ",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "f")), nullptr, nullptr, nullptr, nullptr, 0, 5);},
        "SELECT foo FROM Filter AS `f` LIMIT 5"
    ),
    Antlr4TestQueries(
        "SELECT  o1.objectId FROM Object o1 WHERE ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) ) < 1;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("ABS", ValueExpr("", FactorOp(ValueFactor(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))), query::ValueExpr::MINUS), FactorOp(ValueFactor(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId FROM Object AS `o1` WHERE ABS((scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS))-(scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)))<1"
    ),
    Antlr4TestQueries(
        "SELECT  o1.objectId, o2.objectId objectId2 FROM Object o1, Object o2 WHERE scisql_angSep(o1.ra_Test, o1.decl_Test, o2.ra_Test, o2.decl_Test) < 0.00001 AND o1.objectId <> o2.objectId AND ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - (scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)) ) < 1;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("objectId2", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o1"), TableRef("", "Object", "o2")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_angSep", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "ra_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "decl_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "ra_Test")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "decl_Test")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.00001"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "objectId")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("ABS", ValueExpr("", FactorOp(ValueFactor(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o1", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))), query::ValueExpr::MINUS), FactorOp(ValueFactor(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT o1.objectId,o2.objectId AS `objectId2` FROM Object AS `o1`,Object AS `o2` WHERE scisql_angSep(o1.ra_Test,o1.decl_Test,o2.ra_Test,o2.decl_Test)<0.00001 AND o1.objectId<>o2.objectId AND ABS((scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS))-(scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)))<1"
    ),
    Antlr4TestQueries(
        "SELECT * FROM RefObjMatch;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "RefObjMatch", "")), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM RefObjMatch"
    ),
    Antlr4TestQueries(
        "SELECT * FROM RefObjMatch WHERE foo<>bar AND baz<3.14159;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "RefObjMatch", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "foo")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "bar")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "baz")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("3.14159"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM RefObjMatch WHERE foo<>bar AND baz<3.14159"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.foo FROM Source s, Object o WHERE s.objectIdSourceTest=o.objectIdObjTest and o.objectIdObjTest = 430209694171136;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s"), TableRef("", "Object", "o")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "objectIdSourceTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectIdObjTest")), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430209694171136"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.foo FROM Source AS `s`,Object AS `o` WHERE s.objectIdSourceTest=o.objectIdObjTest AND o.objectIdObjTest=430209694171136"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.foo FROM Object o JOIN Source2 s USING (objectIdObjTest) JOIN Source2 s2 USING (objectIdObjTest) WHERE o.objectId = 430209694171136;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source2", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectIdObjTest"), nullptr)), JoinRef(TableRef("", "Source2", "s2"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectIdObjTest"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430209694171136"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.foo FROM Object AS `o` JOIN Source2 AS `s` USING(objectIdObjTest) JOIN Source2 AS `s2` USING(objectIdObjTest) WHERE o.objectId=430209694171136"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.foo FROM Object o JOIN Source s ON s.objectIdSourceTest = Object.objectIdObjTest JOIN Source s2 ON s.objectIdSourceTest = s2.objectIdSourceTest WHERE LSST.Object.objectId = 430209694171136;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(nullptr, BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "objectIdSourceTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Object", "objectIdObjTest")), query::ValueExpr::NONE)))))), JoinRef(TableRef("", "Source", "s2"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(nullptr, BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "objectIdSourceTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "objectIdSourceTest")), query::ValueExpr::NONE)))))))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("LSST", "Object", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430209694171136"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.foo FROM Object AS `o` JOIN Source AS `s` ON s.objectIdSourceTest=Object.objectIdObjTest JOIN Source AS `s2` ON s.objectIdSourceTest=s2.objectIdSourceTest WHERE LSST.Object.objectId=430209694171136"
    ),
    Antlr4TestQueries(
        "SELECT s1.foo, s2.foo AS s2_foo FROM Source s1 NATURAL LEFT JOIN Source s2 WHERE s1.bar = s2.bar;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "foo")), query::ValueExpr::NONE)),
                ValueExpr("s2_foo", FactorOp(ValueFactor(ColumnRef("", "s2", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s1", JoinRef(TableRef("", "Source", "s2"), query::JoinRef::LEFT, NATURAL, nullptr))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s1.foo,s2.foo AS `s2_foo` FROM Source AS `s1` NATURAL LEFT OUTER JOIN Source AS `s2` WHERE s1.bar=s2.bar"
    ),
    Antlr4TestQueries(
        "SELECT s1.foo, s2.foo AS s2_foo FROM Source s1 NATURAL LEFT JOIN Source s2 WHERE s1.bar = s2.bar;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "foo")), query::ValueExpr::NONE)),
                ValueExpr("s2_foo", FactorOp(ValueFactor(ColumnRef("", "s2", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s1", JoinRef(TableRef("", "Source", "s2"), query::JoinRef::LEFT, NATURAL, nullptr))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s1.foo,s2.foo AS `s2_foo` FROM Source AS `s1` NATURAL LEFT OUTER JOIN Source AS `s2` WHERE s1.bar=s2.bar"
    ),
    Antlr4TestQueries(
        "SELECT s1.foo, s2.foo AS s2_foo FROM Source s1 NATURAL RIGHT JOIN Source s2 WHERE s1.bar = s2.bar;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "foo")), query::ValueExpr::NONE)),
                ValueExpr("s2_foo", FactorOp(ValueFactor(ColumnRef("", "s2", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s1", JoinRef(TableRef("", "Source", "s2"), query::JoinRef::RIGHT, NATURAL, nullptr))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s1.foo,s2.foo AS `s2_foo` FROM Source AS `s1` NATURAL RIGHT OUTER JOIN Source AS `s2` WHERE s1.bar=s2.bar"
    ),
    Antlr4TestQueries(
        "SELECT s1.foo, s2.foo AS s2_foo FROM Source s1 NATURAL JOIN Source s2 WHERE s1.bar = s2.bar;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "foo")), query::ValueExpr::NONE)),
                ValueExpr("s2_foo", FactorOp(ValueFactor(ColumnRef("", "s2", "foo")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s1", JoinRef(TableRef("", "Source", "s2"), query::JoinRef::DEFAULT, NATURAL, nullptr))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s1.foo,s2.foo AS `s2_foo` FROM Source AS `s1` NATURAL JOIN Source AS `s2` WHERE s1.bar=s2.bar"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Filter f JOIN Science_Ccd_Exposure USING(exposureId);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "f", JoinRef(TableRef("", "Science_Ccd_Exposure", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "exposureId"), nullptr)))), nullptr, nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Filter AS `f` JOIN Science_Ccd_Exposure USING(exposureId)"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Object WHERE objectIdObjTest = 430213989000;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("430213989000"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Object WHERE objectIdObjTest=430213989000"
    ),
    Antlr4TestQueries(
        "SELECT s.ra, s.decl, o.raRange, o.declRange FROM   Object o JOIN   Source2 s USING (objectIdObjTest) WHERE  o.objectIdObjTest = 390034570102582 AND    o.latestObsTime = s.taiMidPoint;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "ra")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "decl")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "raRange")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "declRange")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "Source2", "s"), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectIdObjTest"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("390034570102582"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "latestObsTime")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s", "taiMidPoint")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT s.ra,s.decl,o.raRange,o.declRange FROM Object AS `o` JOIN Source2 AS `s` USING(objectIdObjTest) WHERE o.objectIdObjTest=390034570102582 AND o.latestObsTime=s.taiMidPoint"
    ),
    Antlr4TestQueries(
        "SELECT sce.filterId, sce.filterName FROM Science_Ccd_Exposure AS sce WHERE (sce.visit = 887404831) AND (sce.raftName = '3,3') AND (sce.ccdName LIKE '%')",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "filterName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Science_Ccd_Exposure", "sce")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "visit")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("887404831"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "raftName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'3,3'"), query::ValueExpr::NONE))))))), PassTerm(")")),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "sce", "ccdName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'%'"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT sce.filterId,sce.filterName FROM Science_Ccd_Exposure AS `sce` WHERE (sce.visit=887404831) AND (sce.raftName='3,3') AND (sce.ccdName LIKE '%')"
    ),
    Antlr4TestQueries(
        "SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux) FROM   Source JOIN   Object USING(objectId) JOIN   Filter USING(filterId) WHERE qserv_areaspec_box(355, 0, 360, 20) AND filterName = 'g' ORDER BY objectId, taiMidPoint ASC;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "taiMidPoint")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "psfFlux")), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "", JoinRef(TableRef("", "Object", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "objectId"), nullptr)), JoinRef(TableRef("", "Filter", ""), query::JoinRef::DEFAULT, NOT_NATURAL, JoinSpec(ColumnRef("", "", "filterId"), nullptr)))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("'g'"), query::ValueExpr::NONE)))))), AreaRestrictorBox("355", "0", "360", "20")),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, ""), OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "taiMidPoint")), query::ValueExpr::NONE)), query::OrderByTerm::ASC, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId,taiMidPoint,scisql_fluxToAbMag(psfFlux) FROM Source JOIN Object USING(objectId) JOIN Filter USING(filterId) WHERE qserv_areaspec_box(355,0,360,20) filterName='g' ORDER BY objectId, taiMidPoint ASC"
    ),
    Antlr4TestQueries(
        "SELECT DISTINCT rFlux_PS FROM Object;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")), nullptr, nullptr, nullptr, nullptr, 1, -1);},
        "SELECT DISTINCT rFlux_PS FROM Object"
    ),
    Antlr4TestQueries(
        "SELECT count(*) FROM   Object o WHERE closestToObj is NULL;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, NullPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "closestToObj")), query::ValueExpr::NONE)), IS_NULL))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object AS `o` WHERE closestToObj IS NULL"
    ),
    Antlr4TestQueries(
        "SELECT count(*) FROM   Object o INNER JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId) INNER JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId) WHERE  closestToObj = 1 OR closestToObj is NULL;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, FuncExpr("count", ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE)))), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "o", JoinRef(TableRef("", "RefObjMatch", "o2t"), query::JoinRef::INNER, NOT_NATURAL, JoinSpec(nullptr, BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o", "objectIdObjTest")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2t", "objectId")), query::ValueExpr::NONE)))))), JoinRef(TableRef("", "SimRefObject", "t"), query::JoinRef::INNER, NOT_NATURAL, JoinSpec(nullptr, BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "o2t", "refObjectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "t", "refObjectId")), query::ValueExpr::NONE)))))))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "closestToObj")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))), AndTerm(BoolFactor(IS, NullPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "closestToObj")), query::ValueExpr::NONE)), IS_NULL))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT count(*) FROM Object AS `o` INNER JOIN RefObjMatch AS `o2t` ON o.objectIdObjTest=o2t.objectId INNER JOIN SimRefObject AS `t` ON o2t.refObjectId=t.refObjectId WHERE closestToObj=1 OR closestToObj IS NULL"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Source s1 CROSS JOIN Source s2 WHERE s1.bar = s2.bar;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "s1", JoinRef(TableRef("", "Source", "s2"), query::JoinRef::CROSS, NOT_NATURAL, nullptr))),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s1", "bar")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "s2", "bar")), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Source AS `s1` CROSS JOIN Source AS `s2` WHERE s1.bar=s2.bar"
    ),
    Antlr4TestQueries(
        "SELECT objectId, scisql_fluxToAbMag(uFlux_PS), scisql_fluxToAbMag(gFlux_PS), scisql_fluxToAbMag(rFlux_PS), scisql_fluxToAbMag(iFlux_PS), scisql_fluxToAbMag(zFlux_PS), scisql_fluxToAbMag(yFlux_PS), ra_PS, decl_PS FROM   Object WHERE  ( scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.7 OR scisql_fluxToAbMag(gFlux_PS) > 22.3 ) AND    scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.1 AND    ( scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) < (0.08 + 0.42 * (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) - 0.96))  OR scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 1.26 ) AND    scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) < 0.8;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "uFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "yFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.7"), query::ValueExpr::NONE))))), AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("22.3"), query::ValueExpr::NONE))))))), PassTerm(")")), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.1"), query::ValueExpr::NONE)))),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.08"), query::ValueExpr::PLUS), FactorOp(ValueFactor("0.42"), query::ValueExpr::MULTIPLY), FactorOp(ValueFactor(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor("0.96"), query::ValueExpr::NONE))), query::ValueExpr::NONE))))), AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "gFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "rFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1.26"), query::ValueExpr::NONE))))))), PassTerm(")")), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::MINUS), FactorOp(ValueFactor(query::ValueFactor::FUNCTION, FuncExpr("scisql_fluxToAbMag", ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlux_PS")), query::ValueExpr::NONE)))), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("0.8"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,scisql_fluxToAbMag(uFlux_PS),scisql_fluxToAbMag(gFlux_PS),scisql_fluxToAbMag(rFlux_PS),scisql_fluxToAbMag(iFlux_PS),scisql_fluxToAbMag(zFlux_PS),scisql_fluxToAbMag(yFlux_PS),ra_PS,decl_PS FROM Object WHERE ((scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))>0.7 OR scisql_fluxToAbMag(gFlux_PS)>22.3) AND (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))>0.1 AND ((scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS))<(0.08+0.42 *(scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS)-0.96)) OR (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))>1.26) AND (scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS))<0.8"
    ),
    Antlr4TestQueries(
        "select objectId, ra_PS from Object where ra_PS > 359.5 and (objectId = 417853073271391 or  objectId = 399294519599888)",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("359.5"), query::ValueExpr::NONE)))),
                BoolFactor(IS, PassTerm("("), BoolTermFactor(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("417853073271391"), query::ValueExpr::NONE))))), AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("399294519599888"), query::ValueExpr::NONE))))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS FROM Object WHERE ra_PS>359.5 AND (objectId=417853073271391 OR objectId=399294519599888)"
    ),
    Antlr4TestQueries(
        "select shortName from Filter where shortName LIKE 'Z'",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "shortName")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "shortName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'Z'"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT shortName FROM Filter WHERE shortName LIKE 'Z'"
    ),
    Antlr4TestQueries(
        "SELECT Source.sourceId, Source.objectId From Source WHERE Source.objectId IN (386942193651348) ORDER BY Source.sourceId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "objectId")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT Source.sourceId,Source.objectId FROM Source WHERE Source.objectId IN(386942193651348) ORDER BY Source.sourceId"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS FROM Object WHERE objectId = 417857368235490;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM Object WHERE objectId=417857368235490"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS FROM Object WHERE objectId <> 417857368235490;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM Object WHERE objectId<>417857368235490"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS FROM Object WHERE objectId != 417857368235490;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NOT_EQUALS_OP_ALT, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM Object WHERE objectId!=417857368235490"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS FROM Object WHERE objectId < 417857368235490;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM Object WHERE objectId<417857368235490"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS FROM Object WHERE objectId <= 417857368235490;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OR_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM Object WHERE objectId<=417857368235490"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS FROM Object WHERE objectId >= 417857368235490;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OR_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM Object WHERE objectId>=417857368235490"
    ),
    Antlr4TestQueries(
        "SELECT ra_PS FROM Object WHERE objectId > 417857368235490;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM Object WHERE objectId>417857368235490"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS FROM Object WHERE objectId IN (417857368235490, 420949744686724, 420954039650823);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), IN, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("420949744686724"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("420954039650823"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS FROM Object WHERE objectId IN(417857368235490,420949744686724,420954039650823)"
    ),
    Antlr4TestQueries(
        "SELECT objectId, ra_PS FROM Object WHERE objectId BETWEEN 417857368235490 AND 420949744686724;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), BETWEEN, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor("420949744686724"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS FROM Object WHERE objectId BETWEEN 417857368235490 AND 420949744686724"
    ),
    Antlr4TestQueries(
        "SELECT * FROM Filter WHERE filterName LIKE 'dd';",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE)), LIKE, ValueExpr("", FactorOp(ValueFactor("'dd'"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Filter WHERE filterName LIKE 'dd'"
    ),
    Antlr4TestQueries(
        "select objectId from Object where zFlags is NULL;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, NullPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlags")), query::ValueExpr::NONE)), IS_NULL))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE zFlags IS NULL"
    ),
    Antlr4TestQueries(
        "select objectId from Object where zFlags is NOT NULL;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, NullPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "zFlags")), query::ValueExpr::NONE)), IS_NOT_NULL))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE zFlags IS NOT NULL"
    ),
    Antlr4TestQueries(
        "select objectId, iRadius_SG, ra_PS, decl_PS from Object where iRadius_SG > .5 AND ra_PS < 2 AND decl_PS < 3;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iRadius_SG")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iRadius_SG")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor(".5"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("2"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("3"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,iRadius_SG,ra_PS,decl_PS FROM Object WHERE iRadius_SG>.5 AND ra_PS<2 AND decl_PS<3"
    ),
    Antlr4TestQueries(
        "select objectId from Object where objectId < 400000000000000 OR objectId > 430000000000000 ORDER BY objectId",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("400000000000000"), query::ValueExpr::NONE))))), AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("430000000000000"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE objectId<400000000000000 OR objectId>430000000000000 ORDER BY objectId"
    ),
    Antlr4TestQueries(
        "SELECT objectId from Object where ra_PS/2 > 1",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Object", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::DIVIDE), FactorOp(ValueFactor("2"), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (ra_PS/2)>1"
    ),
    // tests NOT LIKE (which is 'NOT LIKE', different than 'NOT' and 'LIKE' operators separately)
    Antlr4TestQueries(
        "SELECT filterId FROM Filter WHERE filterName NOT LIKE 'Z'",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Filter", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, LikePredicate(
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterName")), query::ValueExpr::NONE)),
                NOT_LIKE,
                ValueExpr("", FactorOp(ValueFactor("'Z'"), query::ValueExpr::NONE))))))),
            nullptr, nullptr, nullptr, 0, -1);},
        "SELECT filterId FROM Filter WHERE filterName NOT LIKE 'Z'"
    ),
    // tests quoted IDs
    Antlr4TestQueries(
        "SELECT `Source`.`sourceId`, `Source`.`objectId` From Source WHERE `Source`.`objectId` IN (386942193651348) ORDER BY `Source`.`sourceId`",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(
            SelectList(
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "sourceId")), query::ValueExpr::NONE)),
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "objectId")), query::ValueExpr::NONE))),
            FromList(TableRef("", "Source", "")),
            WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "objectId")), query::ValueExpr::NONE)),
                IN,
                ValueExpr("", FactorOp(ValueFactor("386942193651348"), query::ValueExpr::NONE))))))),
            OrderByClause(OrderByTerm(
                ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "Source", "sourceId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")),
            nullptr, nullptr, 0, -1);},
        "SELECT Source.sourceId,Source.objectId FROM Source WHERE Source.objectId IN(386942193651348) ORDER BY Source.sourceId"
    ),

    // tests the null-safe equals operator
    Antlr4TestQueries(
        "SELECT ra_PS FROM Object WHERE objectId<=>417857368235490",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::NULL_SAFE_EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT ra_PS FROM Object WHERE objectId<=>417857368235490"
    ),

    // tests the NOT BETWEEN operator
    Antlr4TestQueries(
        "SELECT objectId,ra_PS FROM Object WHERE objectId NOT BETWEEN 417857368235490 AND 420949744686724",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, BetweenPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), NOT_BETWEEN, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor("420949744686724"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS FROM Object WHERE objectId NOT BETWEEN 417857368235490 AND 420949744686724"
    ),

    // tests the && operator.
    // The Qserv IR converts && to AND as a result of the IR structure and how it serializes it to string.
    Antlr4TestQueries(
        "select objectId, iRadius_SG, ra_PS, decl_PS from Object where iRadius_SG > .5 && ra_PS < 2 && decl_PS < 3;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iRadius_SG")), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "iRadius_SG")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor(".5"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("2"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("3"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,iRadius_SG,ra_PS,decl_PS FROM Object WHERE iRadius_SG>.5 AND ra_PS<2 AND decl_PS<3"
    ),

    // tests the || operator.
    // The Qserv IR converts || to OR as a result of the IR structure and how it serializes it to string.
    Antlr4TestQueries(
        "select objectId from Object where objectId < 400000000000000 || objectId > 430000000000000 ORDER BY objectId;",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("400000000000000"), query::ValueExpr::NONE))))), AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("430000000000000"), query::ValueExpr::NONE))))))), OrderByClause(OrderByTerm(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), query::OrderByTerm::DEFAULT, "")), nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE objectId<400000000000000 OR objectId>430000000000000 ORDER BY objectId"
    ),

    // tests NOT IN in the InPredicate
    Antlr4TestQueries(
        "SELECT objectId, ra_PS FROM Object WHERE objectId NOT IN (417857368235490, 420949744686724, 420954039650823);",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, InPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), NOT_IN, ValueExpr("", FactorOp(ValueFactor("417857368235490"), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor("420949744686724"), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor("420954039650823"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,ra_PS FROM Object WHERE objectId NOT IN(417857368235490,420949744686724,420954039650823)"
    ),

    // tests the modulo operator
    Antlr4TestQueries(
        "select objectId, ra_PS % 3, decl_PS from Object where ra_PS % 3 > 1.5",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::MODULO), FactorOp(ValueFactor("3"), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::MODULO), FactorOp(ValueFactor("3"), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1.5"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,(ra_PS % 3),decl_PS FROM Object WHERE (ra_PS % 3)>1.5"
    ),

    // tests the MOD operator
    Antlr4TestQueries(
        "select objectId, ra_PS MOD 3, decl_PS from Object where ra_PS MOD 3 > 1.5",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::MOD), FactorOp(ValueFactor("3"), query::ValueExpr::NONE)), ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "decl_PS")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::MOD), FactorOp(ValueFactor("3"), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1.5"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId,(ra_PS MOD 3),decl_PS FROM Object WHERE (ra_PS MOD 3)>1.5"
    ),

    // tests the DIV operator
    Antlr4TestQueries(
        "SELECT objectId from Object where ra_PS DIV 2 > 1",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "ra_PS")), query::ValueExpr::DIV), FactorOp(ValueFactor("2"), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (ra_PS DIV 2)>1"
    ),

    // tests the & operator
    Antlr4TestQueries(
        "SELECT objectId from Object where objectID & 1 = 1",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectID")), query::ValueExpr::BIT_AND), FactorOp(ValueFactor("1"), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (objectID&1)=1"
    ),

    // tests the | operator
    Antlr4TestQueries(
        "SELECT objectId from Object where objectID | 1 = 1",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectID")), query::ValueExpr::BIT_OR), FactorOp(ValueFactor("1"), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (objectID|1)=1"
    ),

    // tests the << operator
    Antlr4TestQueries(
        "SELECT objectId from Object where objectID << 10 = 1",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectID")), query::ValueExpr::BIT_SHIFT_LEFT), FactorOp(ValueFactor("10"), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (objectID<<10)=1"
    ),

    // tests the >> operator
    Antlr4TestQueries(
        "SELECT objectId from Object where objectID >> 10 = 1",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectID")), query::ValueExpr::BIT_SHIFT_RIGHT), FactorOp(ValueFactor("10"), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (objectID>>10)=1"
    ),

    // tests the ^ operator
    Antlr4TestQueries(
        "SELECT objectId from Object where objectID ^ 1 = 1",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectId")), query::ValueExpr::NONE))), FromList(TableRef("", "Object", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "objectID")), query::ValueExpr::BIT_XOR), FactorOp(ValueFactor("1"), query::ValueExpr::NONE)), query::CompPredicate::EQUALS_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT objectId FROM Object WHERE (objectID^1)=1"
    ),

    // tests NOT with a BoolFactor
    Antlr4TestQueries(
        "select * from Filter where NOT filterId > 1 AND filterId < 6",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))), FromList(TableRef("", "Filter", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS_NOT, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("6"), query::ValueExpr::NONE))))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Filter WHERE NOT filterId>1 AND filterId<6"
    ),

    // tests NOT with an AND term
    Antlr4TestQueries(
        "select * from Filter where NOT (filterId > 1 AND filterId < 6)",
        []() -> shared_ptr<query::SelectStmt> { return SelectStmt(SelectList(ValueExpr("", FactorOp(ValueFactor(STAR, ""), query::ValueExpr::NONE))), FromList(TableRef("", "Filter", "")), WhereClause(OrTerm(AndTerm(BoolFactor(IS_NOT, PassTerm("("), BoolTermFactor(AndTerm(BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)), query::CompPredicate::GREATER_THAN_OP, ValueExpr("", FactorOp(ValueFactor("1"), query::ValueExpr::NONE)))), BoolFactor(IS, CompPredicate(ValueExpr("", FactorOp(ValueFactor(ColumnRef("", "", "filterId")), query::ValueExpr::NONE)), query::CompPredicate::LESS_THAN_OP, ValueExpr("", FactorOp(ValueFactor("6"), query::ValueExpr::NONE)))))), PassTerm(")"))))), nullptr, nullptr, nullptr, 0, -1);},
        "SELECT * FROM Filter WHERE NOT(filterId>1 AND filterId<6)"
    ),

    // tests expression with alias in select list
    Antlr4TestQueries(
        "SELECT objectId - 1 AS o FROM Object",
        []() -> shared_ptr<query::SelectStmt> { return
            SelectStmt(
                SelectList(
                    ValueExpr("o",
                        FactorOp(ValueFactor(ColumnRef(TableRef("", "", ""), "objectId")), query::ValueExpr::MINUS),
                        FactorOp(ValueFactor("1"), query::ValueExpr::NONE))
                    ),
                FromList(TableRef("", "Object", "")), nullptr, nullptr, nullptr, nullptr, 0, -1)
        ; },
        "SELECT (objectId-1) AS `o` FROM Object"
    ),
};


BOOST_DATA_TEST_CASE(antlr4_test, ANTLR4_TEST_QUERIES, queryInfo) {
    query::SelectStmt::Ptr selectStatement;
    BOOST_REQUIRE_NO_THROW(selectStatement = parser::SelectParser::makeSelectStmt(queryInfo.query));
    BOOST_REQUIRE(selectStatement != nullptr);
    BOOST_TEST_MESSAGE("antlr4 selectStmt structure:" << *selectStatement);
    // verify the selectStatements are the same:
    auto compareStatement = queryInfo.compareStmt();
    BOOST_REQUIRE_MESSAGE(*selectStatement == *compareStatement, "parser-generated statement:" << *selectStatement <<
        "does not match compare statement:" << *compareStatement);
    // verify the selectStatement converted back to sql is the same as the original query:
    std::string serializedQuery;
    BOOST_REQUIRE_NO_THROW(serializedQuery = selectStatement->getQueryTemplate().sqlFragment());
    BOOST_REQUIRE_EQUAL(serializedQuery,
             (queryInfo.serializedQuery != "" ? queryInfo.serializedQuery : queryInfo.query));
}


BOOST_AUTO_TEST_SUITE_END()
