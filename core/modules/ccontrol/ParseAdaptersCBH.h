// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST.
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


#include <memory>
#include <string>
#include <vector>


#ifndef LSST_QSERV_CCONTROL_PARSEADAPTERSCBH
#define LSST_QSERV_CCONTROL_PARSEADAPTERSCBH


namespace antlr4 {
    class ParserRuleContext;
}

namespace lsst {
namespace qserv {
namespace ccontrol {
    class UserQuery;
}
namespace query {
    class BetweenPredicate;
    class BoolTerm;
    class CompPredicate;
    class FromList;
    class GroupByClause;
    class HavingClause;
    class InPredicate;
    class JoinRef;
    class LikePredicate;
    class LogicalTerm;
    class NullPredicate;
    class OrderByClause;
    class OrderByTerm;
    class SelectList;
    class SelectStmt;
    class TableRef;
    class ValueExpr;
    class ValueFactor;
    class WhereClause;
}}}


namespace lsst {
namespace qserv {
namespace ccontrol {


// Callback Handler (CBH) classes define the interface of a 'parent' ParseAdapter node that may be called
// by the child node. This allows the child to call the parent with Qserv Intermediate Representation (IR)
// object instance(s) that it or its children created. The result is that as the listener finishes walking
// parts of the parse tree, the IR migrates 'up' to the root Adapter node, and results in a complete IR
// hierarchy that represents the given SQL statement.


class BaseCBH {
public:
    virtual ~BaseCBH() {}
};


class DmlStatementCBH : public BaseCBH {
public:
    virtual void handleDmlStatement(std::shared_ptr<query::SelectStmt> const& selectStatement) = 0;
    virtual void handleDmlStatement(std::shared_ptr<ccontrol::UserQuery> const& userQuery) = 0;
};


class SimpleSelectCBH : public BaseCBH {
public:
    virtual void handleSelectStatement(std::shared_ptr<query::SelectStmt> const& selectStatement) = 0;
};


class QuerySpecificationCBH : public BaseCBH {
public:
    virtual void handleQuerySpecification(std::shared_ptr<query::SelectList> const& selectList,
                                          std::shared_ptr<query::FromList> const& fromList,
                                          std::shared_ptr<query::WhereClause> const& whereClause,
                                          std::shared_ptr<query::OrderByClause> const& orderByClause,
                                          int limit,
                                          std::shared_ptr<query::GroupByClause> const& groupByClause,
                                          std::shared_ptr<query::HavingClause> const& havingClause,
                                          bool distinct) = 0;
};


class SelectElementsCBH : public BaseCBH {
public:
    virtual void handleSelectList(std::shared_ptr<query::SelectList> const& selectList) = 0;
};


class FullColumnNameCBH : public BaseCBH {
public:
    virtual void handleFullColumnName(std::shared_ptr<query::ValueFactor> const& valueFactor) = 0;
};


class TableNameCBH : public BaseCBH {
public:
    virtual void handleTableName(std::vector<std::string> const& str) = 0;
};


class FromClauseCBH : public BaseCBH {
public:
    virtual void handleFromClause(std::shared_ptr<query::FromList> const& fromList,
                                  std::shared_ptr<query::WhereClause> const& whereClause,
                                  std::shared_ptr<query::GroupByClause> const& groupByClause,
                                  std::shared_ptr<query::HavingClause> const& havingClause) = 0;
};


class TableSourcesCBH : public BaseCBH {
public:
    virtual void handleTableSources(
        std::shared_ptr<std::vector<std::shared_ptr<query::TableRef>>> const& tableRefList) = 0;
};


class TableSourceBaseCBH : public BaseCBH {
public:
    virtual void handleTableSource(std::shared_ptr<query::TableRef> const& tableRef) = 0;
};


class AtomTableItemCBH : public BaseCBH {
public:
    virtual void handleAtomTableItem(std::shared_ptr<query::TableRef> const& tableRef) = 0;
};


class UidCBH : public BaseCBH {
public:
    virtual void handleUid(std::string const& uidString) = 0;
};


class FullIdCBH : public BaseCBH {
public:
    virtual void handleFullId(std::vector<std::string> const& uidlist) = 0;
};


class ConstantExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleConstantExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) = 0;
};


class ExpressionAtomPredicateCBH : public BaseCBH {
public:
    virtual void handleExpressionAtomPredicate(std::shared_ptr<query::ValueExpr> const& valueExpr,
            antlr4::ParserRuleContext* childCtx) = 0;

    virtual void handleExpressionAtomPredicate(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) = 0;
};


class QservFunctionSpecCBH : public BaseCBH {
public:
    virtual void handleQservFunctionSpec(std::string const& functionName,
            std::vector<std::shared_ptr<query::ValueFactor>> const& args) = 0;
};


class ComparisonOperatorCBH : public BaseCBH {
public:
    virtual void handleComparisonOperator(std::string const& text) = 0;
};


class CallStatementCBH : public BaseCBH {
public:
    virtual void handleCallStatement(std::shared_ptr<ccontrol::UserQuery> const& userQuery) = 0;
};


class OrderByClauseCBH : public BaseCBH {
public:
    virtual void handleOrderByClause(std::shared_ptr<query::OrderByClause> const& orderByClause) = 0;
};


class OrderByExpressionCBH : public BaseCBH {
public:
    virtual void handleOrderByExpression(query::OrderByTerm const& orderByTerm) = 0;
};


class InnerJoinCBH : public BaseCBH {
public:
    virtual void handleInnerJoin(std::shared_ptr<query::JoinRef> const& joinRef) = 0;
};


class NaturalJoinCBH : public BaseCBH {
public:
    virtual void handleNaturalJoin(std::shared_ptr<query::JoinRef> const& joinRef) = 0;
};


class SelectSpecCBH : public BaseCBH {
public:
    virtual void handleSelectSpec(bool distinct) = 0;
};


class SelectStarElementCBH : public BaseCBH {
public:
    virtual void handleSelectStarElement(std::shared_ptr<query::ValueExpr> const& valueExpr) = 0;
};


class SelectFunctionElementCBH: public BaseCBH {
public:
    virtual void handleSelectFunctionElement(std::shared_ptr<query::ValueExpr> const& selectFunction) = 0;
};


class SelectExpressionElementCBH: public BaseCBH {
public:
    virtual void handleSelectExpressionElement(std::shared_ptr<query::ValueExpr> const& valueExpr) = 0;
};


class GroupByItemCBH : public BaseCBH {
public:
    virtual void handleGroupByItem(std::shared_ptr<query::ValueExpr> const& valueExpr) = 0;
};


class LimitClauseCBH: public BaseCBH {
public:
    virtual void handleLimitClause(int limit) = 0;
};


class SimpleIdCBH: public BaseCBH {
public:
    virtual void handleSimpleId(std::string const& val) = 0;
};


class DottedIdCBH: public BaseCBH {
public:
    virtual void handleDottedId(std::string const& dot_id) = 0;
};

class NullNotnullCBH: public BaseCBH {
public:
    // isNotNull will be:
    // true: if the expression is like "NOT NULL",
    // false: if the expression is like "NULL".
    virtual void handleNullNotnull(bool isNotNull) = 0;
};

class SelectColumnElementCBH : public BaseCBH {
public:
    virtual void handleColumnElement(std::shared_ptr<query::ValueExpr> const& columnElement) = 0;
};


class FullColumnNameExpressionAtomCBH : public BaseCBH {
public:
    virtual void HandleFullColumnNameExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) = 0;
};


class BinaryComparasionPredicateCBH : public BaseCBH {
public:
    virtual ~BinaryComparasionPredicateCBH() {}
    virtual void handleBinaryComparasionPredicate(
            std::shared_ptr<query::CompPredicate> const& comparisonPredicate) = 0;
};


class PredicateExpressionCBH : public BaseCBH {
public:
    virtual void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) = 0;
    virtual void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) = 0;
};


class ConstantCBH : public BaseCBH {
public:
    virtual void handleConstant(std::string const& val) = 0;
};


class UidListCBH : public BaseCBH {
public:
    virtual void handleUidList(std::vector<std::string> const& strings) = 0;
};


class ExpressionsCBH : public BaseCBH {
public:
    virtual void handleExpressions(std::vector<std::shared_ptr<query::ValueExpr>> const& valueExprs) = 0;
};


class ConstantsCBH : public BaseCBH {
public:
    virtual void handleConstants(std::vector<std::string> const& values) = 0;
};


class AggregateFunctionCallCBH : public BaseCBH {
public:
    virtual void handleAggregateFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) = 0;
};


class ScalarFunctionCallCBH : public BaseCBH {
public:
    virtual void handleScalarFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) = 0;
};


class UdfFunctionCallCBH : public BaseCBH {
public:
    virtual void handleUdfFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) = 0;
};


class AggregateWindowedFunctionCBH : public BaseCBH {
public:
    virtual void handleAggregateWindowedFunction(std::shared_ptr<query::ValueFactor> const& aggValueFactor) = 0;
};


class ScalarFunctionNameCBH : public BaseCBH {
public:
    virtual void handleScalarFunctionName(std::string const& name) = 0;
};


class FunctionArgsCBH : public BaseCBH {
public:
    virtual void handleFunctionArgs(std::vector<std::shared_ptr<query::ValueExpr>> const& valueExprs) = 0;
};


class FunctionArgCBH : public BaseCBH {
public:
    virtual void handleFunctionArg(std::shared_ptr<query::ValueFactor> const& valueFactor) = 0;
};


class NotExpressionCBH : public BaseCBH {
public:
    virtual void handleNotExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) = 0;
};


class LogicalExpressionCBH : public BaseCBH {
public:
    // pass thru to parent for qserv function spec
    virtual void handleQservFunctionSpec(std::string const& functionName,
            std::vector<std::shared_ptr<query::ValueFactor>> const& args) = 0;

    virtual void handleLogicalExpression(std::shared_ptr<query::LogicalTerm> const& logicalTerm,
            antlr4::ParserRuleContext* childCtx) = 0;
};


class InPredicateCBH : public BaseCBH {
public :
    virtual void handleInPredicate(std::shared_ptr<query::InPredicate> const& inPredicate) = 0;
};


class BetweenPredicateCBH : public BaseCBH {
public:
    virtual void handleBetweenPredicate(std::shared_ptr<query::BetweenPredicate> const& betweenPredicate) = 0;
};


class IsNullPredicateCBH : public BaseCBH {
public:
    virtual void handleIsNullPredicate(std::shared_ptr<query::NullPredicate> const& nullPredicate) = 0;
};


class LikePredicateCBH : public BaseCBH {
public:
    virtual void handleLikePredicate(std::shared_ptr<query::LikePredicate> const& likePredicate) = 0;
};


class NestedExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleNestedExpressionAtom(std::shared_ptr<query::BoolTerm> const& boolTerm) = 0;
    virtual void handleNestedExpressionAtom(std::shared_ptr<query::ValueExpr> const& valueExpr) = 0;
};


class MathExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleMathExpressionAtom(std::shared_ptr<query::ValueExpr> const& valueExpr) = 0;
};


class FunctionCallExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleFunctionCallExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) = 0;
};


class BitExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleBitExpressionAtom(std::shared_ptr<query::ValueExpr> const& valueExpr) = 0;
};


class LogicalOperatorCBH : public BaseCBH {
public:
    enum OperatorType {
        AND,
        OR,
    };
    virtual void handleLogicalOperator(OperatorType operatorType) = 0;

    static std::string OperatorTypeToStr(OperatorType operatorType) {
        return operatorType == AND ? "AND" : "OR";
    }
};


class BitOperatorCBH : public BaseCBH {
public:
    enum OperatorType {
        LEFT_SHIFT,
        RIGHT_SHIFT,
        AND,
        XOR,
        OR,
    };

    virtual void handleBitOperator(OperatorType operatorType) = 0;
};


class MathOperatorCBH : public BaseCBH {
public:
    enum OperatorType {
        SUBTRACT,
        ADD,
        DIVIDE, // `/` operator
        MULTIPLY,
        DIV,    // `DIV` operator
        MOD,    // `MOD` operator
        MODULO, // `%`
    };
    virtual void handleMathOperator(OperatorType operatorType) = 0;
};


class FunctionNameBaseCBH : public BaseCBH {
public:
    virtual void handleFunctionNameBase(std::string const& name) = 0;
};


}}} // lsst::qserv::ccontrol


#endif // LSST_QSERV_CCONTROL_PARSEADAPTERSCBH
