// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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

#include "parser/QSMySqlListener.h"

#include <cxxabi.h>
#include <sstream>
#include <vector>

#include "lsst/log/Log.h"

#include "parser/ParseException.h"
#include "parser/SelectListFactory.h"
#include "parser/ValueExprFactory.h"
#include "parser/ValueFactorFactory.h"
#include "parser/WhereFactory.h"
#include "query/BoolTerm.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/GroupByClause.h"
#include "query/JoinRef.h"
#include "query/OrderByClause.h"
#include "query/Predicate.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/SqlSQL2Tokens.h"
#include "query/TableRef.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "util/IterableFormatter.h"


using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.QSMySqlListener");

std::string getQueryString(antlr4::ParserRuleContext* ctx) {
    return ctx->getStart()->getInputStream()->getText(
           antlr4::misc::Interval(ctx->getStart()->getStartIndex(), ctx->getStop()->getStopIndex()));
}

template <typename T>
std::string getTypeName() {
    int status;
    return abi::__cxa_demangle(typeid(T).name(),0,0,&status);
}

template <typename T>
std::string getTypeName(T obj) {
    int status;
    return abi::__cxa_demangle(typeid(obj).name(),0,0,&status);
}

} // end namespace


// This macro creates the enterXXX and exitXXX function definitions, for functions declared in
// QSMySqlListener.h; the enter function pushes the adapter onto the stack (with parent from top of the
// stack), and the exit function pops the adapter from the top of the stack.
#define ENTER_EXIT_PARENT(NAME) \
void QSMySqlListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) { \
    LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " '" << getQueryString(ctx) << "'"); \
    pushAdapterStack<NAME##CBH, NAME##Adapter, QSMySqlParser::NAME##Context>(ctx); \
} \
\
void QSMySqlListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) { \
    LOGS(_log, LOG_LVL_TRACE, __FUNCTION__); \
    popAdapterStack<NAME##Adapter>(ctx); \
} \


// This macro creates the enterXXX and exitXXX function definitions similar to ENTER_EXIT_PARENT to satisfy
// the QSMySqlParserListener class API but expects that the grammar element will not be used. The enter
// function throws an adapter_order_error so that if the grammar element is unexpectedly entered the query
// parsing will abort.
#define UNHANDLED(NAME) \
void QSMySqlListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) { \
    LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " is UNHANDLED '" << getQueryString(ctx) << "'"); \
    throw adapter_order_error(string(__FUNCTION__) + string(" not supported.")); \
} \
\
void QSMySqlListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {}\


// This macro creates the enterXXX and exitXXX function definitions similar to ENTER_EXIT_PARENT but does not
// push (or pop) an adapter on the stack. Other adapters are expected to handle the grammar element as may be
// appropraite.
#define IGNORED(NAME) \
void QSMySqlListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) { \
    LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " is IGNORED"); \
} \
\
void QSMySqlListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {\
    LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " is IGNORED"); \
} \


// This macro is similar to IGNORED, but allows the enter message to log a specific warning message when it is
// called.
#define IGNORED_WARN(NAME, WARNING) \
void QSMySqlListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) { \
    LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " is IGNORED"); \
} \
\
void QSMySqlListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {\
    LOGS(_log, LOG_LVL_TRACE, __FUNCTION__ << " is IGNORED"); \
} \


// A fucntion to assert certain condtions during runtime. In the case of a failure an adapter_exectuion_error
// is raised with a context specific message.
#define ASSERT_EXECUTION_CONDITION(CONDITION, MESSAGE, CTX) \
if (false == (CONDITION)) { \
    { \
        ostringstream msg; \
        msg << getTypeName(this) << "::" << __FUNCTION__; \
        msg << " messsage:\"" << MESSAGE << "\""; \
        msg << ", in query:" << getStatementStr(); \
        msg << ", in or around query segment: '" << getQueryString(CTX) << "'"; \
        msg << ", with adapter stack:" << adapterStackToString(); \
        msg << ", string tree:" << getStringTree(); \
        msg << ", tokens:" << getTokens(); \
        throw adapter_execution_error(msg.str()); \
    } \
} \


namespace lsst {
namespace qserv {
namespace parser {

/// Callback Handler classes

class BaseCBH {
public:
    virtual ~BaseCBH() {}
};


class DmlStatementCBH : public BaseCBH {
public:
    virtual void handleDmlStatement(shared_ptr<query::SelectStmt> const & selectStatement) = 0;
};


class SimpleSelectCBH : public BaseCBH {
public:
    virtual void handleSelectStatement(shared_ptr<query::SelectStmt> const & selectStatement) = 0;
};


class QuerySpecificationCBH : public BaseCBH {
public:
    virtual void handleQuerySpecification(shared_ptr<query::SelectList> const & selectList,
                                          shared_ptr<query::FromList> const & fromList,
                                          shared_ptr<query::WhereClause> const & whereClause,
                                          shared_ptr<query::OrderByClause> const & orderByClause,
                                          int limit,
                                          shared_ptr<query::GroupByClause> const & groupByClause,
                                          bool distinct) = 0;
};


class SelectElementsCBH : public BaseCBH {
public:
    virtual void handleSelectList(shared_ptr<query::SelectList> const & selectList) = 0;
};


class FullColumnNameCBH : public BaseCBH {
public:
    virtual void handleFullColumnName(shared_ptr<query::ValueFactor> const & valueFactor) = 0;
};


class TableNameCBH : public BaseCBH {
public:
    virtual void handleTableName(vector<string> const & str) = 0;
};


class FromClauseCBH : public BaseCBH {
public:
    virtual void handleFromClause(shared_ptr<query::FromList> const & fromList,
                                  shared_ptr<query::WhereClause> const & whereClause,
                                  shared_ptr<query::GroupByClause> const & groupByClause) = 0;
};


class TableSourcesCBH : public BaseCBH {
public:
    virtual void handleTableSources(query::TableRefListPtr const & tableRefList) = 0;
};

class TableSourceBaseCBH : public BaseCBH {
public:
    virtual void handleTableSource(shared_ptr<query::TableRef> const & tableRef) = 0;
};


class AtomTableItemCBH : public BaseCBH {
public:
    virtual void handleAtomTableItem(shared_ptr<query::TableRef> const & tableRef) = 0;
};


class UidCBH : public BaseCBH {
public:
    virtual void handleUid(string const & string) = 0;
};


class FullIdCBH : public BaseCBH {
public:
    virtual void handleFullId(vector<string> const & uidlist) = 0;
};


class ConstantExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleConstantExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) = 0;
};


class ExpressionAtomPredicateCBH : public BaseCBH {
public:
    virtual void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr> const & valueExpr,
            antlr4::ParserRuleContext* childCtx) = 0;

    virtual void handleExpressionAtomPredicate(shared_ptr<query::BoolFactorTerm> const & boolFactorTerm,
            antlr4::ParserRuleContext* childCtx) = 0;
};


class QservFunctionSpecCBH : public BaseCBH {
public:
    virtual void handleQservFunctionSpec(string const & functionName,
            vector<shared_ptr<query::ValueFactor>> const & args) = 0;
};


class ComparisonOperatorCBH : public BaseCBH {
public:
    virtual void handleComparisonOperator(string const & text) = 0;
};


class OrderByClauseCBH : public BaseCBH {
public:
    virtual void handleOrderByClause(shared_ptr<query::OrderByClause> const & orderByClause) = 0;
};


class OrderByExpressionCBH : public BaseCBH {
public:
    virtual void handleOrderByExpression(query::OrderByTerm const & orderByTerm) = 0;
};

class InnerJoinCBH : public BaseCBH {
public:
    virtual void handleInnerJoin(shared_ptr<query::JoinRef> const & joinRef) = 0;
};

class SelectSpecCBH : public BaseCBH {
public:
    virtual void handleSelectSpec(bool distinct) = 0;
};

class SelectFunctionElementCBH: public BaseCBH {
public:
    virtual void handleSelectFunctionElement(shared_ptr<query::ValueExpr> const & selectFunction) = 0;
};


class GroupByItemCBH : public BaseCBH {
public:
    virtual void handleGroupByItem(shared_ptr<query::ValueExpr> const & valueExpr) = 0;
};


class LimitClauseCBH: public BaseCBH {
public:
    virtual void handleLimitClause(int limit) = 0;
};

class SimpleIdCBH: public BaseCBH {
public:
    virtual void handleSimpleId(string const & val) = 0;
};

class DottedIdCBH: public BaseCBH {
public:
    virtual void handleDottedId(string const & dot_id) = 0;
};


class SelectColumnElementCBH : public BaseCBH {
public:
    virtual void handleColumnElement(shared_ptr<query::ValueExpr> const & columnElement) = 0;
};


class FullColumnNameExpressionAtomCBH : public BaseCBH {
public:
    virtual void HandleFullColumnNameExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) = 0;
};


class BinaryComparasionPredicateCBH : public BaseCBH {
public:
    virtual ~BinaryComparasionPredicateCBH() {}
    virtual void handleBinaryComparasionPredicate(
            shared_ptr<query::CompPredicate> const & comparisonPredicate) = 0;
};


class PredicateExpressionCBH : public BaseCBH {
public:
    virtual void handlePredicateExpression(shared_ptr<query::BoolFactor> const & boolFactor) = 0;
    virtual void handlePredicateExpression(shared_ptr<query::ValueExpr> const & valueExpr) = 0;
};


class ConstantCBH : public BaseCBH {
public:
    virtual void handleConstant(string const & val) = 0;
};


class UidListCBH : public BaseCBH {
public:
    virtual void handleUidList(vector<string> const & strings) = 0;
};

class ExpressionsCBH : public BaseCBH {
public:
    virtual void handleExpressions(vector<shared_ptr<query::ValueExpr>> const & valueExprs) = 0;
};


class ConstantsCBH : public BaseCBH {
public:
    virtual void handleConstants(vector<string> const & values) = 0;
};


class AggregateFunctionCallCBH : public BaseCBH {
public:
    virtual void handleAggregateFunctionCall(shared_ptr<query::ValueFactor> const & aggValueFactor) = 0;
};


class ScalarFunctionCallCBH : public BaseCBH {
public:
    virtual void handleScalarFunctionCall(shared_ptr<query::FuncExpr> const & funcExpr) = 0;
};


class UdfFunctionCallCBH : public BaseCBH {
public:
    virtual void handleUdfFunctionCall(shared_ptr<query::FuncExpr> const & valueExpr) = 0;
};


class AggregateWindowedFunctionCBH : public BaseCBH {
public:
    virtual void handleAggregateWindowedFunction(shared_ptr<query::ValueFactor> const & aggValueFactor) = 0;
};


class ScalarFunctionNameCBH : public BaseCBH {
public:
    virtual void handleScalarFunctionName(string const & name) = 0;
};


class FunctionArgsCBH : public BaseCBH {
public:
    virtual void handleFunctionArgs(vector<shared_ptr<query::ValueExpr>> const & valueExprs) = 0;
};


class FunctionArgCBH : public BaseCBH {
public:
    virtual void handleFunctionArg(shared_ptr<query::ValueFactor> const & valueFactor) = 0;
};


class LogicalExpressionCBH : public BaseCBH {
public:
    // pass thru to parent for qserv function spec
    virtual void handleQservFunctionSpec(string const & functionName,
            vector<shared_ptr<query::ValueFactor>> const & args) = 0;

    virtual void handleLogicalExpression(shared_ptr<query::LogicalTerm> const & logicalTerm,
            antlr4::ParserRuleContext* childCtx) = 0;
};


class InPredicateCBH : public BaseCBH {
public :
    virtual void handleInPredicate(shared_ptr<query::InPredicate> const & inPredicate) = 0;
};


class BetweenPredicateCBH : public BaseCBH {
public:
    virtual void handleBetweenPredicate(shared_ptr<query::BetweenPredicate> const & betweenPredicate) = 0;
};


class LikePredicateCBH : public BaseCBH {
public:
    virtual void handleLikePredicate(shared_ptr<query::LikePredicate> const & likePredicate) = 0;
};


class UnaryExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleUnaryExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) = 0;
};


class NestedExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleNestedExpressionAtom(shared_ptr<query::BoolFactorTerm> const & boolFactorTerm) = 0;
};


class MathExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleMathExpressionAtomAdapter(shared_ptr<query::ValueExpr> const & valueExpr) = 0;
};


class FunctionCallExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleFunctionCallExpressionAtom(shared_ptr<query::FuncExpr> const & funcExpr) = 0;
};


class UnaryOperatorCBH : public BaseCBH {
public:
    virtual void handleUnaryOperator(string const & val) = 0;
};


class LogicalOperatorCBH : public BaseCBH {
public:
    enum OperatorType {
        AND,
        OR,
    };
    virtual void handleLogicalOperator(OperatorType operatorType) = 0;
};


class MathOperatorCBH : public BaseCBH {
public:
    enum OperatorType {
        SUBTRACT,
        ADD,
    };
    virtual void handleMathOperator(OperatorType operatorType) = 0;
};


class FunctionNameBaseCBH : public BaseCBH {
public:
    virtual void handleFunctionNameBase(string const & name) = 0;
};

/// Adapter classes


// Adapter is the base class that represents a node in the antlr4 syntax tree. There is a one-to-one
// relationship between types of adapter subclass and each variation of enter/exit functions that are the
// result of the grammar defined in QSMySqlParser.g4 and the enter/exit functions that the antlr4 code generater
// creates in MySqlParserBaseListener.
class Adapter {
public:
    Adapter() {}
    virtual ~Adapter() {}

    // onEnter is called just after the Adapter is pushed onto the context stack
    virtual void onEnter() {}

    // onExit is called just before the Adapter is popped from the context stack
    virtual void onExit() = 0;

    virtual string name() const = 0;
};


template <typename CBH, typename CTX>
class AdapterT : public Adapter {
public:
    AdapterT(shared_ptr<CBH> const & parent, CTX * ctx, QSMySqlListener const * const listener)
    : _ctx(ctx), qsMySqlListener(listener), _parent(parent) {}

protected:
    shared_ptr<CBH> lockedParent() {
        shared_ptr<CBH> parent = _parent.lock();
        if (nullptr == parent) {
            throw adapter_execution_error(
                    "Locking weak ptr to parent callback handler returned null");
        }
        return parent;
    }

    CTX* _ctx;

    // Used for error messages, uses the QSMySqlListener to get a list of the names of the adapters in the
    // adapter stack,
    std::string adapterStackToString() const { return qsMySqlListener->adapterStackToString(); }
    std::string getStringTree() const { return qsMySqlListener->getStringTree(); }
    std::string getTokens() const { return qsMySqlListener->getTokens(); }
    std::string getStatementStr() const { return qsMySqlListener->getStatementStr(); }


    // TODO write an accessor?
    // Mostly the QSMySqlListener is not used by adapters. It is needed to get the adapter stack list for
    // error messages.
    QSMySqlListener const * const qsMySqlListener;

private:
    weak_ptr<CBH> _parent;
};


class RootAdapter :
        public Adapter,
        public DmlStatementCBH {
public:
    RootAdapter()
    : _ctx(nullptr)
    , qsMySqlListener(nullptr)
    {}

    shared_ptr<query::SelectStmt> const & getSelectStatement() { return _selectStatement; }

    void handleDmlStatement(shared_ptr<query::SelectStmt> const & selectStatement) override {
        _selectStatement = selectStatement;
    }

    void onEnter(QSMySqlParser::RootContext* ctx, QSMySqlListener const * const listener) {
        _ctx = ctx;
        qsMySqlListener = listener;
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_selectStatement != nullptr, "Could not parse query.", _ctx);
    }

    string name() const override { return getTypeName(this); }

    std::string adapterStackToString() const { return qsMySqlListener->adapterStackToString(); }
    std::string getStringTree() const { return qsMySqlListener->getStringTree(); }
    std::string getTokens() const { return qsMySqlListener->getTokens(); }
    std::string getStatementStr() const { return qsMySqlListener->getStatementStr(); }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
    QSMySqlParser::RootContext* _ctx;
    QSMySqlListener const * qsMySqlListener;
};


class DmlStatementAdapter :
        public AdapterT<DmlStatementCBH, QSMySqlParser::DmlStatementContext>,
        public SimpleSelectCBH {
public:
    using AdapterT::AdapterT;

    void handleSelectStatement(shared_ptr<query::SelectStmt> const & selectStatement) override {
        _selectStatement = selectStatement;
    }

    void onExit() override {
        lockedParent()->handleDmlStatement(_selectStatement);
    }

    string name () const override { return getTypeName(this); }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
};


class SimpleSelectAdapter :
        public AdapterT<SimpleSelectCBH, QSMySqlParser::SimpleSelectContext>,
        public QuerySpecificationCBH {
public:
    using AdapterT::AdapterT;

    void handleQuerySpecification(shared_ptr<query::SelectList> const & selectList,
                                  shared_ptr<query::FromList> const & fromList,
                                  shared_ptr<query::WhereClause> const & whereClause,
                                  shared_ptr<query::OrderByClause> const & orderByClause,
                                  int limit,
                                  shared_ptr<query::GroupByClause> const & groupByClause,
                                  bool distinct) override {
        _selectList = selectList;
        _fromList = fromList;
        _whereClause = whereClause;
        _orderByClause = orderByClause;
        _limit = limit;
        _groupByClause = groupByClause;
        _distinct = distinct;
    }

    void onExit() override {
        auto selectStatement = make_shared<query::SelectStmt>(_fromList, _selectList, _whereClause,
                _orderByClause, _groupByClause, nullptr, _distinct, _limit);
        lockedParent()->handleSelectStatement(selectStatement);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::SelectList> _selectList;
    shared_ptr<query::FromList> _fromList;
    shared_ptr<query::WhereClause> _whereClause;
    shared_ptr<query::OrderByClause> _orderByClause;
    shared_ptr<query::GroupByClause> _groupByClause;
    int _limit{lsst::qserv::NOTSET};
    int _distinct{false};
};


class QuerySpecificationAdapter :
        public AdapterT<QuerySpecificationCBH, QSMySqlParser::QuerySpecificationContext>,
        public SelectElementsCBH,
        public FromClauseCBH,
        public OrderByClauseCBH,
        public LimitClauseCBH,
        public SelectSpecCBH {
public:
    using AdapterT::AdapterT;

    void handleSelectList(shared_ptr<query::SelectList> const & selectList) override {
        _selectList = selectList;
    }

    void handleFromClause(shared_ptr<query::FromList> const & fromList,
                          shared_ptr<query::WhereClause> const & whereClause,
                          shared_ptr<query::GroupByClause> const & groupByClause) override {
        _fromList = fromList;
        _whereClause = whereClause;
        _groupByClause = groupByClause;
    }

    void handleOrderByClause(shared_ptr<query::OrderByClause> const & orderByClause) {
        _orderByClause = orderByClause;
    }

    void handleLimitClause(int limit) override {
        _limit = limit;
    }

    void handleSelectSpec(bool distinct) override {
        _distinct = distinct;
    }

    void onExit() override {
        lockedParent()->handleQuerySpecification(_selectList, _fromList, _whereClause, _orderByClause,
                _limit, _groupByClause, _distinct);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::WhereClause> _whereClause;
    shared_ptr<query::FromList> _fromList;
    shared_ptr<query::SelectList> _selectList;
    shared_ptr<query::OrderByClause> _orderByClause;
    shared_ptr<query::GroupByClause> _groupByClause;
    int _limit{lsst::qserv::NOTSET};
    bool _distinct{false};
};


class SelectElementsAdapter :
        public AdapterT<SelectElementsCBH, QSMySqlParser::SelectElementsContext>,
        public SelectColumnElementCBH,
        public SelectFunctionElementCBH {
public:
    using AdapterT::AdapterT;

    void onEnter() override {
        if (_ctx->star != nullptr) {
            SelectListFactory::addStarFactor(_selectList);
        }
    }

    void handleColumnElement(shared_ptr<query::ValueExpr> const & columnElement) override {
        SelectListFactory::addValueExpr(_selectList, columnElement);
    }

    void handleSelectFunctionElement(shared_ptr<query::ValueExpr> const & selectFunction) override {
        SelectListFactory::addSelectAggFunction(_selectList, selectFunction);
    }

    void onExit() override {
        lockedParent()->handleSelectList(_selectList);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::SelectList> _selectList{make_shared<query::SelectList>()};
};


class FromClauseAdapter :
        public AdapterT<FromClauseCBH, QSMySqlParser::FromClauseContext>,
        public TableSourcesCBH,
        public PredicateExpressionCBH,
        public LogicalExpressionCBH,
        public QservFunctionSpecCBH,
        public GroupByItemCBH {
public:
    using AdapterT::AdapterT;

    void handleTableSources(query::TableRefListPtr const & tableRefList) override {
        _tableRefList = tableRefList;
    }

    void handlePredicateExpression(shared_ptr<query::BoolFactor> const & boolFactor) override {
        shared_ptr<query::AndTerm> andTerm = make_shared<query::AndTerm>();
        shared_ptr<query::BoolTerm> boolTerm = boolFactor;
        andTerm->addBoolTerm(boolTerm);
        shared_ptr<query::BoolTerm> andBoolTerm = andTerm;
        _getRootTerm()->addBoolTerm(andBoolTerm);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr> const & valueExpr) override {
        ASSERT_EXECUTION_CONDITION(false, "Unhandled valueExpr predicateExpression.", _ctx);
    }

    void handleLogicalExpression(shared_ptr<query::LogicalTerm> const & logicalTerm,
            antlr4::ParserRuleContext* childCtx) override {
        if (_ctx->whereExpr == childCtx) {
            auto boolTerm = shared_ptr<query::BoolTerm>(logicalTerm);
            _getRootTerm()->addBoolTerm(boolTerm);
            return;
        } else if (_ctx->havingExpr == childCtx) {
            ASSERT_EXECUTION_CONDITION(false, "The HAVING expression is not yet supported.", _ctx);
        }
        ASSERT_EXECUTION_CONDITION(false, "This logical expression is not yet supported.", _ctx);
    }

    void handleQservFunctionSpec(string const & functionName,
            vector<shared_ptr<query::ValueFactor>> const & args) {
        _initWhereClause();
        WhereFactory::addQservRestrictor(_whereClause, functionName, args);
    }

    void handleGroupByItem(shared_ptr<query::ValueExpr> const & valueExpr) {
        if (nullptr == _groupByClause) {
            _groupByClause = make_shared<query::GroupByClause>();
        }
        _groupByClause->addTerm(query::GroupByTerm(valueExpr, ""));
    }

    void onExit() override {
        shared_ptr<query::FromList> fromList = make_shared<query::FromList>(_tableRefList);
        if (_rootTerm != nullptr) {
            _initWhereClause();
            _whereClause->setRootTerm(_rootTerm);
        }

        lockedParent()->handleFromClause(fromList, _whereClause, _groupByClause);
    }

    string name() const override { return getTypeName(this); }

private:
    void _initWhereClause() {
        if (nullptr == _whereClause) {
            _whereClause = make_shared<query::WhereClause>();
        }
    }

    shared_ptr<query::OrTerm> const & _getRootTerm() {
        if (nullptr == _rootTerm) {
            _rootTerm = make_shared<query::OrTerm>();
        }
        return _rootTerm;
    }

    // I think the first term of a where clause is always an OrTerm, and it needs to be added by default.
    shared_ptr<query::WhereClause> _whereClause;
    query::TableRefListPtr _tableRefList;
    shared_ptr<query::OrTerm> _rootTerm;
    shared_ptr<query::GroupByClause> _groupByClause;
};


class TableSourcesAdapter :
        public AdapterT<TableSourcesCBH, QSMySqlParser::TableSourcesContext>,
        public TableSourceBaseCBH {
public:
    using AdapterT::AdapterT;

    void handleTableSource(shared_ptr<query::TableRef> const & tableRef) override {
        _tableRefList->push_back(tableRef);
    }

    void onExit() override {
        lockedParent()->handleTableSources(_tableRefList);
    }

    string name() const override { return getTypeName(this); }

private:
    query::TableRefListPtr _tableRefList{make_shared<query::TableRefList>()};
};


class TableSourceBaseAdapter :
        public AdapterT<TableSourceBaseCBH, QSMySqlParser::TableSourceBaseContext>,
        public AtomTableItemCBH,
        public InnerJoinCBH {
public:
    using AdapterT::AdapterT;

    void handleAtomTableItem(shared_ptr<query::TableRef> const & tableRef) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _tableRef, "expeceted one AtomTableItem callback.", _ctx);
        _tableRef = tableRef;
    }

    void handleInnerJoin(shared_ptr<query::JoinRef> const & joinRef) override {
        _joinRefs.push_back(joinRef);
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_tableRef != nullptr, "tableRef was not populated.", _ctx);
        _tableRef->addJoins(_joinRefs);
        lockedParent()->handleTableSource(_tableRef);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::TableRef> _tableRef;
    vector<shared_ptr<query::JoinRef>> _joinRefs;
};


class AtomTableItemAdapter :
        public AdapterT<AtomTableItemCBH, QSMySqlParser::AtomTableItemContext>,
        public TableNameCBH,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    void handleTableName(vector<string> const & uidlist) override {
        if (uidlist.size() == 1) {
            _table = uidlist.at(0);
        } else if (uidlist.size() == 2) {
            _db = uidlist.at(0);
            _table = uidlist.at(1);
        } else {
            ASSERT_EXECUTION_CONDITION(false, "Illegal number of UIDs in table reference.", _ctx);
        }
    }

    void handleUid(string const & string) override {
        _alias = string;
    }

    void onExit() override {
        shared_ptr<query::TableRef> tableRef = make_shared<query::TableRef>(_db, _table, _alias);
        lockedParent()->handleAtomTableItem(tableRef);
    }

    string name() const override { return getTypeName(this); }

protected:
    string _db;
    string _table;
    string _alias;
};


class TableNameAdapter :
        public AdapterT<TableNameCBH, QSMySqlParser::TableNameContext>,
        public FullIdCBH {
public:
    using AdapterT::AdapterT;

    void handleFullId(vector<string> const & uidlist) override {
        lockedParent()->handleTableName(uidlist);
    }

    void onExit() override {}

    string name() const override { return getTypeName(this); }
};



class FullIdAdapter :
        public AdapterT<FullIdCBH, QSMySqlParser::FullIdContext>,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    virtual ~FullIdAdapter() {}

    void handleUid(string const & str) override {
        _uidlist.push_back(str);
        if (_ctx && _ctx->DOT_ID()) {
            string s = _ctx->DOT_ID()->getText();
            if (s.front() == '.') _uidlist.push_back(s.erase(0,1));
            else _uidlist.push_back(s);
        }
    }

    void onExit() override {
        lockedParent()->handleFullId(_uidlist);
    }

    string name() const override { return getTypeName(this); }

private:
    vector<string> _uidlist;
};


class FullColumnNameAdapter :
        public AdapterT<FullColumnNameCBH, QSMySqlParser::FullColumnNameContext>,
        public UidCBH,
        public DottedIdCBH {
public:
    using AdapterT::AdapterT;

    void handleUid(string const & string) override {
        _strings.push_back(string);
    }

    void handleDottedId(string const & dot_id) override {
        _strings.push_back(dot_id);
    }

    void onExit() override {
        shared_ptr<query::ValueFactor> valueFactor;
        switch(_strings.size()) {
        case 1:
            // only 1 value is set in strings; it is the column name.
            valueFactor = ValueFactorFactory::newColumnColumnFactor("", "", _strings[0]);
            break;
        case 2:
            // 2 values are set in strings; they are table and column name.
            valueFactor = ValueFactorFactory::newColumnColumnFactor("", _strings[0], _strings[1]);
            break;
        case 3:
            // 3 values are set in strings; they are database name, table name, and column name.
            valueFactor = ValueFactorFactory::newColumnColumnFactor(_strings[0], _strings[1], _strings[2]);
            break;
        default:
            ASSERT_EXECUTION_CONDITION(false, "Unhandled number of strings.", _ctx);
        }
        lockedParent()->handleFullColumnName(valueFactor);
    }

    string name() const override { return getTypeName(this); }

private:
    vector<string> _strings;
};


class ConstantExpressionAtomAdapter :
        public AdapterT<ConstantExpressionAtomCBH, QSMySqlParser::ConstantExpressionAtomContext>,
        public ConstantCBH {
public:
    using AdapterT::AdapterT;

    void handleConstant(string const & val) override {
        lockedParent()->handleConstantExpressionAtom(query::ValueFactor::newConstFactor(val));
    }

    void onExit() override {}

    string name() const override { return getTypeName(this); }
};



class FullColumnNameExpressionAtomAdapter :
        public AdapterT<FullColumnNameExpressionAtomCBH, QSMySqlParser::FullColumnNameExpressionAtomContext>,
        public FullColumnNameCBH {
public:
    using AdapterT::AdapterT;

    void handleFullColumnName(shared_ptr<query::ValueFactor> const & valueFactor) override {
        lockedParent()->HandleFullColumnNameExpressionAtom(valueFactor);
    }

    void onExit() override {}

    string name() const override { return getTypeName(this); }
};



class ExpressionAtomPredicateAdapter :
        public AdapterT<ExpressionAtomPredicateCBH, QSMySqlParser::ExpressionAtomPredicateContext>,
        public ConstantExpressionAtomCBH,
        public FullColumnNameExpressionAtomCBH,
        public FunctionCallExpressionAtomCBH,
        public NestedExpressionAtomCBH,
        public MathExpressionAtomCBH,
        public UnaryExpressionAtomCBH {
public:
    using AdapterT::AdapterT;

    void handleConstantExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) override {
        auto valueExpr = query::ValueExpr::newSimple(valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleFunctionCallExpressionAtom(shared_ptr<query::FuncExpr> const & funcExpr) override {
        auto valueFactor = query::ValueFactor::newFuncFactor(funcExpr);
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleMathExpressionAtomAdapter(shared_ptr<query::ValueExpr> const & valueExpr) override {
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void HandleFullColumnNameExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) override {
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleNestedExpressionAtom(shared_ptr<query::BoolFactorTerm> const & boolFactorTerm) override {
        lockedParent()->handleExpressionAtomPredicate(boolFactorTerm, _ctx);
    }

    void handleUnaryExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) override {
        auto valueExpr = query::ValueExpr::newSimple(valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void onEnter() override {
        ASSERT_EXECUTION_CONDITION(_ctx->LOCAL_ID() == nullptr, "LOCAL_ID is not supported", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->VAR_ASSIGN() == nullptr, "VAR_ASSIGN is not supported", _ctx);
    }

    void onExit() override {}

    string name() const override { return getTypeName(this); }
};


class QservFunctionSpecAdapter :
        public AdapterT<QservFunctionSpecCBH, QSMySqlParser::QservFunctionSpecContext>,
        public ConstantsCBH {
public:
    using AdapterT::AdapterT;

    void handleConstants(vector<string> const & values) override {
        ASSERT_EXECUTION_CONDITION(_args.empty(), "args should be set exactly once.", _ctx);
        for (auto&& value : values) {
            _args.push_back(query::ValueFactor::newConstFactor(value));
        }
    }

    void onExit() override {
        lockedParent()->handleQservFunctionSpec(getFunctionName(), _args);
    }

    string name() const override { return getTypeName(this); }

private:
    string getFunctionName() {
        if (_ctx->QSERV_AREASPEC_BOX() != nullptr){
            return _ctx->QSERV_AREASPEC_BOX()->getSymbol()->getText();
        }
        if (_ctx->QSERV_AREASPEC_CIRCLE() != nullptr){
            return _ctx->QSERV_AREASPEC_CIRCLE()->getSymbol()->getText();
        }
        if (_ctx->QSERV_AREASPEC_ELLIPSE() != nullptr){
            return _ctx->QSERV_AREASPEC_ELLIPSE()->getSymbol()->getText();
        }
        if (_ctx->QSERV_AREASPEC_POLY() != nullptr){
            return _ctx->QSERV_AREASPEC_POLY()->getSymbol()->getText();
        }
        if (_ctx->QSERV_AREASPEC_HULL() != nullptr){
            return _ctx->QSERV_AREASPEC_HULL()->getSymbol()->getText();
        }
        ASSERT_EXECUTION_CONDITION(false, "could not get qserv function name.", _ctx);
    }

    vector<shared_ptr<query::ValueFactor>> _args;
};


// PredicateExpressionAdapter gathers BoolFactors into a BoolFactor (which is a BoolTerm).
class PredicateExpressionAdapter :
        public AdapterT<PredicateExpressionCBH, QSMySqlParser::PredicateExpressionContext>,
        public BinaryComparasionPredicateCBH,
        public BetweenPredicateCBH,
        public InPredicateCBH,
        public ExpressionAtomPredicateCBH,
        public LikePredicateCBH {
public:
    using AdapterT::AdapterT;

    // BinaryComparasionPredicateCBH
    void handleBinaryComparasionPredicate(
            shared_ptr<query::CompPredicate> const & comparisonPredicate) override {
        _prepBoolFactor();
        _boolFactor->addBoolFactorTerm(comparisonPredicate);
    }

    void handleBetweenPredicate(shared_ptr<query::BetweenPredicate> const & betweenPredicate) override {
        _prepBoolFactor();
        _boolFactor->addBoolFactorTerm(betweenPredicate);
    }

    void handleInPredicate(shared_ptr<query::InPredicate> const & inPredicate) override {
        _prepBoolFactor();
        _boolFactor->addBoolFactorTerm(inPredicate);
    }

    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr> const & valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        _prepValueExpr();
        _valueExpr = valueExpr;
    }

    void handleExpressionAtomPredicate(shared_ptr<query::BoolFactorTerm> const & boolFactorTerm,
            antlr4::ParserRuleContext* childCtx) override {
        _prepBoolFactor();
        _boolFactor->addBoolFactorTerm(boolFactorTerm);
    }

    void handleLikePredicate(shared_ptr<query::LikePredicate> const & likePredicate) override {
        _prepBoolFactor();
        _boolFactor->addBoolFactorTerm(likePredicate);
    }

    void onExit() {
        ASSERT_EXECUTION_CONDITION(nullptr != _valueExpr || nullptr != _boolFactor,
                "PredicateExpressionAdapter was not populated.", _ctx);
        if (_boolFactor != nullptr) {
            lockedParent()->handlePredicateExpression(_boolFactor);
        } else if (_valueExpr != nullptr) {
            lockedParent()->handlePredicateExpression(_valueExpr);
        } else {

        }
    }

    string name() const override { return getTypeName(this); }

private:
    void _prepBoolFactor() {
        ASSERT_EXECUTION_CONDITION(nullptr == _valueExpr, "Can't use PredicateExpressionAdapter for " <<
                "BoolFactor and ValueExpr at the same time.", _ctx);
        if (nullptr == _boolFactor) {
            _boolFactor = make_shared<query::BoolFactor>();
        }
    }

    void _prepValueExpr() {
        ASSERT_EXECUTION_CONDITION(nullptr == _boolFactor, "Can't use PredicateExpressionAdapter for " <<
                "BoolFactor and ValueExpr at the same time.", _ctx);
        ASSERT_EXECUTION_CONDITION(nullptr == _valueExpr, "Can only set _valueExpr once.", _ctx);
    }

    shared_ptr<query::BoolFactor> _boolFactor;
    shared_ptr<query::ValueExpr> _valueExpr;
};


class BinaryComparasionPredicateAdapter :
        public AdapterT<BinaryComparasionPredicateCBH, QSMySqlParser::BinaryComparasionPredicateContext>,
        public ExpressionAtomPredicateCBH,
        public ComparisonOperatorCBH {
public:
    using AdapterT::AdapterT;

    void handleComparisonOperator(string const & text) override {
        ASSERT_EXECUTION_CONDITION(_comparison.empty(), "comparison must be set only once.", _ctx);
        _comparison = text;
    }

    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr> const & valueExpr,
            antlr4::ParserRuleContext* ctx) override {
        if (_left == nullptr) {
            _left = valueExpr;
        } else if (_right == nullptr) {
            _right = valueExpr;
        } else {
            ASSERT_EXECUTION_CONDITION(false, "left and right values must be set only once.", _ctx)
        }
    }

    void handleExpressionAtomPredicate(shared_ptr<query::BoolFactorTerm> const & boolFactorTerm,
            antlr4::ParserRuleContext* childCtx) override {
        ASSERT_EXECUTION_CONDITION(false, "unhandled ExpressionAtomPredicate BoolFactor callback.", _ctx);
    }

    void onExit() {
        ASSERT_EXECUTION_CONDITION(_left != nullptr && _right != nullptr,
                "left and right values must both be populated", _ctx);

        auto compPredicate = make_shared<query::CompPredicate>();
        compPredicate->left = _left;

        // We need to remove the coupling between the query classes and the parser classes, in this case where
        // the query classes use the integer token types instead of some other system. For now this if/else
        // block allows us to go from the token string to the SqlSQL2Tokens type defined by the antlr2/3
        // grammar and used by the query objects.
        if ("=" == _comparison) {
            compPredicate->op = SqlSQL2Tokens::EQUALS_OP;
        } else if (">" == _comparison) {
            compPredicate->op = SqlSQL2Tokens::GREATER_THAN_OP;
        } else if ("<" == _comparison) {
            compPredicate->op = SqlSQL2Tokens::LESS_THAN_OP;
        } else if ("<>" == _comparison) {
            compPredicate->op = SqlSQL2Tokens::NOT_EQUALS_OP;
        } else if ("!=" == _comparison) {
            compPredicate->op = SqlSQL2Tokens::NOT_EQUALS_OP;
        } else {
            ASSERT_EXECUTION_CONDITION(false, "unhandled comparison operator type " << _comparison, _ctx);
        }

        compPredicate->right = _right;

        lockedParent()->handleBinaryComparasionPredicate(compPredicate);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueExpr> _left;
    string _comparison;
    shared_ptr<query::ValueExpr> _right;
};


class ComparisonOperatorAdapter :
        public AdapterT<ComparisonOperatorCBH, QSMySqlParser::ComparisonOperatorContext> {
public:
    using AdapterT::AdapterT;

    void onExit() override {
        lockedParent()->handleComparisonOperator(_ctx->getText());
    }

    string name() const override { return getTypeName(this); }
};



class OrderByClauseAdapter :
        public AdapterT<OrderByClauseCBH, QSMySqlParser::OrderByClauseContext>,
        public OrderByExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handleOrderByExpression(query::OrderByTerm const & orderByTerm) {
        _orderByClause->addTerm(orderByTerm);
    }

    void onExit() override {
        lockedParent()->handleOrderByClause(_orderByClause);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::OrderByClause> _orderByClause { make_shared<query::OrderByClause>() };
};


class OrderByExpressionAdapter :
        public AdapterT<OrderByExpressionCBH, QSMySqlParser::OrderByExpressionContext>,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void onEnter() override {
        if (_ctx->ASC() == nullptr && _ctx->DESC() != nullptr) {
            orderBy = query::OrderByTerm::DESC;
        } else if (_ctx->ASC() != nullptr && _ctx->DESC() == nullptr) {
            orderBy = query::OrderByTerm::ASC;
        } else if (_ctx->ASC() != nullptr && _ctx->DESC() != nullptr) {
            ASSERT_EXECUTION_CONDITION(false, "having both ASC and DESC is unhandled.", _ctx);
        }
        // note that query::OrderByTerm::DEFAULT is the default value of orderBy
    }

    void handlePredicateExpression(shared_ptr<query::BoolFactor> const & boolFactor) override {
        ASSERT_EXECUTION_CONDITION(false, "unexpected BoolFactor callback", _ctx);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr> const & valueExpr) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _valueExpr, "expected exactly one ValueExpr callback", _ctx);
        _valueExpr = valueExpr;
    }

    void onExit() override {
        query::OrderByTerm orderByTerm(_valueExpr, orderBy, "");
        lockedParent()->handleOrderByExpression(orderByTerm);
    }

    string name() const override { return getTypeName(this); }

private:
    query::OrderByTerm::Order orderBy {query::OrderByTerm::DEFAULT};
    shared_ptr<query::ValueExpr> _valueExpr;
};


class InnerJoinAdapter :
        public AdapterT<InnerJoinCBH, QSMySqlParser::InnerJoinContext>,
        public AtomTableItemCBH,
        public UidListCBH {
public:
    using AdapterT::AdapterT;

    void onEnter() override {
        ASSERT_EXECUTION_CONDITION(nullptr == _ctx->INNER() && nullptr == _ctx->CROSS(),
                "INNER and CROSS join are not currently supported by the parser.", _ctx);
    }

    void handleAtomTableItem(shared_ptr<query::TableRef> const & tableRef) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _tableRef, "expected only one atomTableItem callback.", _ctx);
        _tableRef = tableRef;
    }

    void handleUidList(vector<string> const & strings) override {
        ASSERT_EXECUTION_CONDITION(strings.size() == 1,
            "Current intermediate representation can only handle 1 `using` string.", _ctx);
        ASSERT_EXECUTION_CONDITION(nullptr == _using, "_using should be set exactly once.", _ctx);
        _using = make_shared<query::ColumnRef>("", "", strings[0]);
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_tableRef != nullptr, "TableRef was not set.", _ctx);
        ASSERT_EXECUTION_CONDITION(_using != nullptr, "`using` was not set.", _ctx);
        auto joinSpec = make_shared<query::JoinSpec>(_using);
        // todo where does type get defined?
        auto joinRef = make_shared<query::JoinRef>(_tableRef, query::JoinRef::DEFAULT, false, joinSpec);
        lockedParent()->handleInnerJoin(joinRef);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ColumnRef> _using;
    shared_ptr<query::TableRef> _tableRef;
};


class SelectSpecAdapter :
        public AdapterT<SelectSpecCBH, QSMySqlParser::SelectSpecContext> {
public:
    using AdapterT::AdapterT;

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_ctx->ALL() == nullptr,
                "ALL is not supported.", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->DISTINCTROW() == nullptr,
                "DISTINCTROW is not supported.", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->HIGH_PRIORITY() == nullptr,
                "HIGH_PRIORITY", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->STRAIGHT_JOIN() == nullptr,
                "STRAIGHT_JOIN is not supported.", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->SQL_SMALL_RESULT() == nullptr,
                "SQL_SMALL_RESULT is not supported.", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->SQL_BIG_RESULT() == nullptr,
                "SQL_BIG_RESULT is not supported.", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->SQL_BUFFER_RESULT() == nullptr,
                "SQL_BUFFER_RESULT is not supported.", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->SQL_CACHE() == nullptr,
                "SQL_CACHE", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->SQL_NO_CACHE() == nullptr,
                "SQL_NO_CACHE is not supported.", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->SQL_CALC_FOUND_ROWS() == nullptr,
                "SQL_CALC_FOUND_ROWS is not supported.", _ctx);

        lockedParent()->handleSelectSpec(_ctx->DISTINCT() != nullptr);
    }

    string name() const override { return getTypeName(this); }
};



// handles `functionCall (AS? uid)?` e.g. "COUNT AS object_count"
class SelectFunctionElementAdapter :
        public AdapterT<SelectFunctionElementCBH, QSMySqlParser::SelectFunctionElementContext>,
        public AggregateFunctionCallCBH,
        public UidCBH,
        public UdfFunctionCallCBH,
        public ScalarFunctionCallCBH {
public:
    using AdapterT::AdapterT;

    void handleUid(string const & string) override {
        // Uid is expected to be the aliasName in `functionCall AS aliasName`
        ASSERT_EXECUTION_CONDITION(_asName.empty(), "Second call to handleUid.", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->AS() != nullptr, "Call to handleUid but AS is null.", _ctx);
        _asName = string;
    }

    void handleAggregateFunctionCall(shared_ptr<query::ValueFactor> const & aggValueFactor) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _functionValueFactor, "should only be called once.",
                _ctx);
        _functionValueFactor = aggValueFactor;
    }

    void handleUdfFunctionCall(shared_ptr<query::FuncExpr> const & funcExpr) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _functionValueFactor, "should only be set once.",
                _ctx);
        _functionValueFactor = query::ValueFactor::newFuncFactor(funcExpr);
    }

    void handleScalarFunctionCall(shared_ptr<query::FuncExpr> const & funcExpr) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _functionValueFactor, "should only be set once.",
                _ctx);
        _functionValueFactor = query::ValueFactor::newFuncFactor(funcExpr);
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(nullptr != _functionValueFactor,
                "function value factor not populated.", _ctx);
        auto valueExpr = std::make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, _functionValueFactor);
        valueExpr->setAlias(_asName);
        lockedParent()->handleSelectFunctionElement(valueExpr);
    }

    string name() const override { return getTypeName(this); }

private:
    string _asName;
    shared_ptr<query::ValueFactor> _functionValueFactor;
};


class GroupByItemAdapter :
        public AdapterT<GroupByItemCBH, QSMySqlParser::GroupByItemContext>,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(shared_ptr<query::BoolFactor> const & boolFactor) override {
        ASSERT_EXECUTION_CONDITION(false, "Unexpected GroupByItemAdapter boolFactor callback.", _ctx);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr> const & valueExpr) override {
        _valueExpr = valueExpr;
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_valueExpr != nullptr, "GroupByItemAdapter not populated.", _ctx);
        lockedParent()->handleGroupByItem(_valueExpr);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueExpr> _valueExpr;
};


class LimitClauseAdapter :
        public AdapterT<LimitClauseCBH, QSMySqlParser::LimitClauseContext> {
public:
    using AdapterT::AdapterT;

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_ctx->limit != nullptr,
                "Could not get a decimalLiteral context to read limit.", _ctx);
        lockedParent()->handleLimitClause(atoi(_ctx->limit->getText().c_str()));
    }

    string name() const override { return getTypeName(this); }
};



class SimpleIdAdapter :
        public AdapterT<SimpleIdCBH, QSMySqlParser::SimpleIdContext>,
        public FunctionNameBaseCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionNameBase(string const & name) override {
        // for all callbacks to SimpleIdAdapter are dropped and the value is fetched from the text value
        // of the context on exit.
    }

    void onExit() override {
        lockedParent()->handleSimpleId(_ctx->getText());
    }

    string name() const override { return getTypeName(this); }
};


class DottedIdAdapter :
        public AdapterT<DottedIdCBH, QSMySqlParser::DottedIdContext> {
public:
    using AdapterT::AdapterT;

    void onExit() override {
        // currently the on kind of callback we receive here seems to be the `: DOT_ID` form, which is defined
        // as `'.' ID_LITERAL;`. This means that we have to extract the value from the DOT_ID; we will not be
        //called by a child with the string portion, the ID_LITERAL.
        // I suppose at some point the antlr4 evaulation will try to use the `'.' uid` form, at which point
        // this will have to become a UidCBH. At that point some checking shoudl be applied; we would not
        // expect both forms to be used in one instantiation of this adapter. In the meantime, we only attempt
        // to extract the ID_LITERAL and call our parent with that.
        string txt = _ctx->getText();
        ASSERT_EXECUTION_CONDITION(txt.find('.') == 0, "DottedId text is expected to start with a dot", _ctx);
        txt.erase(0, 1);
        lockedParent()->handleDottedId(txt);
    }

    string name() const override { return getTypeName(this); }
};


class SelectColumnElementAdapter :
        public AdapterT<SelectColumnElementCBH, QSMySqlParser::SelectColumnElementContext>,
        public FullColumnNameCBH,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    void handleFullColumnName(shared_ptr<query::ValueFactor> const & valueFactor) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _valueFactor,
                "handleFullColumnName should be called once.", _ctx);
        _valueFactor = valueFactor;
    }

    void handleUid(string const & string) override {
        ASSERT_EXECUTION_CONDITION(_alias.empty(), "handleUid should be called once.", _ctx);
        _alias = string;
    }

    void onExit() override {
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, _valueFactor);
        valueExpr->setAlias(_alias);
        lockedParent()->handleColumnElement(valueExpr);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueFactor> _valueFactor;
    string _alias;
};


class UidAdapter :
        public AdapterT<UidCBH, QSMySqlParser::UidContext>,
        public SimpleIdCBH {
public:
    using AdapterT::AdapterT;

    void handleSimpleId(string const & val) {
        _val = val;
    }

    void onExit() override {
        // Fetching the string from a Uid shortcuts a large part of the syntax tree defined under Uid
        // (see QSMySqlParser.g4). If Adapters for any nodes in the tree below Uid are implemented then
        // it will have to be handled and this shortcut may not be taken.
        if (_val.empty()) {
            ASSERT_EXECUTION_CONDITION(_ctx->REVERSE_QUOTE_ID() != nullptr ||
                    _ctx->CHARSET_REVERSE_QOUTE_STRING() != nullptr,
                   "If value is not set by callback then one of the terminal nodes should be populated.",
                    _ctx);
            _val = _ctx->getText();
        }
        lockedParent()->handleUid(_val);
    }

    string name() const override { return getTypeName(this); }

private:
    string _val;
};


class ConstantAdapter :
        public AdapterT<ConstantCBH, QSMySqlParser::ConstantContext> {
public:
    using AdapterT::AdapterT;

    void onExit() override {
        lockedParent()->handleConstant(_ctx->getText());
    }

    string name() const override { return getTypeName(this); }
};


class UidListAdapter :
        public AdapterT<UidListCBH, QSMySqlParser::UidListContext>,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    void handleUid(string const & string) override {
        _strings.push_back(string);
    }

    void onExit() override {
        if (false == _strings.empty()) {
            lockedParent()->handleUidList(_strings);
        }
    }

    string name() const override { return getTypeName(this); }

private:
    vector<string> _strings;
};


class ExpressionsAdapter :
        public AdapterT<ExpressionsCBH, QSMySqlParser::ExpressionsContext>,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(shared_ptr<query::BoolFactor> const & boolFactor) override {
        ASSERT_EXECUTION_CONDITION(false, "Unhandled PredicateExpression with BoolFactor.", _ctx);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr> const & valueExpr) override {
        _expressions.push_back(valueExpr);
    }

    void onExit() {
        lockedParent()->handleExpressions(_expressions);
    }

    string name() const override { return getTypeName(this); }

private:
    vector<shared_ptr<query::ValueExpr>> _expressions;
};


class ConstantsAdapter :
        public AdapterT<ConstantsCBH, QSMySqlParser::ConstantsContext>,
        public ConstantCBH {
public:
    using AdapterT::AdapterT;

    void handleConstant(string const & val) override {
        _values.push_back(val);
    }

    void onExit() override {
        lockedParent()->handleConstants(_values);
    }

    string name() const override { return getTypeName(this); }

private:
    vector<string> _values;
};


class AggregateFunctionCallAdapter :
        public AdapterT<AggregateFunctionCallCBH, QSMySqlParser::AggregateFunctionCallContext>,
        public AggregateWindowedFunctionCBH {
public:
    using AdapterT::AdapterT;

    void handleAggregateWindowedFunction(shared_ptr<query::ValueFactor> const & aggValueFactor) override {
        lockedParent()->handleAggregateFunctionCall(aggValueFactor);
    }

    void onExit() override {}

    string name() const override { return getTypeName(this); }
};


class ScalarFunctionCallAdapter :
        public AdapterT<ScalarFunctionCallCBH, QSMySqlParser::ScalarFunctionCallContext>,
        public ScalarFunctionNameCBH,
        public FunctionArgsCBH {
public:
    using AdapterT::AdapterT;

    void handleScalarFunctionName(string const & name) override {
        ASSERT_EXECUTION_CONDITION(_name.empty(), "name should be set once.", _ctx);
        _name = name;
    }

    void handleFunctionArgs(vector<shared_ptr<query::ValueExpr>> const & valueExprs) override {
        ASSERT_EXECUTION_CONDITION(_valueExprs.empty(), "FunctionArgs should be set once.", _ctx);
        _valueExprs = valueExprs;
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_valueExprs.empty() == false && _name.empty() == false,
                "valueExprs or name is not populated.", _ctx);
        auto funcExpr = query::FuncExpr::newWithArgs(_name, _valueExprs);
        lockedParent()->handleScalarFunctionCall(funcExpr);
    }

    string name() const override { return getTypeName(this); }

private:
    vector<shared_ptr<query::ValueExpr>> _valueExprs;
    string _name;
};


class UdfFunctionCallAdapter :
        public AdapterT<UdfFunctionCallCBH, QSMySqlParser::UdfFunctionCallContext>,
        public FullIdCBH,
        public FunctionArgsCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionArgs(vector<shared_ptr<query::ValueExpr>> const & valueExprs) override {
        // This is only expected to be called once.
        // Of course the valueExpr may have more than one valueFactor.
        ASSERT_EXECUTION_CONDITION(_args.empty(), "Args already assigned.", _ctx);
        _args = valueExprs;
    }

    // FullIdCBH
    void handleFullId(vector<string> const & uidlist) override {
        ASSERT_EXECUTION_CONDITION(_functionName.empty(), "Function name already assigned.", _ctx);
        ASSERT_EXECUTION_CONDITION(uidlist.size() == 1, "Function name invalid", _ctx);
        _functionName = uidlist.at(0);
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(!_functionName.empty(), "Function name unpopulated", _ctx);
        ASSERT_EXECUTION_CONDITION(!_args.empty(), "Function arguments unpopulated", _ctx);
        auto funcExpr = query::FuncExpr::newWithArgs(_functionName, _args);
        lockedParent()->handleUdfFunctionCall(funcExpr);
    }

    string name() const override { return getTypeName(this); }

private:
    vector<shared_ptr<query::ValueExpr>> _args;
    string _functionName;
};


class AggregateWindowedFunctionAdapter :
        public AdapterT<AggregateWindowedFunctionCBH, QSMySqlParser::AggregateWindowedFunctionContext>,
        public FunctionArgCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionArg(shared_ptr<query::ValueFactor> const & valueFactor) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _valueFactor,
                "currently ValueFactor can only be set once.", _ctx);
        _valueFactor = valueFactor;
    }

    void onExit() override {
        shared_ptr<query::FuncExpr> funcExpr;
        if (_ctx->COUNT() && _ctx->starArg) {
            string table;
            auto starFactor = query::ValueFactor::newStarFactor(table);
            auto starParExpr = std::make_shared<query::ValueExpr>();
            ValueExprFactory::addValueFactor(starParExpr, starFactor);
            funcExpr = query::FuncExpr::newArg1(_ctx->COUNT()->getText(), starParExpr);
        } else if (_ctx->AVG() || _ctx->MAX() || _ctx->MIN() ) {
            auto param = std::make_shared<query::ValueExpr>();
            ASSERT_EXECUTION_CONDITION(nullptr != _valueFactor, "ValueFactor must be populated.", _ctx);
            ValueExprFactory::addValueFactor(param, _valueFactor);
            antlr4::tree::TerminalNode * terminalNode;
            if (_ctx->AVG()) {
                terminalNode = _ctx->AVG();
            } else if (_ctx->MAX()) {
                terminalNode = _ctx->MAX();
            } else if (_ctx->MIN()) {
                terminalNode = _ctx->MIN();
            } else {
                ASSERT_EXECUTION_CONDITION(false, "Unhandled function type", _ctx);
            }
            funcExpr = query::FuncExpr::newArg1(terminalNode->getText(), param);
        } else {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled function type", _ctx);
        }
        auto aggValueFactor = query::ValueFactor::newAggFactor(funcExpr);
        lockedParent()->handleAggregateWindowedFunction(aggValueFactor);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueFactor> _valueFactor;
};


class ScalarFunctionNameAdapter :
        public AdapterT<ScalarFunctionNameCBH, QSMySqlParser::ScalarFunctionNameContext>,
        public FunctionNameBaseCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionNameBase(string const & name) override {
        _name = name;
    }

    void onExit() override {
        string str;
        if (_name.empty()) {
            _name = _ctx->getText();
        }
        ASSERT_EXECUTION_CONDITION(_name.empty() == false,
                "not populated; expected a callback from functionNameBase", _ctx);
        lockedParent()->handleScalarFunctionName(_name);
    }

    string name() const override { return getTypeName(this); }

private:
    string _name;
};


class FunctionArgsAdapter :
        public AdapterT<FunctionArgsCBH, QSMySqlParser::FunctionArgsContext>,
        public ConstantCBH,
        public FullColumnNameCBH,
        public ScalarFunctionCallCBH{
public:
    using AdapterT::AdapterT;

    // ConstantCBH
    void handleConstant(string const & val) override {
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, query::ValueFactor::newConstFactor(val));
        _args.push_back(valueExpr);
    }

    void handleFullColumnName(shared_ptr<query::ValueFactor> const & columnName) override {
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, columnName);
        _args.push_back(valueExpr);
    }

    void handleScalarFunctionCall(shared_ptr<query::FuncExpr> const & funcExpr) {
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addFuncExpr(valueExpr, funcExpr);
        _args.push_back(valueExpr);
    }

    void onExit() override {
        lockedParent()->handleFunctionArgs(_args);
    }

    string name() const override { return getTypeName(this); }

private:
    vector<shared_ptr<query::ValueExpr>> _args;
};


class FunctionArgAdapter :
        public AdapterT<FunctionArgCBH, QSMySqlParser::FunctionArgContext>,
        public FullColumnNameCBH {
public:
    using AdapterT::AdapterT;

    void handleFullColumnName(shared_ptr<query::ValueFactor> const & columnName) override {
        ASSERT_EXECUTION_CONDITION(nullptr == _valueFactor,
                "Expected exactly one callback; valueFactor should be NULL.", _ctx);
        _valueFactor = columnName;
    }

    void onExit() override {
        lockedParent()->handleFunctionArg(_valueFactor);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueFactor> _valueFactor;
};


class LogicalExpressionAdapter :
        public AdapterT<LogicalExpressionCBH, QSMySqlParser::LogicalExpressionContext>,
        public LogicalExpressionCBH,
        public PredicateExpressionCBH,
        public LogicalOperatorCBH,
        public QservFunctionSpecCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(shared_ptr<query::BoolFactor> const & boolFactor) override {
        _setNextTerm(boolFactor);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr> const & valueExpr) override {
        ASSERT_EXECUTION_CONDITION(false, "Unhandled PredicateExpression with ValueExpr.", _ctx);
    }

    void handleQservFunctionSpec(string const & functionName,
            vector<shared_ptr<query::ValueFactor>> const & args) override {
        // qserv query IR handles qserv restrictor functions differently than the and/or bool tree that
        // handles the rest of the where clause, pass the function straight up to the parent.
        lockedParent()->handleQservFunctionSpec(functionName, args);
    }

    void handleLogicalOperator(LogicalOperatorCBH::OperatorType operatorType) {
        switch (operatorType) {
        default:
            ASSERT_EXECUTION_CONDITION(false, "unhandled operator type", _ctx);
            break;

        case LogicalOperatorCBH::AND:
            // We capture the AndTerm into a base class so we can pass by reference into the setter.
            _setLogicalOperator(make_shared<query::AndTerm>());
            break;

        case LogicalOperatorCBH::OR:
            // We capture the OrTerm into a base class so we can pass by reference into the setter.
            _setLogicalOperator(make_shared<query::OrTerm>());
            break;
        }
    }

    virtual void handleLogicalExpression(shared_ptr<query::LogicalTerm> const & logicalTerm,
            antlr4::ParserRuleContext* childCtx) {
        if (_logicalOperator != nullptr && _logicalOperator->merge(*logicalTerm)) {
            return;
        }
        _setNextTerm(logicalTerm);
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_logicalOperator != nullptr, "logicalOperator is not set; " << *this, _ctx);
        // Since this is a logical expression e.g. `a AND b` (per the grammar) and `a` or `b` may also be a
        // logical expression, we try to merge each term, e.g. if this is an AND (query::AndTerm) and the
        // BoolTerm in the terms list is also an AND term they can be merged.
        for (auto term : _terms) {
            if (false == _logicalOperator->merge(*term)) {
                _logicalOperator->addBoolTerm(term);
            }
        }
        lockedParent()->handleLogicalExpression(_logicalOperator, _ctx);
    }

    string name() const override { return getTypeName(this); }

private:
    void _setLogicalOperator(shared_ptr<query::LogicalTerm> const & logicalTerm) {
        ASSERT_EXECUTION_CONDITION(nullptr == _logicalOperator,
                "logical operator must be set only once. existing:" << *this <<
                ", new:" << logicalTerm, _ctx);
        _logicalOperator = logicalTerm;
    }

    void _setNextTerm(shared_ptr<query::BoolTerm> term) {
        _terms.push_back(term);
    }

    friend ostream& operator<<(ostream& os, const LogicalExpressionAdapter& logicaAndlExpressionAdapter);

    // a qserv restrictor fucntion can be the left side of a predicate (currently it can only be the left
    // side; that is to say, it can only be the first term in the WHERE clause. If `handleQservFunctionSpec`
    // is called and _leftTerm is null (as well as _rightTerm and _logicalOperator, then _leftHandled is set
    // to true to indicate that the left term has been handled. This allows onExit to put only one term into
    // the logicalOperator and know that it was ok (the qserv IR accepts an AndTerm with only one factor).
    // This mechanism does not fully proect against qserv restrictors that may be the left side of a
    // subsequent logical expression. TBD if that's really an issue.
    vector<shared_ptr<query::BoolTerm>> _terms;
    shared_ptr<query::LogicalTerm> _logicalOperator;
};


ostream& operator<<(ostream& os, const LogicalExpressionAdapter& logicalExpressionAdapter) {
    os << "LogicalExpressionAdapter(";
    os << "terms:" << util::printable(logicalExpressionAdapter._terms);
    return os;
}


class InPredicateAdapter :
        public AdapterT<InPredicateCBH, QSMySqlParser::InPredicateContext>,
        public ExpressionAtomPredicateCBH,
        public ExpressionsCBH {
public:
    using AdapterT::AdapterT;

    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr> const & valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        ASSERT_EXECUTION_CONDITION(_ctx->predicate() == childCtx, "callback from unexpected element.", _ctx);
        ASSERT_EXECUTION_CONDITION(nullptr == _predicate, "Predicate should be set exactly once.", _ctx);
        _predicate = valueExpr;
    }

    void handleExpressionAtomPredicate(shared_ptr<query::BoolFactorTerm> const & boolFactorTerm,
            antlr4::ParserRuleContext* childCtx) override {
        ASSERT_EXECUTION_CONDITION(false, "unhandled ExpressionAtomPredicate BoolFactor callback.", _ctx);
    }

    void handleExpressions(vector<shared_ptr<query::ValueExpr>> const& valueExprs) override {
        ASSERT_EXECUTION_CONDITION(_expressions.empty(), "expressions should be set exactly once.", _ctx);
        _expressions = valueExprs;
    }

    void onExit() {
        ASSERT_EXECUTION_CONDITION(false == _expressions.empty() && _predicate != nullptr,
                "InPredicateAdapter was not fully populated:" << *this, _ctx);
        auto inPredicate = std::make_shared<query::InPredicate>();
        inPredicate->value = _predicate;
        inPredicate->cands = _expressions;
        lockedParent()->handleInPredicate(inPredicate);
    }

    friend ostream& operator<<(ostream& os, const InPredicateAdapter& inPredicateAdapter) {
        os << "InPredicateAdapter(";
        os << "predicate:" << inPredicateAdapter._predicate;
        os << ", expressions:" << util::printable(inPredicateAdapter._expressions);
        return os;
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueExpr> _predicate;
    vector<shared_ptr<query::ValueExpr>> _expressions;
};


class BetweenPredicateAdapter :
        public AdapterT<BetweenPredicateCBH, QSMySqlParser::BetweenPredicateContext>,
        public ExpressionAtomPredicateCBH {
public:
    using AdapterT::AdapterT;

    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr> const & valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        if (childCtx == _ctx->val) {
            ASSERT_EXECUTION_CONDITION(nullptr == _val, "val should be set exactly once.", _ctx);
            _val = valueExpr;
            return;
        }
        if (childCtx == _ctx->min) {
            ASSERT_EXECUTION_CONDITION(nullptr == _min, "min should be set exactly once.", _ctx);
            _min = valueExpr;
            return;
        }
        if (childCtx == _ctx->max) {
            ASSERT_EXECUTION_CONDITION(nullptr == _max, "max should be set exactly once.", _ctx);
            _max = valueExpr;
            return;
        }
    }

    void handleExpressionAtomPredicate(shared_ptr<query::BoolFactorTerm> const & boolFactorTerm,
            antlr4::ParserRuleContext* childCtx) override {
        ASSERT_EXECUTION_CONDITION(false, "unhandled ExpressionAtomPredicate BoolFactor callback.", _ctx);
    }

    void onExit() {
        ASSERT_EXECUTION_CONDITION(nullptr != _val && nullptr != _min && nullptr != _max,
                "val, min, and max must all be set.", _ctx);
        auto betweenPredicate = make_shared<query::BetweenPredicate>(_val, _min, _max);
        lockedParent()->handleBetweenPredicate(betweenPredicate);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueExpr> _val;
    shared_ptr<query::ValueExpr> _min;
    shared_ptr<query::ValueExpr> _max;
};


class LikePredicateAdapter :
        public AdapterT<LikePredicateCBH, QSMySqlParser::LikePredicateContext>,
        public ExpressionAtomPredicateCBH {
public:
    using AdapterT::AdapterT;

    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr> const & valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        if (nullptr == _valueExprA) {
            _valueExprA = valueExpr;
        } else if (nullptr == _valueExprB) {
            _valueExprB = valueExpr;
        } else {
            ASSERT_EXECUTION_CONDITION(false, "Expected to be called back exactly twice.", _ctx);
        }
    }

    void handleExpressionAtomPredicate(shared_ptr<query::BoolFactorTerm> const & boolFactorTerm,
            antlr4::ParserRuleContext* childCtx) override {
        ASSERT_EXECUTION_CONDITION(false,
                "Unhandled BoolFactorTerm callback.", _ctx);
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_valueExprA != nullptr && _valueExprB != nullptr,
                "LikePredicateAdapter was not fully populated.", _ctx);
        auto likePredicate = make_shared<query::LikePredicate>();
        likePredicate->value = _valueExprA;
        likePredicate->charValue = _valueExprB;
        lockedParent()->handleLikePredicate(likePredicate);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueExpr> _valueExprA;
    shared_ptr<query::ValueExpr> _valueExprB;
};


class UnaryExpressionAtomAdapter :
        public AdapterT<UnaryExpressionAtomCBH, QSMySqlParser::UnaryExpressionAtomContext>,
        public UnaryOperatorCBH,
        public ConstantExpressionAtomCBH {
public:
    using AdapterT::AdapterT;

    void handleUnaryOperator(string const & val) override {
        ASSERT_EXECUTION_CONDITION(_operatorPrefix.empty(),
                "Expected to set the unary operator only once.", _ctx);
        _operatorPrefix = val;
    }

    void handleConstantExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) {
        ASSERT_EXECUTION_CONDITION(nullptr == _valueFactor,
                "Expected to set the ValueFactor only once.", _ctx);
        _valueFactor = valueFactor;
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(false == _operatorPrefix.empty() && _valueFactor != nullptr,
                "Expected unary operator (" << _operatorPrefix << ") and ValueFactor(" << _valueFactor <<
                "to be populated", _ctx);
        ASSERT_EXECUTION_CONDITION(_valueFactor->getType() == query::ValueFactor::CONST,
                "Currently can only handle const val", _ctx);
            _valueFactor->setConstVal(_operatorPrefix + _valueFactor->getConstVal());
        lockedParent()->handleUnaryExpressionAtom(_valueFactor);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueFactor> _valueFactor;
    string _operatorPrefix;
};


class NestedExpressionAtomAdapter :
        public AdapterT<NestedExpressionAtomCBH, QSMySqlParser::NestedExpressionAtomContext>,
        public PredicateExpressionCBH,
        public LogicalExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(shared_ptr<query::BoolFactor> const & boolFactor) override {
        auto andTerm = make_shared<query::AndTerm>();
        andTerm->addBoolTerm(boolFactor);
        auto orTerm = make_shared<query::OrTerm>();
        orTerm->addBoolTerm(andTerm);
        auto boolTermFactor = make_shared<query::BoolTermFactor>(orTerm);
        _boolTermFactors.push_back(boolTermFactor);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr> const & valueExpr) override {
        ASSERT_EXECUTION_CONDITION(false, "Unhandled PredicateExpression with ValueExpr.", _ctx);
    }

    void handleLogicalExpression(shared_ptr<query::LogicalTerm> const & logicalTerm,
            antlr4::ParserRuleContext* childCtx) override {
        auto boolTermFactor = std::make_shared<query::BoolTermFactor>(logicalTerm);
        _boolTermFactors.push_back(boolTermFactor);
    }

    void handleQservFunctionSpec(string const & functionName,
            vector<shared_ptr<query::ValueFactor>> const & args) override {
        ASSERT_EXECUTION_CONDITION(false, "Qserv functions may not appear in nested contexts.", _ctx);
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_boolTermFactors.empty() == false,
                "NestedExpressionAtomAdapter not populated.", _ctx)
        lockedParent()->handleNestedExpressionAtom(make_shared<query::PassTerm>("("));
        for (auto&& boolTermFactor : _boolTermFactors) {
            lockedParent()->handleNestedExpressionAtom(boolTermFactor);
        }
        lockedParent()->handleNestedExpressionAtom(make_shared<query::PassTerm>(")"));
    }

    string name() const override { return getTypeName(this); }

private:
    vector<shared_ptr<query::BoolTermFactor>> _boolTermFactors;
};


class MathExpressionAtomAdapter :
        public AdapterT<MathExpressionAtomCBH, QSMySqlParser::MathExpressionAtomContext>,
        public MathOperatorCBH,
        public FunctionCallExpressionAtomCBH,
        public FullColumnNameExpressionAtomCBH,
        public ConstantExpressionAtomCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionCallExpressionAtom(shared_ptr<query::FuncExpr> const & funcExpr) override {
        ValueExprFactory::addFuncExpr(_getValueExpr(), funcExpr);
    }

    void handleMathOperator(MathOperatorCBH::OperatorType operatorType) override {
        switch (operatorType) {
        default:
            ASSERT_EXECUTION_CONDITION(false, "Unhandled operatorType.", _ctx);
            break;

        case MathOperatorCBH::SUBTRACT: {
            bool success = ValueExprFactory::addOp(_getValueExpr(), query::ValueExpr::MINUS);
            ASSERT_EXECUTION_CONDITION(success,
                    "Failed to add an operator to valueExpr:" << _getValueExpr(), _ctx);
            break;
        }

        case MathOperatorCBH::ADD: {
            bool success = ValueExprFactory::addOp(_getValueExpr(), query::ValueExpr::PLUS);
            ASSERT_EXECUTION_CONDITION(success,
                    "Failed to add an operator to valueExpr:" << _getValueExpr(), _ctx);
            break;
        }

        }
    }

    void HandleFullColumnNameExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) override {
        ValueExprFactory::addValueFactor(_getValueExpr(), valueFactor);
    }

    void handleConstantExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) override {
        ValueExprFactory::addValueFactor(_getValueExpr(), valueFactor);
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_valueExpr != nullptr, "valueExpr not populated.", _ctx);
        lockedParent()->handleMathExpressionAtomAdapter(_valueExpr);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::ValueExpr> const & _getValueExpr() {
        if (nullptr == _valueExpr) {
            _valueExpr = make_shared<query::ValueExpr>();
        }
        return _valueExpr;
    }

    shared_ptr<query::ValueExpr> _valueExpr;
};


class FunctionCallExpressionAtomAdapter :
        public AdapterT<FunctionCallExpressionAtomCBH, QSMySqlParser::FunctionCallExpressionAtomContext>,
        public UdfFunctionCallCBH,
        public ScalarFunctionCallCBH {
public:
    using AdapterT::AdapterT;

    void handleUdfFunctionCall(shared_ptr<query::FuncExpr> const & funcExpr) override {
        ASSERT_EXECUTION_CONDITION(_funcExpr == nullptr, "the funcExpr must be set only once.", _ctx)
        _funcExpr = funcExpr;
    }

    void handleScalarFunctionCall(shared_ptr<query::FuncExpr> const & funcExpr) override {
        ASSERT_EXECUTION_CONDITION(_funcExpr == nullptr, "the funcExpr must be set only once.", _ctx)
        _funcExpr = funcExpr;
    }

    // someday: the `AS uid` part should be handled by making this a UID CBH,
    // it will set the alias in the generated valueFactor

    void onExit() {
        lockedParent()->handleFunctionCallExpressionAtom(_funcExpr);
    }

    string name() const override { return getTypeName(this); }

private:
    shared_ptr<query::FuncExpr> _funcExpr;
};


class UnaryOperatorAdapter :
        public AdapterT<UnaryOperatorCBH, QSMySqlParser::UnaryOperatorContext> {
public:
    using AdapterT::AdapterT;


    void onExit() override {
        lockedParent()->handleUnaryOperator(_ctx->getText());
    }

    string name() const override { return getTypeName(this); }
};



class LogicalOperatorAdapter :
        public AdapterT<LogicalOperatorCBH, QSMySqlParser::LogicalOperatorContext> {
public:
    using AdapterT::AdapterT;

    void onExit() override {
        if (_ctx->AND() != nullptr) {
            lockedParent()->handleLogicalOperator(LogicalOperatorCBH::AND);
        } else if (_ctx->OR() != nullptr) {
            lockedParent()->handleLogicalOperator(LogicalOperatorCBH::OR);
        } else {
            ASSERT_EXECUTION_CONDITION(false, "unhandled logical operator", _ctx);
        }
    }

    string name() const override { return getTypeName(this); }
};


class MathOperatorAdapter :
        public AdapterT<MathOperatorCBH, QSMySqlParser::MathOperatorContext> {
public:
    using AdapterT::AdapterT;

    void onExit() override {
        if (_ctx->getText() == "-") {
            lockedParent()->handleMathOperator(MathOperatorCBH::SUBTRACT);
        } else if (_ctx->getText() == "+") {
            lockedParent()->handleMathOperator(MathOperatorCBH::ADD);
        } else {
            ASSERT_EXECUTION_CONDITION(false, "Unhanlded operator type:" << _ctx->getText(), _ctx);
        }
    }

    string name() const override { return getTypeName(this); }
};



class FunctionNameBaseAdapter :
        public AdapterT<FunctionNameBaseCBH, QSMySqlParser::FunctionNameBaseContext> {
public:
    using AdapterT::AdapterT;

    void onExit() override {
        lockedParent()->handleFunctionNameBase(_ctx->getText());
    }

    string name() const override { return getTypeName(this); }
};


/// QSMySqlListener impl


QSMySqlListener::QSMySqlListener(std::shared_ptr<ListenerDebugHelper> listenerDebugHelper)
: _listenerDebugHelper(listenerDebugHelper)
{}


shared_ptr<query::SelectStmt> QSMySqlListener::getSelectStatement() const {
    return _rootAdapter->getSelectStatement();
}


// Create and push an Adapter onto the context stack, using the current top of the stack as a callback handler
// for the new Adapter. Returns the new Adapter.
template<typename ParentCBH, typename ChildAdapter, typename Context>
shared_ptr<ChildAdapter> QSMySqlListener::pushAdapterStack(Context* ctx) {
    auto p = dynamic_pointer_cast<ParentCBH>(_adapterStack.back());
    ASSERT_EXECUTION_CONDITION(p != nullptr, "can't acquire expected Adapter `" <<
            getTypeName<ParentCBH>() <<
            "` from top of listenerStack.",
            ctx);
    auto childAdapter = make_shared<ChildAdapter>(p, ctx, this);
    _adapterStack.push_back(childAdapter);
    childAdapter->onEnter();
    return childAdapter;
}


template<typename ChildAdapter>
void QSMySqlListener::popAdapterStack(antlr4::ParserRuleContext* ctx) {
    shared_ptr<Adapter> adapterPtr = _adapterStack.back();
    adapterPtr->onExit();
    _adapterStack.pop_back();
    // capturing adapterPtr and casting it to the expected type is useful as a sanity check that the enter &
    // exit functions are called in the correct order (balanced). The dynamic cast is of course not free and
    // this code could be optionally disabled or removed entirely if the check is found to be unnecesary or
    // adds too much of a performance penalty.
    shared_ptr<ChildAdapter> derivedPtr = dynamic_pointer_cast<ChildAdapter>(adapterPtr);
    ASSERT_EXECUTION_CONDITION(derivedPtr != nullptr, "Top of listenerStack was not of expected type. " <<
        "Expected: " << getTypeName<ChildAdapter>() <<
        ", Actual: " << getTypeName(adapterPtr) <<
        ", Are there out of order or unhandled listener exits?",
        ctx);
}


string QSMySqlListener::adapterStackToString() const {
    string ret;
    for (auto&& adapter : _adapterStack) {
        ret += adapter->name() + ", ";
    }
    return ret;
}

// QSMySqlListener class methods


void QSMySqlListener::enterRoot(QSMySqlParser::RootContext* ctx) {
    ASSERT_EXECUTION_CONDITION(_adapterStack.empty(), "RootAdatper should be the first entry on the stack.",
            ctx);
    _rootAdapter = make_shared<RootAdapter>();
    _adapterStack.push_back(_rootAdapter);
    _rootAdapter->onEnter(ctx, this);
}


void QSMySqlListener::exitRoot(QSMySqlParser::RootContext* ctx) {
    popAdapterStack<RootAdapter>(ctx);
}


std::string QSMySqlListener::getStringTree() const {
    auto ldh = _listenerDebugHelper.lock();
    if (ldh != nullptr) {
        return ldh->getStringTree();
    }
    return "unexpected null listener debug helper.";
}

std::string QSMySqlListener::getTokens() const {
    auto ldh = _listenerDebugHelper.lock();
    if (ldh != nullptr) {
        return ldh->getTokens();
    }
    return "unexpected null listener debug helper.";
}

std::string QSMySqlListener::getStatementStr() const {
    auto ldh = _listenerDebugHelper.lock();
    if (ldh != nullptr) {
        return ldh->getStatementStr();
    }
    return "unexpected null listener debug helper.";
}


IGNORED(SqlStatements)
IGNORED(SqlStatement)
IGNORED(EmptyStatement)
IGNORED(DdlStatement)
ENTER_EXIT_PARENT(DmlStatement)
ENTER_EXIT_PARENT(SimpleSelect)
ENTER_EXIT_PARENT(QuerySpecification)
ENTER_EXIT_PARENT(SelectElements)
ENTER_EXIT_PARENT(SelectColumnElement)
ENTER_EXIT_PARENT(FromClause)
ENTER_EXIT_PARENT(TableSources)
ENTER_EXIT_PARENT(TableSourceBase)
ENTER_EXIT_PARENT(AtomTableItem)
ENTER_EXIT_PARENT(TableName)
ENTER_EXIT_PARENT(FullColumnName)
ENTER_EXIT_PARENT(FullId)
ENTER_EXIT_PARENT(Uid)
IGNORED(DecimalLiteral)
IGNORED(StringLiteral)
ENTER_EXIT_PARENT(PredicateExpression)
ENTER_EXIT_PARENT(ExpressionAtomPredicate)
ENTER_EXIT_PARENT(QservFunctionSpec)
ENTER_EXIT_PARENT(BinaryComparasionPredicate)
ENTER_EXIT_PARENT(ConstantExpressionAtom)
ENTER_EXIT_PARENT(FullColumnNameExpressionAtom)
ENTER_EXIT_PARENT(ComparisonOperator)
UNHANDLED(TransactionStatement)
UNHANDLED(ReplicationStatement)
UNHANDLED(PreparedStatement)
UNHANDLED(CompoundStatement)
UNHANDLED(AdministrationStatement)
UNHANDLED(UtilityStatement)
UNHANDLED(CreateDatabase)
UNHANDLED(CreateEvent)
UNHANDLED(CreateIndex)
UNHANDLED(CreateLogfileGroup)
UNHANDLED(CreateProcedure)
UNHANDLED(CreateFunction)
UNHANDLED(CreateServer)
UNHANDLED(CopyCreateTable)
UNHANDLED(QueryCreateTable)
UNHANDLED(ColumnCreateTable)
UNHANDLED(CreateTablespaceInnodb)
UNHANDLED(CreateTablespaceNdb)
UNHANDLED(CreateTrigger)
UNHANDLED(CreateView)
UNHANDLED(CreateDatabaseOption)
UNHANDLED(OwnerStatement)
UNHANDLED(PreciseSchedule)
UNHANDLED(IntervalSchedule)
UNHANDLED(TimestampValue)
UNHANDLED(IntervalExpr)
UNHANDLED(IntervalType)
UNHANDLED(EnableType)
UNHANDLED(IndexType)
UNHANDLED(IndexOption)
UNHANDLED(ProcedureParameter)
UNHANDLED(FunctionParameter)
UNHANDLED(RoutineComment)
UNHANDLED(RoutineLanguage)
UNHANDLED(RoutineBehavior)
UNHANDLED(RoutineData)
UNHANDLED(RoutineSecurity)
UNHANDLED(ServerOption)
UNHANDLED(CreateDefinitions)
UNHANDLED(ColumnDeclaration)
UNHANDLED(ConstraintDeclaration)
UNHANDLED(IndexDeclaration)
UNHANDLED(ColumnDefinition)
UNHANDLED(NullColumnConstraint)
UNHANDLED(DefaultColumnConstraint)
UNHANDLED(AutoIncrementColumnConstraint)
UNHANDLED(PrimaryKeyColumnConstraint)
UNHANDLED(UniqueKeyColumnConstraint)
UNHANDLED(CommentColumnConstraint)
UNHANDLED(FormatColumnConstraint)
UNHANDLED(StorageColumnConstraint)
UNHANDLED(ReferenceColumnConstraint)
UNHANDLED(PrimaryKeyTableConstraint)
UNHANDLED(UniqueKeyTableConstraint)
UNHANDLED(ForeignKeyTableConstraint)
UNHANDLED(CheckTableConstraint)
UNHANDLED(ReferenceDefinition)
UNHANDLED(ReferenceAction)
UNHANDLED(ReferenceControlType)
UNHANDLED(SimpleIndexDeclaration)
UNHANDLED(SpecialIndexDeclaration)
UNHANDLED(TableOptionEngine)
UNHANDLED(TableOptionAutoIncrement)
UNHANDLED(TableOptionAverage)
UNHANDLED(TableOptionCharset)
UNHANDLED(TableOptionChecksum)
UNHANDLED(TableOptionCollate)
UNHANDLED(TableOptionComment)
UNHANDLED(TableOptionCompression)
UNHANDLED(TableOptionConnection)
UNHANDLED(TableOptionDataDirectory)
UNHANDLED(TableOptionDelay)
UNHANDLED(TableOptionEncryption)
UNHANDLED(TableOptionIndexDirectory)
UNHANDLED(TableOptionInsertMethod)
UNHANDLED(TableOptionKeyBlockSize)
UNHANDLED(TableOptionMaxRows)
UNHANDLED(TableOptionMinRows)
UNHANDLED(TableOptionPackKeys)
UNHANDLED(TableOptionPassword)
UNHANDLED(TableOptionRowFormat)
UNHANDLED(TableOptionRecalculation)
UNHANDLED(TableOptionPersistent)
UNHANDLED(TableOptionSamplePage)
UNHANDLED(TableOptionTablespace)
UNHANDLED(TableOptionUnion)
UNHANDLED(TablespaceStorage)
UNHANDLED(PartitionDefinitions)
UNHANDLED(PartitionFunctionHash)
UNHANDLED(PartitionFunctionKey)
UNHANDLED(PartitionFunctionRange)
UNHANDLED(PartitionFunctionList)
UNHANDLED(SubPartitionFunctionHash)
UNHANDLED(SubPartitionFunctionKey)
UNHANDLED(PartitionComparision)
UNHANDLED(PartitionListAtom)
UNHANDLED(PartitionListVector)
UNHANDLED(PartitionSimple)
UNHANDLED(PartitionDefinerAtom)
UNHANDLED(PartitionDefinerVector)
UNHANDLED(SubpartitionDefinition)
UNHANDLED(PartitionOptionEngine)
UNHANDLED(PartitionOptionComment)
UNHANDLED(PartitionOptionDataDirectory)
UNHANDLED(PartitionOptionIndexDirectory)
UNHANDLED(PartitionOptionMaxRows)
UNHANDLED(PartitionOptionMinRows)
UNHANDLED(PartitionOptionTablespace)
UNHANDLED(PartitionOptionNodeGroup)
UNHANDLED(AlterSimpleDatabase)
UNHANDLED(AlterUpgradeName)
UNHANDLED(AlterEvent)
UNHANDLED(AlterFunction)
UNHANDLED(AlterInstance)
UNHANDLED(AlterLogfileGroup)
UNHANDLED(AlterProcedure)
UNHANDLED(AlterServer)
UNHANDLED(AlterTable)
UNHANDLED(AlterTablespace)
UNHANDLED(AlterView)
UNHANDLED(AlterByTableOption)
UNHANDLED(AlterByAddColumn)
UNHANDLED(AlterByAddColumns)
UNHANDLED(AlterByAddIndex)
UNHANDLED(AlterByAddPrimaryKey)
UNHANDLED(AlterByAddUniqueKey)
UNHANDLED(AlterByAddSpecialIndex)
UNHANDLED(AlterByAddForeignKey)
UNHANDLED(AlterBySetAlgorithm)
UNHANDLED(AlterByChangeDefault)
UNHANDLED(AlterByChangeColumn)
UNHANDLED(AlterByLock)
UNHANDLED(AlterByModifyColumn)
UNHANDLED(AlterByDropColumn)
UNHANDLED(AlterByDropPrimaryKey)
UNHANDLED(AlterByDropIndex)
UNHANDLED(AlterByDropForeignKey)
UNHANDLED(AlterByDisableKeys)
UNHANDLED(AlterByEnableKeys)
UNHANDLED(AlterByRename)
UNHANDLED(AlterByOrder)
UNHANDLED(AlterByConvertCharset)
UNHANDLED(AlterByDefaultCharset)
UNHANDLED(AlterByDiscardTablespace)
UNHANDLED(AlterByImportTablespace)
UNHANDLED(AlterByForce)
UNHANDLED(AlterByValidate)
UNHANDLED(AlterByAddPartition)
UNHANDLED(AlterByDropPartition)
UNHANDLED(AlterByDiscardPartition)
UNHANDLED(AlterByImportPartition)
UNHANDLED(AlterByTruncatePartition)
UNHANDLED(AlterByCoalescePartition)
UNHANDLED(AlterByReorganizePartition)
UNHANDLED(AlterByExchangePartition)
UNHANDLED(AlterByAnalyzePartitiion)
UNHANDLED(AlterByCheckPartition)
UNHANDLED(AlterByOptimizePartition)
UNHANDLED(AlterByRebuildPartition)
UNHANDLED(AlterByRepairPartition)
UNHANDLED(AlterByRemovePartitioning)
UNHANDLED(AlterByUpgradePartitioning)
UNHANDLED(DropDatabase)
UNHANDLED(DropEvent)
UNHANDLED(DropIndex)
UNHANDLED(DropLogfileGroup)
UNHANDLED(DropProcedure)
UNHANDLED(DropFunction)
UNHANDLED(DropServer)
UNHANDLED(DropTable)
UNHANDLED(DropTablespace)
UNHANDLED(DropTrigger)
UNHANDLED(DropView)
UNHANDLED(RenameTable)
UNHANDLED(RenameTableClause)
UNHANDLED(TruncateTable)
UNHANDLED(CallStatement)
UNHANDLED(DeleteStatement)
UNHANDLED(DoStatement)
UNHANDLED(HandlerStatement)
UNHANDLED(InsertStatement)
UNHANDLED(LoadDataStatement)
UNHANDLED(LoadXmlStatement)
UNHANDLED(ReplaceStatement)
UNHANDLED(ParenthesisSelect)
UNHANDLED(UnionSelect)
UNHANDLED(UnionParenthesisSelect)
UNHANDLED(UpdateStatement)
UNHANDLED(InsertStatementValue)
UNHANDLED(UpdatedElement)
UNHANDLED(AssignmentField)
UNHANDLED(LockClause)
UNHANDLED(SingleDeleteStatement)
UNHANDLED(MultipleDeleteStatement)
UNHANDLED(HandlerOpenStatement)
UNHANDLED(HandlerReadIndexStatement)
UNHANDLED(HandlerReadStatement)
UNHANDLED(HandlerCloseStatement)
UNHANDLED(SingleUpdateStatement)
UNHANDLED(MultipleUpdateStatement)
ENTER_EXIT_PARENT(OrderByClause)
ENTER_EXIT_PARENT(OrderByExpression)
UNHANDLED(TableSourceNested)
UNHANDLED(SubqueryTableItem)
UNHANDLED(TableSourcesItem)
UNHANDLED(IndexHint)
UNHANDLED(IndexHintType)
ENTER_EXIT_PARENT(InnerJoin)
UNHANDLED(StraightJoin)
UNHANDLED(OuterJoin)
UNHANDLED(NaturalJoin)
UNHANDLED(QueryExpression)
UNHANDLED(QueryExpressionNointo)
UNHANDLED(QuerySpecificationNointo)
UNHANDLED(UnionParenthesis)
UNHANDLED(UnionStatement)
ENTER_EXIT_PARENT(SelectSpec)
UNHANDLED(SelectStarElement)
ENTER_EXIT_PARENT(SelectFunctionElement)
UNHANDLED(SelectExpressionElement)
UNHANDLED(SelectIntoVariables)
UNHANDLED(SelectIntoDumpFile)
UNHANDLED(SelectIntoTextFile)
UNHANDLED(SelectFieldsInto)
UNHANDLED(SelectLinesInto)
ENTER_EXIT_PARENT(GroupByItem)
ENTER_EXIT_PARENT(LimitClause)
UNHANDLED(StartTransaction)
UNHANDLED(BeginWork)
UNHANDLED(CommitWork)
UNHANDLED(RollbackWork)
UNHANDLED(SavepointStatement)
UNHANDLED(RollbackStatement)
UNHANDLED(ReleaseStatement)
UNHANDLED(LockTables)
UNHANDLED(UnlockTables)
UNHANDLED(SetAutocommitStatement)
UNHANDLED(SetTransactionStatement)
UNHANDLED(TransactionMode)
UNHANDLED(LockTableElement)
UNHANDLED(LockAction)
UNHANDLED(TransactionOption)
UNHANDLED(TransactionLevel)
UNHANDLED(ChangeMaster)
UNHANDLED(ChangeReplicationFilter)
UNHANDLED(PurgeBinaryLogs)
UNHANDLED(ResetMaster)
UNHANDLED(ResetSlave)
UNHANDLED(StartSlave)
UNHANDLED(StopSlave)
UNHANDLED(StartGroupReplication)
UNHANDLED(StopGroupReplication)
UNHANDLED(MasterStringOption)
UNHANDLED(MasterDecimalOption)
UNHANDLED(MasterBoolOption)
UNHANDLED(MasterRealOption)
UNHANDLED(MasterUidListOption)
UNHANDLED(StringMasterOption)
UNHANDLED(DecimalMasterOption)
UNHANDLED(BoolMasterOption)
UNHANDLED(ChannelOption)
UNHANDLED(DoDbReplication)
UNHANDLED(IgnoreDbReplication)
UNHANDLED(DoTableReplication)
UNHANDLED(IgnoreTableReplication)
UNHANDLED(WildDoTableReplication)
UNHANDLED(WildIgnoreTableReplication)
UNHANDLED(RewriteDbReplication)
UNHANDLED(TablePair)
UNHANDLED(ThreadType)
UNHANDLED(GtidsUntilOption)
UNHANDLED(MasterLogUntilOption)
UNHANDLED(RelayLogUntilOption)
UNHANDLED(SqlGapsUntilOption)
UNHANDLED(UserConnectionOption)
UNHANDLED(PasswordConnectionOption)
UNHANDLED(DefaultAuthConnectionOption)
UNHANDLED(PluginDirConnectionOption)
UNHANDLED(GtuidSet)
UNHANDLED(XaStartTransaction)
UNHANDLED(XaEndTransaction)
UNHANDLED(XaPrepareStatement)
UNHANDLED(XaCommitWork)
UNHANDLED(XaRollbackWork)
UNHANDLED(XaRecoverWork)
UNHANDLED(PrepareStatement)
UNHANDLED(ExecuteStatement)
UNHANDLED(DeallocatePrepare)
UNHANDLED(RoutineBody)
UNHANDLED(BlockStatement)
UNHANDLED(CaseStatement)
UNHANDLED(IfStatement)
UNHANDLED(IterateStatement)
UNHANDLED(LeaveStatement)
UNHANDLED(LoopStatement)
UNHANDLED(RepeatStatement)
UNHANDLED(ReturnStatement)
UNHANDLED(WhileStatement)
UNHANDLED(CloseCursor)
UNHANDLED(FetchCursor)
UNHANDLED(OpenCursor)
UNHANDLED(DeclareVariable)
UNHANDLED(DeclareCondition)
UNHANDLED(DeclareCursor)
UNHANDLED(DeclareHandler)
UNHANDLED(HandlerConditionCode)
UNHANDLED(HandlerConditionState)
UNHANDLED(HandlerConditionName)
UNHANDLED(HandlerConditionWarning)
UNHANDLED(HandlerConditionNotfound)
UNHANDLED(HandlerConditionException)
UNHANDLED(ProcedureSqlStatement)
UNHANDLED(CaseAlternative)
UNHANDLED(ElifAlternative)
UNHANDLED(AlterUserMysqlV56)
UNHANDLED(AlterUserMysqlV57)
UNHANDLED(CreateUserMysqlV56)
UNHANDLED(CreateUserMysqlV57)
UNHANDLED(DropUser)
UNHANDLED(GrantStatement)
UNHANDLED(GrantProxy)
UNHANDLED(RenameUser)
UNHANDLED(DetailRevoke)
UNHANDLED(ShortRevoke)
UNHANDLED(RevokeProxy)
UNHANDLED(SetPasswordStatement)
UNHANDLED(UserSpecification)
UNHANDLED(PasswordAuthOption)
UNHANDLED(StringAuthOption)
UNHANDLED(HashAuthOption)
UNHANDLED(SimpleAuthOption)
UNHANDLED(TlsOption)
UNHANDLED(UserResourceOption)
UNHANDLED(UserPasswordOption)
UNHANDLED(UserLockOption)
UNHANDLED(PrivelegeClause)
UNHANDLED(Privilege)
UNHANDLED(CurrentSchemaPriviLevel)
UNHANDLED(GlobalPrivLevel)
UNHANDLED(DefiniteSchemaPrivLevel)
UNHANDLED(DefiniteFullTablePrivLevel)
UNHANDLED(DefiniteTablePrivLevel)
UNHANDLED(RenameUserClause)
UNHANDLED(AnalyzeTable)
UNHANDLED(CheckTable)
UNHANDLED(ChecksumTable)
UNHANDLED(OptimizeTable)
UNHANDLED(RepairTable)
UNHANDLED(CheckTableOption)
UNHANDLED(CreateUdfunction)
UNHANDLED(InstallPlugin)
UNHANDLED(UninstallPlugin)
UNHANDLED(SetVariable)
UNHANDLED(SetCharset)
UNHANDLED(SetNames)
UNHANDLED(SetPassword)
UNHANDLED(SetTransaction)
UNHANDLED(SetAutocommit)
UNHANDLED(ShowMasterLogs)
UNHANDLED(ShowLogEvents)
UNHANDLED(ShowObjectFilter)
UNHANDLED(ShowColumns)
UNHANDLED(ShowCreateDb)
UNHANDLED(ShowCreateFullIdObject)
UNHANDLED(ShowCreateUser)
UNHANDLED(ShowEngine)
UNHANDLED(ShowGlobalInfo)
UNHANDLED(ShowErrors)
UNHANDLED(ShowCountErrors)
UNHANDLED(ShowSchemaFilter)
UNHANDLED(ShowRoutine)
UNHANDLED(ShowGrants)
UNHANDLED(ShowIndexes)
UNHANDLED(ShowOpenTables)
UNHANDLED(ShowProfile)
UNHANDLED(ShowSlaveStatus)
UNHANDLED(VariableClause)
UNHANDLED(ShowCommonEntity)
UNHANDLED(ShowFilter)
UNHANDLED(ShowGlobalInfoClause)
UNHANDLED(ShowSchemaEntity)
UNHANDLED(ShowProfileType)
UNHANDLED(BinlogStatement)
UNHANDLED(CacheIndexStatement)
UNHANDLED(FlushStatement)
UNHANDLED(KillStatement)
UNHANDLED(LoadIndexIntoCache)
UNHANDLED(ResetStatement)
UNHANDLED(ShutdownStatement)
UNHANDLED(TableIndexes)
UNHANDLED(SimpleFlushOption)
UNHANDLED(ChannelFlushOption)
UNHANDLED(TableFlushOption)
UNHANDLED(FlushTableOption)
UNHANDLED(LoadedTableIndexes)
UNHANDLED(SimpleDescribeStatement)
UNHANDLED(FullDescribeStatement)
UNHANDLED(HelpStatement)
UNHANDLED(UseStatement)
UNHANDLED(DescribeStatements)
UNHANDLED(DescribeConnection)
UNHANDLED(IndexColumnName)
UNHANDLED(UserName)
UNHANDLED(MysqlVariable)
UNHANDLED(CharsetName)
UNHANDLED(CollationName)
UNHANDLED(EngineName)
UNHANDLED(UuidSet)
UNHANDLED(Xid)
UNHANDLED(XuidStringId)
UNHANDLED(AuthPlugin)
ENTER_EXIT_PARENT(SimpleId)
ENTER_EXIT_PARENT(DottedId)
UNHANDLED(FileSizeLiteral)
UNHANDLED(BooleanLiteral)
UNHANDLED(HexadecimalLiteral)
UNHANDLED(NullNotnull)
ENTER_EXIT_PARENT(Constant)
UNHANDLED(StringDataType)
UNHANDLED(DimensionDataType)
UNHANDLED(SimpleDataType)
UNHANDLED(CollectionDataType)
UNHANDLED(SpatialDataType)
UNHANDLED(ConvertedDataType)
UNHANDLED(LengthOneDimension)
UNHANDLED(LengthTwoDimension)
UNHANDLED(LengthTwoOptionalDimension)
ENTER_EXIT_PARENT(UidList)
UNHANDLED(Tables)
UNHANDLED(IndexColumnNames)
ENTER_EXIT_PARENT(Expressions)
UNHANDLED(ExpressionsWithDefaults)
ENTER_EXIT_PARENT(Constants)
UNHANDLED(SimpleStrings)
UNHANDLED(UserVariables)
UNHANDLED(DefaultValue)
UNHANDLED(ExpressionOrDefault)
UNHANDLED(IfExists)
UNHANDLED(IfNotExists)
UNHANDLED(SpecificFunctionCall)
ENTER_EXIT_PARENT(AggregateFunctionCall)
ENTER_EXIT_PARENT(ScalarFunctionCall)
ENTER_EXIT_PARENT(UdfFunctionCall)
UNHANDLED(PasswordFunctionCall)
UNHANDLED(SimpleFunctionCall)
UNHANDLED(DataTypeFunctionCall)
UNHANDLED(ValuesFunctionCall)
UNHANDLED(CaseFunctionCall)
UNHANDLED(CharFunctionCall)
UNHANDLED(PositionFunctionCall)
UNHANDLED(SubstrFunctionCall)
UNHANDLED(TrimFunctionCall)
UNHANDLED(WeightFunctionCall)
UNHANDLED(ExtractFunctionCall)
UNHANDLED(GetFormatFunctionCall)
UNHANDLED(CaseFuncAlternative)
UNHANDLED(LevelWeightList)
UNHANDLED(LevelWeightRange)
UNHANDLED(LevelInWeightListElement)
ENTER_EXIT_PARENT(AggregateWindowedFunction)
ENTER_EXIT_PARENT(ScalarFunctionName)
UNHANDLED(PasswordFunctionClause)
ENTER_EXIT_PARENT(FunctionArgs)
ENTER_EXIT_PARENT(FunctionArg)
UNHANDLED(IsExpression)
UNHANDLED(NotExpression)
IGNORED(QservFunctionSpecExpression)
ENTER_EXIT_PARENT(LogicalExpression)
UNHANDLED(SoundsLikePredicate)
ENTER_EXIT_PARENT(InPredicate)
UNHANDLED(SubqueryComparasionPredicate)
ENTER_EXIT_PARENT(BetweenPredicate)
UNHANDLED(IsNullPredicate)
ENTER_EXIT_PARENT(LikePredicate)
UNHANDLED(RegexpPredicate)
ENTER_EXIT_PARENT(UnaryExpressionAtom)
UNHANDLED(CollateExpressionAtom)
UNHANDLED(SubqueryExpessionAtom)
UNHANDLED(MysqlVariableExpressionAtom)
ENTER_EXIT_PARENT(NestedExpressionAtom)
UNHANDLED(NestedRowExpressionAtom)
ENTER_EXIT_PARENT(MathExpressionAtom)
UNHANDLED(IntervalExpressionAtom)
UNHANDLED(ExistsExpessionAtom)
ENTER_EXIT_PARENT(FunctionCallExpressionAtom)
UNHANDLED(BinaryExpressionAtom)
UNHANDLED(BitExpressionAtom)
ENTER_EXIT_PARENT(UnaryOperator)
ENTER_EXIT_PARENT(LogicalOperator)
UNHANDLED(BitOperator)
ENTER_EXIT_PARENT(MathOperator)
UNHANDLED(CharsetNameBase)
UNHANDLED(TransactionLevelBase)
UNHANDLED(PrivilegesBase)
UNHANDLED(IntervalTypeBase)
UNHANDLED(DataTypeBase)
IGNORED_WARN(KeywordsCanBeId, "Keyword reused as ID") // todo emit a warning?
ENTER_EXIT_PARENT(FunctionNameBase)

}}} // namespace lsst::qserv::parser
