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

#include "parser/SelectListFactory.h"
#include "parser/ValueExprFactory.h"
#include "parser/ValueFactorFactory.h"
#include "parser/WhereFactory.h"
#include "query/BoolTerm.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
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
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " " << ctx->getText()); \
    pushAdapterStack<NAME##CBH, NAME##Adapter>(ctx); \
} \
\
void QSMySqlListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) { \
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__); \
    popAdapterStack<NAME##Adapter>(ctx); \
} \


#define UNHANDLED(NAME) \
void QSMySqlListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) { \
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " is UNHANDLED " << ctx->getText()); \
    throw QSMySqlListener::adapter_order_error(string(__FUNCTION__) + string(" not supported.")); \
} \
\
void QSMySqlListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {}\


#define IGNORED(NAME) \
void QSMySqlListener::enter##NAME(QSMySqlParser::NAME##Context* ctx) { \
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " is IGNORED"); \
} \
\
void QSMySqlListener::exit##NAME(QSMySqlParser::NAME##Context* ctx) {\
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " is IGNORED"); \
} \


#define ASSERT_EXECUTION_CONDITION(CONDITION, MESSAGE, CTX) \
if (false == (CONDITION)) { \
    { \
        ostringstream msg; \
        msg << getTypeName(this) << "::" << __FUNCTION__; \
        msg << " messsage:\"" << MESSAGE << "\""; \
        msg << ", in or around query segment:" << getQueryString(CTX); \
        throw QSMySqlListener::adapter_execution_error(msg.str()); \
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
    virtual void handleDmlStatement(shared_ptr<query::SelectStmt>& selectStatement) = 0;
};


class SimpleSelectCBH : public BaseCBH {
public:
    virtual void handleSelectStatement(shared_ptr<query::SelectStmt>& selectStatement) = 0;
};


class QuerySpecificationCBH : public BaseCBH {
public:
    virtual void handleQuerySpecification(shared_ptr<query::SelectList>& selectList,
                                          shared_ptr<query::FromList>& fromList,
                                          shared_ptr<query::WhereClause>& whereClause) = 0;
};


class SelectElementsCBH : public BaseCBH {
public:
    virtual void handleSelectList(shared_ptr<query::SelectList>& selectList) = 0;
};


class FullColumnNameCBH : public BaseCBH {
public:
    virtual void handleFullColumnName(shared_ptr<query::ValueFactor>& valueFactor) = 0;
};


class TableNameCBH : public BaseCBH {
public:
    virtual void handleTableName(const string& string) = 0;
};


class FromClauseCBH : public BaseCBH {
public:
    virtual void handleFromClause(shared_ptr<query::FromList>& fromList,
                                  shared_ptr<query::WhereClause>& whereClause) = 0;
};


class TableSourcesCBH : public BaseCBH {
public:
    virtual void handleTableSources(query::TableRefListPtr tableRefList) = 0;
};

class TableSourceBaseCBH : public BaseCBH {
public:
    virtual void handleTableSource(shared_ptr<query::TableRef>& tableRef) = 0;
};


class AtomTableItemCBH : public BaseCBH {
public:
    virtual void handleAtomTableItem(shared_ptr<query::TableRef>& tableRef) = 0;
};


class UidCBH : public BaseCBH {
public:
    virtual void handleUidString(const string& string) = 0;
};


class FullIdCBH : public BaseCBH {
public:
    virtual void handleFullId(const string& string) = 0;
};


class ConstantExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleConstantExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) = 0;
};


class ExpressionAtomPredicateCBH : public BaseCBH {
public:
    virtual void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr>& valueExpr,
            antlr4::ParserRuleContext* childCtx) = 0;
};


class QservFunctionSpecCBH : public BaseCBH {
public:
    virtual void handleQservFunctionSpec(const string& functionName,
            const vector<shared_ptr<query::ValueFactor>>& args) = 0;
};


class ComparisonOperatorCBH : public BaseCBH {
public:
    virtual void handleComparisonOperator(const string& text) = 0;
};


class SelectFunctionElementCBH: public BaseCBH {
public:
    virtual void handleSelectFunctionElement(shared_ptr<query::ValueExpr> selectFunction) = 0;
};


class SelectColumnElementCBH : public BaseCBH {
public:
    virtual void handleColumnElement(shared_ptr<query::ValueExpr>& columnElement) = 0;
};


class FullColumnNameExpressionAtomCBH : public BaseCBH {
public:
    virtual void HandleFullColumnNameExpressionAtom(shared_ptr<query::ValueFactor>& valueFactor) = 0;
};


class BinaryComparasionPredicateCBH : public BaseCBH {
public:
    virtual ~BinaryComparasionPredicateCBH() {}
    virtual void handleBinaryComparasionPredicate(shared_ptr<query::CompPredicate>& comparisonPredicate) = 0;
};


class PredicateExpressionCBH : public BaseCBH {
public:
    virtual void handlePredicateExpression(shared_ptr<query::BoolFactor>& boolFactor) = 0;
    virtual void handlePredicateExpression(shared_ptr<query::ValueExpr>& ValueExpr) = 0;
};


class ConstantCBH : public BaseCBH {
public:
    virtual void handleConstant(shared_ptr<query::ValueFactor> const & val) = 0;
};


class ExpressionsCBH : public BaseCBH {
public:
    virtual void handleExpressions(vector<shared_ptr<query::ValueExpr>> const& valueExprs) = 0;
};


class ConstantsCBH : public BaseCBH {
public:
    virtual void handleConstants(const vector<shared_ptr<query::ValueFactor>>& valueFactors) = 0;
};


class AggregateFunctionCallCBH : public BaseCBH {
public:
    virtual void handleAggregateFunctionCall(string functionName, string paramete) = 0;
};


class UdfFunctionCallCBH : public BaseCBH {
public:
    virtual void handleUdfFunctionCall(shared_ptr<query::FuncExpr> valueExpr) = 0;
};

class AggregateWindowedFunctionCBH : public BaseCBH {
public:
    virtual void handleAggregateWindowedFunction(string functionName, string parameter) = 0;
};


class FunctionArgsCBH : public BaseCBH {
public:
    virtual void handleFunctionArgs(shared_ptr<query::ValueExpr> valueExpr) = 0;
};

class LogicalExpressionCBH : public BaseCBH {
public:
    // pass thru to parent for qserv function spec
    virtual void handleQservFunctionSpec(const string& functionName,
            const vector<shared_ptr<query::ValueFactor>>& args) = 0;

    virtual void handleLogicalExpression(shared_ptr<query::LogicalTerm>& logicalTerm,
            antlr4::ParserRuleContext* childCtx) = 0;
};

class InPredicateCBH : public BaseCBH {
public :
    virtual void handleInPredicate(shared_ptr<query::InPredicate>& inPredicate) = 0;
};

class BetweenPredicateCBH : public BaseCBH {
public:
    virtual void handleBetweenPredicate(shared_ptr<query::BetweenPredicate>& betweenPredicate) = 0;
};


class UnaryExpressionAtomCBH : public BaseCBH {
public:
};


class MathExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleMathExpressionAtomAdapter(shared_ptr<query::ValueExpr> valueExpr) = 0;
};


class FunctionCallExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleFunctionCallExpressionAtom(shared_ptr<query::FuncExpr> funcExpr) = 0;
};


class UnaryOperatorCBH : public BaseCBH {
public:
};


class LogicalOperatorCBH : public BaseCBH {
public:
    enum OperatorType {
        AND,
    };
    virtual void handleLogicalOperator(OperatorType operatorType) = 0;
};


class MathOperatorCBH : public BaseCBH {
public:
    enum OperatorType {
        SUBTRACT,
    };
    virtual void handleMathOperator(OperatorType operatorType) = 0;
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
};


template <typename CBH>
class AdapterT : public Adapter {
public:
    AdapterT(shared_ptr<CBH>& parent) : _parent(parent) {}

protected:
    shared_ptr<CBH> lockedParent() {
        shared_ptr<CBH> parent = _parent.lock();
        if (nullptr == parent) {
            throw QSMySqlListener::adapter_execution_error(
                    "Locking weak ptr to parent callback handler returned null");
        }
        return parent;
    }

private:
    weak_ptr<CBH> _parent;
};


class RootAdapter :
        public Adapter,
        public DmlStatementCBH {
public:
    RootAdapter()
    : _ctx(nullptr)
    {}

    shared_ptr<query::SelectStmt>& getSelectStatement() { return _selectStatement; }

    void handleDmlStatement(shared_ptr<query::SelectStmt>& selectStatement) override {
        _selectStatement = selectStatement;
    }

    void onEnter(QSMySqlParser::RootContext* ctx) {
        _ctx = ctx;
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(_selectStatement != nullptr, "Could not parse query.", _ctx);
    }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
    QSMySqlParser::RootContext* _ctx;
};


class DmlStatementAdapter :
        public AdapterT<DmlStatementCBH>,
        public SimpleSelectCBH {
public:
    DmlStatementAdapter(shared_ptr<DmlStatementCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleSelectStatement(shared_ptr<query::SelectStmt>& selectStatement) override {
        _selectStatement = selectStatement;
    }

    void onExit() override {
        lockedParent()->handleDmlStatement(_selectStatement);
    }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
};


class SimpleSelectAdapter :
        public AdapterT<SimpleSelectCBH>,
        public QuerySpecificationCBH {
public:
    SimpleSelectAdapter(shared_ptr<SimpleSelectCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleQuerySpecification(shared_ptr<query::SelectList>& selectList,
                                  shared_ptr<query::FromList>& fromList,
                                  shared_ptr<query::WhereClause>& whereClause) override {
        _selectList = selectList;
        _fromList = fromList;
        _whereClause = whereClause;
    }

    void onExit() override {
        auto selectStatement = make_shared<query::SelectStmt>();
        selectStatement->setSelectList(_selectList);
        selectStatement->setFromList(_fromList);
        selectStatement->setWhereClause(_whereClause);
        selectStatement->setLimit(_limit);
        lockedParent()->handleSelectStatement(selectStatement);
    }

private:
    shared_ptr<query::SelectList> _selectList;
    shared_ptr<query::FromList> _fromList;
    shared_ptr<query::WhereClause> _whereClause;
    int _limit{lsst::qserv::NOTSET};
};


class QuerySpecificationAdapter :
        public AdapterT<QuerySpecificationCBH>,
        public SelectElementsCBH,
        public FromClauseCBH {
public:
    QuerySpecificationAdapter(shared_ptr<QuerySpecificationCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleSelectList(shared_ptr<query::SelectList>& selectList) override {
        _selectList = selectList;
    }

    void handleFromClause(shared_ptr<query::FromList>& fromList,
                          shared_ptr<query::WhereClause>& whereClause) override {
        _fromList = fromList;
        _whereClause = whereClause;
    }

    void onExit() override {
        lockedParent()->handleQuerySpecification(_selectList, _fromList, _whereClause);
    }

private:
    shared_ptr<query::WhereClause> _whereClause;
    shared_ptr<query::FromList> _fromList;
    shared_ptr<query::SelectList> _selectList;
};


class SelectElementsAdapter :
        public AdapterT<SelectElementsCBH>,
        public SelectColumnElementCBH,
        public SelectFunctionElementCBH {
public:
    SelectElementsAdapter(shared_ptr<SelectElementsCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleColumnElement(shared_ptr<query::ValueExpr>& columnElement) override {
        SelectListFactory::addValueExpr(_selectList, columnElement);
    }

    void handleSelectFunctionElement(shared_ptr<query::ValueExpr> selectFunction) override {
        SelectListFactory::addSelectAggFunction(_selectList, selectFunction);
    }

    void onExit() override {
        lockedParent()->handleSelectList(_selectList);
    }

private:
    shared_ptr<query::SelectList> _selectList{make_shared<query::SelectList>()};
};


class FromClauseAdapter :
        public AdapterT<FromClauseCBH>,
        public TableSourcesCBH,
        public PredicateExpressionCBH,
        public LogicalExpressionCBH,
        public QservFunctionSpecCBH {
public:
    FromClauseAdapter(shared_ptr<FromClauseCBH>& parent, QSMySqlParser::FromClauseContext* ctx)
    : AdapterT(parent), _ctx(ctx) {}

    void handleTableSources(query::TableRefListPtr tableRefList) override {
        _tableRefList = tableRefList;
    }

    void handlePredicateExpression(shared_ptr<query::BoolFactor>& boolFactor) override {
        shared_ptr<query::AndTerm> andTerm = make_shared<query::AndTerm>();
        shared_ptr<query::BoolTerm> boolTerm = boolFactor;
        andTerm->addBoolTerm(boolTerm);
        shared_ptr<query::BoolTerm> andBoolTerm = andTerm;
        _rootTerm->addBoolTerm(andBoolTerm);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr>& ValueExpr) override {
        ASSERT_EXECUTION_CONDITION(false, "Unhandled valueExpr predicateExpression.", _ctx);
    }

    void handleLogicalExpression(shared_ptr<query::LogicalTerm>& logicalTerm,
            antlr4::ParserRuleContext* childCtx) override {
        if (_ctx->whereExpr == childCtx) {
            auto boolTerm = shared_ptr<query::BoolTerm>(logicalTerm);
            _rootTerm->addBoolTerm(boolTerm);
            return;
        } else if (_ctx->havingExpr == childCtx) {
            ASSERT_EXECUTION_CONDITION(false, "The HAVING expression is not yet supported.", _ctx);
        }
        ASSERT_EXECUTION_CONDITION(false, "This logical expression is not yet supported.", _ctx);
    }

    void handleQservFunctionSpec(const string& functionName,
            const vector<shared_ptr<query::ValueFactor>>& args) {
        WhereFactory::addQservRestrictor(_whereClause, functionName, args);
    }

    void onExit() override {
        shared_ptr<query::FromList> fromList = make_shared<query::FromList>(_tableRefList);
        _whereClause->setRootTerm(_rootTerm);

        lockedParent()->handleFromClause(fromList, _whereClause);
    }

private:
    // I think the first term of a where clause is always an OrTerm, and it needs to be added by default.
    shared_ptr<query::WhereClause> _whereClause{make_shared<query::WhereClause>()};
    query::TableRefListPtr _tableRefList;

    shared_ptr<query::OrTerm> _rootTerm {make_shared<query::OrTerm>()};
    QSMySqlParser::FromClauseContext* _ctx;
};


class TableSourcesAdapter :
        public AdapterT<TableSourcesCBH>,
        public TableSourceBaseCBH {
public:
    TableSourcesAdapter(shared_ptr<TableSourcesCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleTableSource(shared_ptr<query::TableRef>& tableRef) override {
        _tableRefList->push_back(tableRef);
    }

    void onExit() override {
        lockedParent()->handleTableSources(_tableRefList);
    }

private:
    query::TableRefListPtr _tableRefList{make_shared<query::TableRefList>()};
};


class TableSourceBaseAdapter :
        public AdapterT<TableSourceBaseCBH>,
        public AtomTableItemCBH {
public:
    TableSourceBaseAdapter(shared_ptr<TableSourceBaseCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleAtomTableItem(shared_ptr<query::TableRef>& tableRef) override {
        _tableRef = tableRef;
    }

    void onExit() override {
        lockedParent()->handleTableSource(_tableRef);
    }

private:
    shared_ptr<query::TableRef> _tableRef;
};


class AtomTableItemAdapter :
        public AdapterT<AtomTableItemCBH>,
        public TableNameCBH {
public:
    AtomTableItemAdapter(shared_ptr<AtomTableItemCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleTableName(const string& string) override {
        _table = string;
    }

    void onExit() override {
        shared_ptr<query::TableRef> tableRef = make_shared<query::TableRef>(_db, _table, _alias);
        lockedParent()->handleAtomTableItem(tableRef);
    }

protected:
    string _db;
    string _table;
    string _alias;
};


class TableNameAdapter :
        public AdapterT<TableNameCBH>,
        public FullIdCBH {
public:
    TableNameAdapter(shared_ptr<TableNameCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleFullId(const string& string) override {
        lockedParent()->handleTableName(string);
    }

    void onExit() override {}
};


class FullIdAdapter :
        public AdapterT<FullIdCBH>,
        public UidCBH {
public:
    FullIdAdapter(shared_ptr<FullIdCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    virtual ~FullIdAdapter() {}

    void handleUidString(const string& string) override {
        lockedParent()->handleFullId(string);
    }

    void onExit() override {}
};


class FullColumnNameAdapter :
        public AdapterT<FullColumnNameCBH>,
        public UidCBH {
public:
    FullColumnNameAdapter(shared_ptr<FullColumnNameCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleUidString(const string& string) override {
        auto valueFactor = ValueFactorFactory::newColumnColumnFactor("", "", string);
        lockedParent()->handleFullColumnName(valueFactor);
    }

    void onExit() override {}
};


class ConstantExpressionAtomAdapter :
        public AdapterT<ConstantExpressionAtomCBH>,
        public ConstantCBH {
public:
    ConstantExpressionAtomAdapter(shared_ptr<ConstantExpressionAtomCBH>& parent,
                                  antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleConstant(shared_ptr<query::ValueFactor> const & valueFactor) override {
        lockedParent()->handleConstantExpressionAtom(valueFactor);
    }

    void onExit() override {}
};


class FullColumnNameExpressionAtomAdapter :
        public AdapterT<FullColumnNameExpressionAtomCBH>,
        public FullColumnNameCBH {
public:
    FullColumnNameExpressionAtomAdapter(shared_ptr<FullColumnNameExpressionAtomCBH>& parent,
                                        antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleFullColumnName(shared_ptr<query::ValueFactor>& valueFactor) override {
        lockedParent()->HandleFullColumnNameExpressionAtom(valueFactor);
    }

    void onExit() override {}
};


class ExpressionAtomPredicateAdapter :
        public AdapterT<ExpressionAtomPredicateCBH>,
        public ConstantExpressionAtomCBH,
        public FullColumnNameExpressionAtomCBH,
        public FunctionCallExpressionAtomCBH,
        public UnaryExpressionAtomCBH,
        public MathExpressionAtomCBH {
public:
    ExpressionAtomPredicateAdapter(shared_ptr<ExpressionAtomPredicateCBH>& parent,
                                   QSMySqlParser::ExpressionAtomPredicateContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx) {}

    void handleConstantExpressionAtom(shared_ptr<query::ValueFactor> const & valueFactor) override {
        auto valueExpr = query::ValueExpr::newSimple(valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleFunctionCallExpressionAtom(shared_ptr<query::FuncExpr> funcExpr) override {
        auto valueFactor = query::ValueFactor::newFuncFactor(funcExpr);
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleMathExpressionAtomAdapter(shared_ptr<query::ValueExpr> valueExpr) override {
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void HandleFullColumnNameExpressionAtom(shared_ptr<query::ValueFactor>& valueFactor) override {
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void onEnter() override {
        ASSERT_EXECUTION_CONDITION(_ctx->LOCAL_ID() == nullptr, "LOCAL_ID is not supported", _ctx);
        ASSERT_EXECUTION_CONDITION(_ctx->VAR_ASSIGN() == nullptr, "VAR_ASSIGN is not supported", _ctx);
    }

    void onExit() override {}

private:
    QSMySqlParser::ExpressionAtomPredicateContext* _ctx;
};


class QservFunctionSpecAdapter :
        public AdapterT<QservFunctionSpecCBH>,
        public ConstantsCBH {
public:
    QservFunctionSpecAdapter(shared_ptr<QservFunctionSpecCBH> & parent,
            QSMySqlParser::QservFunctionSpecContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx) {}

    void handleConstants(const vector<shared_ptr<query::ValueFactor>>& valueFactors) override {
        ASSERT_EXECUTION_CONDITION(_args.empty(), "args should be set exactly once.", _ctx);
        _args = valueFactors;
    }

    void onExit() override {
        lockedParent()->handleQservFunctionSpec(getFunctionName(), _args);
    }

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

    QSMySqlParser::QservFunctionSpecContext* _ctx;
    vector<shared_ptr<query::ValueFactor>> _args;
};


// PredicateExpressionAdapter gathers BoolFactors into a BoolFactor (which is a BoolTerm).
class PredicateExpressionAdapter :
        public AdapterT<PredicateExpressionCBH>,
        public BinaryComparasionPredicateCBH,
        public BetweenPredicateCBH,
        public InPredicateCBH,
        public ExpressionAtomPredicateCBH {
public:
    PredicateExpressionAdapter(shared_ptr<PredicateExpressionCBH>& parent,
            QSMySqlParser::PredicateExpressionContext* ctx)
    : AdapterT(parent), _ctx(ctx) {}

    // BinaryComparasionPredicateCBH
    void handleBinaryComparasionPredicate(shared_ptr<query::CompPredicate>& comparisonPredicate) override {
        _prepBoolFactor();
        _boolFactor->addBoolFactorTerm(comparisonPredicate);
    }

    void handleBetweenPredicate(shared_ptr<query::BetweenPredicate>& betweenPredicate) override {
        _prepBoolFactor();
        _boolFactor->addBoolFactorTerm(betweenPredicate);
    }

    void handleInPredicate(shared_ptr<query::InPredicate>& inPredicate) override {
        _prepBoolFactor();
        _boolFactor->addBoolFactorTerm(inPredicate);
    }

    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr>& valueExpr,
            antlr4::ParserRuleContext* childCtx) {
        _prepValueExpr();
        _valueExpr = valueExpr;
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
    QSMySqlParser::PredicateExpressionContext* _ctx;
};


class BinaryComparasionPredicateAdapter :
        public AdapterT<BinaryComparasionPredicateCBH>,
        public ExpressionAtomPredicateCBH,
        public ComparisonOperatorCBH {
public:
    BinaryComparasionPredicateAdapter(shared_ptr<BinaryComparasionPredicateCBH>& parent,
            QSMySqlParser::BinaryComparasionPredicateContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx)
    {}

    void handleComparisonOperator(const string& text) override {
        ASSERT_EXECUTION_CONDITION(_comparison.empty(), "comparison must be set only once.", _ctx);
        _comparison = text;
    }

    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr>& valueExpr,
            antlr4::ParserRuleContext* ctx) override {
        if (_left == nullptr) {
            _left = valueExpr;
        } else if (_right == nullptr) {
            _right = valueExpr;
        } else {
            ASSERT_EXECUTION_CONDITION(false, "left and right values must be set only once.", _ctx)
        }
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
        } else {
            ASSERT_EXECUTION_CONDITION(false, "unhandled comparison operator type" << _comparison, _ctx);
        }

        compPredicate->right = _right;

        lockedParent()->handleBinaryComparasionPredicate(compPredicate);
    }

private:
    shared_ptr<query::ValueExpr> _left;
    string _comparison;
    shared_ptr<query::ValueExpr> _right;
    QSMySqlParser::BinaryComparasionPredicateContext* _ctx;
};


class ComparisonOperatorAdapter :
        public AdapterT<ComparisonOperatorCBH> {
public:
    ComparisonOperatorAdapter(shared_ptr<ComparisonOperatorCBH>& parent,
            QSMySqlParser::ComparisonOperatorContext* ctx)
    : AdapterT(parent)
    ,  _ctx(ctx)
    {}

    void onExit() override {
        lockedParent()->handleComparisonOperator(_ctx->getText());
    }

private:
    QSMySqlParser::ComparisonOperatorContext* _ctx;
};


// handles `functionCall (AS? uid)?` e.g. "COUNT AS object_count"
class SelectFunctionElementAdapter :
        public AdapterT<SelectFunctionElementCBH>,
        public AggregateFunctionCallCBH,
        public UidCBH {
public:
    SelectFunctionElementAdapter(shared_ptr<SelectFunctionElementCBH>& parent,
                                 QSMySqlParser::SelectFunctionElementContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx)
    {}

    void handleUidString(const string& string) override {
        // Uid is expected to be the aliasName in `functionCall AS aliasName`
        if (false == _asName.empty()) {
            throw QSMySqlListener::adapter_execution_error("Second call to handleUidString.");
        }
        if (_ctx->AS() == nullptr) {
            throw QSMySqlListener::adapter_execution_error("Call to handleUidString but AS is null.");
        }
        _asName = string;
    }

    void handleAggregateFunctionCall(string functionName, string parameter) override {
        ASSERT_EXECUTION_CONDITION(_functionName.empty() && _parameter.empty(), "should only be called once.",
                _ctx);
        _functionName = functionName;
        _parameter = parameter;
    }

    void onExit() override {
        // handle this in aggregateWindowedFunction
        string table;
        auto starFactor = query::ValueFactor::newStarFactor(table);
        auto starParExpr = std::make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(starParExpr, starFactor);
        auto countStarFuncExpr = query::FuncExpr::newArg1(_functionName, starParExpr);

        // this probably wants to be in aggregateFunctionCall (because it makes an aggregate function value factor)
        auto aggFuncValueFactor = query::ValueFactor::newAggFactor(countStarFuncExpr);
        auto countStarFuncAsValueExpr = std::make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(countStarFuncAsValueExpr, aggFuncValueFactor);
        countStarFuncAsValueExpr->setAlias(_asName);
        lockedParent()->handleSelectFunctionElement(countStarFuncAsValueExpr);
    }

private:
    string _functionName;
    string _parameter; // todo will probably end up being a vector of string
    string _asName;
    QSMySqlParser::SelectFunctionElementContext* _ctx;
};


class SelectColumnElementAdapter :
        public AdapterT<SelectColumnElementCBH>,
        public FullColumnNameCBH {
public:
    SelectColumnElementAdapter(shared_ptr<SelectColumnElementCBH>& parent, antlr4::ParserRuleContext* ctx)
    : AdapterT(parent) {}

    void handleFullColumnName(shared_ptr<query::ValueFactor>& valueFactor) override {
        auto valueExpr = make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, valueFactor);
        lockedParent()->handleColumnElement(valueExpr);
    }

    void onExit() override {}
};


class UidAdapter :
        public AdapterT<UidCBH> {
public:
    UidAdapter(shared_ptr<UidCBH>& parent, QSMySqlParser::UidContext* ctx)
    : AdapterT(parent), _uidContext(ctx) {}

    void onExit() override {
        // Fetching the string from a Uid shortcuts a large part of the syntax tree defined under Uid
        // (see QSMySqlParser.g4). If Adapters for any nodes in the tree below Uid are implemented then
        // it will have to be handled and this shortcut may not be taken.
        lockedParent()->handleUidString(_uidContext->getText());
    }

private:
    QSMySqlParser::UidContext* _uidContext;
};


class ConstantAdapter :
        public AdapterT<ConstantCBH> {
public:
    ConstantAdapter(shared_ptr<ConstantCBH>& parent, QSMySqlParser::ConstantContext* ctx)
    : AdapterT(parent), _ctx(ctx) {}

    void onExit() override {
        std::shared_ptr<query::ValueFactor> valueFactor;
        if (_ctx->decimalLiteral()) {
            valueFactor = query::ValueFactor::newConstFactor(_ctx->getText());
        } else if (_ctx->stringLiteral()) {
            valueFactor = ValueFactorFactory::newColumnColumnFactor("", "", _ctx->getText());
        } else if (_ctx->hexadecimalLiteral()) {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled type: hexadecimalLiteral", _ctx);
        } else if (_ctx->booleanLiteral()) {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled type: booleanLiteral", _ctx);
        } else if (_ctx->REAL_LITERAL()) {
            valueFactor = query::ValueFactor::newConstFactor(_ctx->getText());
        } else if (_ctx->BIT_STRING()) {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled type: BIT_STRING", _ctx);
        } else if (_ctx->NULL_LITERAL()) {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled type: NULL_LITERAL", _ctx);
        } else if (_ctx->NULL_SPEC_LITERAL()) {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled type: NULL_SPEC_LITERAL", _ctx);
        } else if (_ctx->NOT()) {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled type: NOT", _ctx);
        } else if (_ctx->nullLiteral) {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled type: nullliteral", _ctx);
        } else {
            ASSERT_EXECUTION_CONDITION(false, "Unhandled type.", _ctx);
        }
        lockedParent()->handleConstant(valueFactor);
    }

private:
    QSMySqlParser::ConstantContext* _ctx;
};


class ExpressionsAdapter :
        public AdapterT<ExpressionsCBH>,
        public PredicateExpressionCBH {
public:
    ExpressionsAdapter(shared_ptr<ExpressionsCBH>& parent, QSMySqlParser::ExpressionsContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx)
    {}

    void handlePredicateExpression(shared_ptr<query::BoolFactor>& boolFactor) override {
        ASSERT_EXECUTION_CONDITION(false, "Unhandled PredicateExpression with BoolFactor.", _ctx);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr>& valueExpr) override {
        _expressions.push_back(valueExpr);
    }

    void onExit() {
        lockedParent()->handleExpressions(_expressions);
    }

private:
    QSMySqlParser::ExpressionsContext* _ctx;
    vector<shared_ptr<query::ValueExpr>> _expressions;
};


class ConstantsAdapter :
        public AdapterT<ConstantsCBH>,
        public ConstantCBH {
public:
    ConstantsAdapter(shared_ptr<ConstantsCBH>& parent, QSMySqlParser::ConstantsContext* ctx)
    : AdapterT(parent) {}

    void handleConstant(shared_ptr<query::ValueFactor> const & valueFactor) override {
        valueFactors.push_back(valueFactor);
    }

    void onExit() override {
        lockedParent()->handleConstants(valueFactors);
    }

private:
    vector<shared_ptr<query::ValueFactor>> valueFactors;
};


class AggregateFunctionCallAdapter :
        public AdapterT<AggregateFunctionCallCBH>,
        public AggregateWindowedFunctionCBH {
public:
    AggregateFunctionCallAdapter(shared_ptr<AggregateFunctionCallCBH>& parent,
                                 QSMySqlParser::AggregateFunctionCallContext* ctx)
    : AdapterT(parent) {}

    void handleAggregateWindowedFunction(string functionName, string parameter) override {
        lockedParent()->handleAggregateFunctionCall(functionName, parameter);
    }

    void onExit() override {}
};


class UdfFunctionCallAdapter :
        public AdapterT<UdfFunctionCallCBH>,
        public FullIdCBH,
        public FunctionArgsCBH {
public:
    UdfFunctionCallAdapter(shared_ptr<UdfFunctionCallCBH>& parent,
                           QSMySqlParser::UdfFunctionCallContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx)
    {}

    void handleFunctionArgs(shared_ptr<query::ValueExpr> valueExpr) override {
        // This is only expected to be called once.
        // Of course the valueExpr may have more than one valueFactor.
        ASSERT_EXECUTION_CONDITION(nullptr == _args, "Args already assigned.", _ctx);
        _args = valueExpr;
    }

    // FullIdCBH
    void handleFullId(const string& string) override {
        if (false == _functionName.empty()) {
            throw QSMySqlListener::adapter_execution_error("Function name already assigned.");
        }
        _functionName = string;
    }

    void onExit() override {
        if (_functionName.empty() || nullptr == _args) {
            throw QSMySqlListener::adapter_execution_error("Function name & args must be populated");
        }
        auto funcExpr = query::FuncExpr::newArg1(_functionName, _args);
        lockedParent()->handleUdfFunctionCall(funcExpr);
    }

private:
    shared_ptr<query::ValueExpr> _args;
    string _functionName;
    QSMySqlParser::UdfFunctionCallContext* _ctx;
};


class AggregateWindowedFunctionAdapter :
        public AdapterT<AggregateWindowedFunctionCBH> {
public:
    AggregateWindowedFunctionAdapter(shared_ptr<AggregateWindowedFunctionCBH>& parent,
                                     QSMySqlParser::AggregateWindowedFunctionContext* ctx)
    : AdapterT(parent), _ctx(ctx) {}

    void onExit() override {
        if (nullptr == _ctx->COUNT() || nullptr == _ctx->starArg) {
            throw QSMySqlListener::adapter_execution_error(string("AggregateWindowedFunctionAdapter ") +
                    string("currently only supports COUNT(*), can't handle ") + _ctx->getText());
        }
        lockedParent()->handleAggregateWindowedFunction("COUNT", "*");
    }

private:
    QSMySqlParser::AggregateWindowedFunctionContext* _ctx;
};


class FunctionArgsAdapter :
        public AdapterT<FunctionArgsCBH>,
        public ConstantCBH,
        public FullColumnNameCBH {
public:
    FunctionArgsAdapter(shared_ptr<FunctionArgsCBH>& parent,
                        QSMySqlParser::FunctionArgsContext* ctx)
    : AdapterT(parent) {}

    // ConstantCBH
    void handleConstant(shared_ptr<query::ValueFactor> const & valueFactor) override {
        ValueExprFactory::addValueFactor(_args, valueFactor);
    }

    void handleFullColumnName(shared_ptr<query::ValueFactor>& columnName) override {
        ValueExprFactory::addValueFactor(_args, columnName);
    }

    void onExit() override {
        lockedParent()->handleFunctionArgs(_args);
    }

private:
    shared_ptr<query::ValueExpr> _args {make_shared<query::ValueExpr>()};
};


class LogicalExpressionAdapter :
        public AdapterT<LogicalExpressionCBH>,
        public LogicalExpressionCBH,
        public PredicateExpressionCBH,
        public LogicalOperatorCBH,
        public QservFunctionSpecCBH {
public:
    LogicalExpressionAdapter(shared_ptr<LogicalExpressionCBH> parent,
                             QSMySqlParser::LogicalExpressionContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx) {}

    void handlePredicateExpression(shared_ptr<query::BoolFactor>& boolFactor) override {
        _setNextTerm(boolFactor);
    }

    void handlePredicateExpression(shared_ptr<query::ValueExpr>& valueExpr) override {
        ASSERT_EXECUTION_CONDITION(false, "Unhandled PredicateExpression with ValueExpr.", _ctx);
    }

    void handleQservFunctionSpec(const string& functionName,
            const vector<shared_ptr<query::ValueFactor>>& args) override {
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
            shared_ptr<query::LogicalTerm> logicalTerm = make_shared<query::AndTerm>();
            _setLogicalOperator(logicalTerm);
            break;
        }
    }

    virtual void handleLogicalExpression(shared_ptr<query::LogicalTerm>& logicalTerm,
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

private:
    void _setLogicalOperator(shared_ptr<query::LogicalTerm>& logicalTerm) {
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
    QSMySqlParser::LogicalExpressionContext* _ctx;
};


ostream& operator<<(ostream& os, const LogicalExpressionAdapter& logicalExpressionAdapter) {
    os << "LogicalExpressionAdapter(";
    os << "terms:" << util::printable(logicalExpressionAdapter._terms);
    return os;
}


class InPredicateAdapter :
        public AdapterT<InPredicateCBH>,
        public ExpressionAtomPredicateCBH,
        public ExpressionsCBH {
public:
    InPredicateAdapter(shared_ptr<InPredicateCBH> parent,
                            QSMySqlParser::InPredicateContext * ctx)
    : AdapterT(parent)
    , _ctx(ctx) {}

     void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr>& valueExpr,
             antlr4::ParserRuleContext* childCtx) {
         ASSERT_EXECUTION_CONDITION(_ctx->predicate() == childCtx, "callback from unexpected element.", _ctx);
         ASSERT_EXECUTION_CONDITION(nullptr == _predicate, "Predicate should be set exactly once.", _ctx);
         _predicate = valueExpr;
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

private:
    QSMySqlParser::InPredicateContext * _ctx;
    shared_ptr<query::ValueExpr> _predicate;
    vector<shared_ptr<query::ValueExpr>> _expressions;
};


class BetweenPredicateAdapter :
        public AdapterT<BetweenPredicateCBH>,
        public ExpressionAtomPredicateCBH {
public:
    BetweenPredicateAdapter(shared_ptr<BetweenPredicateCBH> parent,
                            QSMySqlParser::BetweenPredicateContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx) {}

    // ExpressionAtomPredicateCBH
    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr>& valueExpr,
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

    void onExit() {
        ASSERT_EXECUTION_CONDITION(nullptr != _val && nullptr != _min && nullptr != _max,
                "val, min, and max must all be set.", _ctx);
        auto betweenPredicate = make_shared<query::BetweenPredicate>(_val, _min, _max);
        lockedParent()->handleBetweenPredicate(betweenPredicate);
    }

private:
    QSMySqlParser::BetweenPredicateContext* _ctx;
    shared_ptr<query::ValueExpr> _val;
    shared_ptr<query::ValueExpr> _min;
    shared_ptr<query::ValueExpr> _max;
};


class UnaryExpressionAtomAdapter :
        public AdapterT<UnaryExpressionAtomCBH>,
        public UnaryOperatorCBH {
public:
    UnaryExpressionAtomAdapter(shared_ptr<UnaryExpressionAtomCBH> parent,
                               QSMySqlParser::UnaryExpressionAtomContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx)
    {}

    void onExit() override {
        // todo
    }

private:
    QSMySqlParser::UnaryExpressionAtomContext* _ctx;
};


class MathExpressionAtomAdapter :
        public AdapterT<MathExpressionAtomCBH>,
        public MathOperatorCBH,
        public FunctionCallExpressionAtomCBH {
public:
    MathExpressionAtomAdapter(shared_ptr<MathExpressionAtomCBH> parent,
                              QSMySqlParser::MathExpressionAtomContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx)
    {}

    void handleFunctionCallExpressionAtom(shared_ptr<query::FuncExpr> funcExpr) override {
        _setNextFuncExpr(funcExpr);
    }

    void handleMathOperator(MathOperatorCBH::OperatorType operatorType) override {
        // need to make:
        // FactorOp(MINUS, factor:ValueFactor(type:Function, funcExpr)
        ASSERT_EXECUTION_CONDITION(MathOperatorCBH::SUBTRACT == operatorType,
                "Currenty only subtract is supported.", _ctx);
        _op = query::ValueExpr::MINUS;
    }

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(query::ValueExpr::NONE != _op && nullptr != _left && nullptr != _right,
                "Not all fields are set:" << *this, _ctx);
        auto valueExpr = ValueExprFactory::newOperationFuncExpr(_left, _op, _right);
        lockedParent()->handleMathExpressionAtomAdapter(valueExpr);
    }

private:
    friend ostream& operator<<(ostream& os, const MathExpressionAtomAdapter& mathExpressionAtomAdapter);

    void _setNextFuncExpr(shared_ptr<query::FuncExpr> funcExpr) {
        if (nullptr == _left) {
            _left = funcExpr;
        } else if (nullptr == _right) {
            _right = funcExpr;
        } else {
            ASSERT_EXECUTION_CONDITION(false, "left and right are both already set:" << *this, _ctx);
        }
    }

    query::ValueExpr::Op _op {query::ValueExpr::NONE};
    shared_ptr<query::FuncExpr> _left;
    shared_ptr<query::FuncExpr> _right;
    QSMySqlParser::MathExpressionAtomContext* _ctx;
};


ostream& operator<<(ostream& os, const MathExpressionAtomAdapter& mathExpressionAtomAdapter) {
os << "MathExpressionAtomAdapter(left:" << mathExpressionAtomAdapter._left
        << ", right:" << mathExpressionAtomAdapter._right
        << ")";
return os;
}


class FunctionCallExpressionAtomAdapter :
        public AdapterT<FunctionCallExpressionAtomCBH>,
        public UdfFunctionCallCBH {
public:
    FunctionCallExpressionAtomAdapter(shared_ptr<FunctionCallExpressionAtomCBH> parent,
                                      QSMySqlParser::FunctionCallExpressionAtomContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx)
    {}

    void handleUdfFunctionCall(shared_ptr<query::FuncExpr> funcExpr) override {
        ASSERT_EXECUTION_CONDITION(_funcExpr == nullptr, "the funcExpr must be set only once.", _ctx)
        _funcExpr = funcExpr;
    }

    // someday: the `AS uid` part should be handled by making this a UID CBH,
    // it will set the alias in the generated valueFactor

    void onExit() {
        lockedParent()->handleFunctionCallExpressionAtom(_funcExpr);
    }

private:
    shared_ptr<query::FuncExpr> _funcExpr;
    QSMySqlParser::FunctionCallExpressionAtomContext* _ctx;
};


class UnaryOperatorAdapter :
        public AdapterT<UnaryOperatorCBH> {
public:
    UnaryOperatorAdapter(shared_ptr<UnaryOperatorCBH> parent,
                         QSMySqlParser::UnaryOperatorContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx)
    {}

    void onExit() override {
        ASSERT_EXECUTION_CONDITION(false, "UnaryOperatorAdapter is not yet handling anything.", _ctx);
    }

private:
    QSMySqlParser::UnaryOperatorContext* _ctx;
};


class LogicalOperatorAdapter :
        public AdapterT<LogicalOperatorCBH> {
public:
    LogicalOperatorAdapter(shared_ptr<LogicalOperatorCBH> parent,
                           QSMySqlParser::LogicalOperatorContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx) {}

    void onExit() override {
        if (_ctx->AND() != nullptr) {
            lockedParent()->handleLogicalOperator(LogicalOperatorCBH::AND);
        } else {
            ASSERT_EXECUTION_CONDITION(false, "undhandled logical operator", _ctx);
        }
    }

private:
    QSMySqlParser::LogicalOperatorContext* _ctx;
};


class MathOperatorAdapter :
        public AdapterT<MathOperatorCBH> {
public:
    MathOperatorAdapter(shared_ptr<MathOperatorCBH> parent,
                        QSMySqlParser::MathOperatorContext* ctx)
    : AdapterT(parent)
    , _ctx(ctx) {}

    void onExit() override {
        if (_ctx->getText() == "-") {
            lockedParent()->handleMathOperator(MathOperatorCBH::SUBTRACT);
        }
    }

private:
    QSMySqlParser::MathOperatorContext* _ctx;
};


/// QSMySqlListener impl


QSMySqlListener::QSMySqlListener() {
}


shared_ptr<query::SelectStmt> QSMySqlListener::getSelectStatement() const {
    return _rootAdapter->getSelectStatement();
}


// Create and push an Adapter onto the context stack, using the current top of the stack as a callback handler
// for the new Adapter. Returns the new Adapter.
template<typename ParentCBH, typename ChildAdapter, typename Context>
shared_ptr<ChildAdapter> QSMySqlListener::pushAdapterStack(Context* ctx) {
    auto p = dynamic_pointer_cast<ParentCBH>(_adapterStack.top());
    ASSERT_EXECUTION_CONDITION(p != nullptr, "can't acquire expected Adapter `" <<
            getTypeName<ParentCBH>() <<
            "` from top of listenerStack.",
            ctx);
    auto childAdapter = make_shared<ChildAdapter>(p, ctx);
    _adapterStack.push(childAdapter);
    childAdapter->onEnter();
    return childAdapter;
}


template<typename ChildAdapter>
void QSMySqlListener::popAdapterStack(antlr4::ParserRuleContext* ctx) {
    shared_ptr<Adapter> adapterPtr = _adapterStack.top();
    adapterPtr->onExit();
    _adapterStack.pop();
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


// QSMySqlListener class methods


void QSMySqlListener::enterRoot(QSMySqlParser::RootContext* ctx) {
    ASSERT_EXECUTION_CONDITION(_adapterStack.empty(), "RootAdatper should be the first entry on the stack.",
            ctx);
    _rootAdapter = make_shared<RootAdapter>();
    _adapterStack.push(_rootAdapter);
    _rootAdapter->onEnter(ctx);
}


void QSMySqlListener::exitRoot(QSMySqlParser::RootContext* ctx) {
    popAdapterStack<RootAdapter>(ctx);
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
UNHANDLED(OrderByClause)
UNHANDLED(OrderByExpression)
UNHANDLED(TableSourceNested)
UNHANDLED(SubqueryTableItem)
UNHANDLED(TableSourcesItem)
UNHANDLED(IndexHint)
UNHANDLED(IndexHintType)
UNHANDLED(InnerJoin)
UNHANDLED(StraightJoin)
UNHANDLED(OuterJoin)
UNHANDLED(NaturalJoin)
UNHANDLED(QueryExpression)
UNHANDLED(QueryExpressionNointo)
UNHANDLED(QuerySpecificationNointo)
UNHANDLED(UnionParenthesis)
UNHANDLED(UnionStatement)
UNHANDLED(SelectSpec)
UNHANDLED(SelectStarElement)
ENTER_EXIT_PARENT(SelectFunctionElement)
UNHANDLED(SelectExpressionElement)
UNHANDLED(SelectIntoVariables)
UNHANDLED(SelectIntoDumpFile)
UNHANDLED(SelectIntoTextFile)
UNHANDLED(SelectFieldsInto)
UNHANDLED(SelectLinesInto)
UNHANDLED(GroupByItem)
UNHANDLED(LimitClause)
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
IGNORED(SimpleId)
UNHANDLED(DottedId)
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
UNHANDLED(UidList)
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
UNHANDLED(ScalarFunctionCall)
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
UNHANDLED(ScalarFunctionName)
UNHANDLED(PasswordFunctionClause)
ENTER_EXIT_PARENT(FunctionArgs)
UNHANDLED(FunctionArg)
UNHANDLED(IsExpression)
UNHANDLED(NotExpression)
IGNORED(QservFunctionSpecExpression)
ENTER_EXIT_PARENT(LogicalExpression)
UNHANDLED(SoundsLikePredicate)
ENTER_EXIT_PARENT(InPredicate)
UNHANDLED(SubqueryComparasionPredicate)
ENTER_EXIT_PARENT(BetweenPredicate)
UNHANDLED(IsNullPredicate)
UNHANDLED(LikePredicate)
UNHANDLED(RegexpPredicate)
ENTER_EXIT_PARENT(UnaryExpressionAtom)
UNHANDLED(CollateExpressionAtom)
UNHANDLED(SubqueryExpessionAtom)
UNHANDLED(MysqlVariableExpressionAtom)
UNHANDLED(NestedExpressionAtom)
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
UNHANDLED(KeywordsCanBeId)
UNHANDLED(FunctionNameBase)

}}} // namespace lsst::qserv::parser
