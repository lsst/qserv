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


#ifndef LSST_QSERV_CCONTROL_PARSEADAPTERS_H
#define LSST_QSERV_CCONTROL_PARSEADAPTERS_H


// System headers
#include <memory>
#include <sstream>
#include <ostream>
#include <string>

// Third-party headers
#include <boost/algorithm/string/predicate.hpp>

// These antlr-related files must be included before Log.h because antlr4 has a function called LOGS that
// conflicts with the LOGS macro defined in Log.h
#include "antlr4-runtime.h"
#include "parser/QSMySqlLexer.h"
#include "parser/QSMySqlParser.h"
#include "parser/QSMySqlParserListener.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ParseAdaptersCBH.h"
#include "ccontrol/ParseHelpers.h"
#include "ccontrol/ParseListener.h"
#include "ccontrol/UserQueryQservManager.h"
#include "parser/ParseException.h"
#include "query/AndTerm.h"
#include "query/BetweenPredicate.h"
#include "query/BoolFactor.h"
#include "query/BoolTerm.h"
#include "query/BoolTermFactor.h"
#include "query/CompPredicate.h"
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/InPredicate.h"
#include "query/JoinRef.h"
#include "query/LikePredicate.h"
#include "query/LogicalTerm.h"
#include "query/NullPredicate.h"
#include "query/OrderByClause.h"
#include "query/OrTerm.h"
#include "query/PassTerm.h"
#include "query/AreaRestrictor.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/TableRef.h"
#include "query/typedefs.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "util/IterableFormatter.h"


namespace lsst {
namespace qserv {
namespace ccontrol {


// Adapter is the base class for factory classes that exist at nodes in the antlr4 parse tree. (The idea
// behind the name "Adapter" is that each Adapter subclass "adapts" the parameters of the parse tree node to
// a Qserv Intermediate Representation object instance).  There is a  one-to-one relationship between types
// of Adapter subclass and each variation of enter/exit functions that are in MySqlParserBaseListener, which
// are the result of the grammar defined in QSMySqlParser.g4
class Adapter {
public:

    Adapter() {}
    virtual ~Adapter() {}

    // checkContext is called after construction and before the adapter is pushed onto the adapter stack.
    // In this function Adapter subclasses should verify that all the token and terminalNode members of the
    // context are either handled (as required or optional) or rejected if they are non-null.
    // Tokens and terminal nodes are members of the context that are _not_ represented by a child context, so
    // no callbacks will be received for them.
    // Other contexts that are child contexts will be represented by another adapter. It is verified that
    // child contexts are handled by verifying that their parent context is a handler for the child.
    // In the implementations of child context, tokens and terminal nodes in the associated context should
    // all be mentioned explicitly, those that are handled and required should be named in
    // assert_execution_condition calls (because they are required), those that are not handled should be
    // named in assertNotSupported calls (because they are not supported), and those that are handled but are
    // optional should be named in a comment that notes that the token or terminal node is optional.
    virtual void checkContext() const = 0;

    // onEnter is called just after the Adapter is pushed onto the context stack
    virtual void onEnter() {}

    // onExit is called just before the Adapter is popped from the context stack
    virtual void onExit() = 0;

    virtual std::string name() const = 0;

    // used to get a string that reprsents the current stack of adapters, comma delimited.
    virtual std::string adapterStackToString() const = 0;

    // gets the antlr4 string representation of the parsed tree, nested in parenthesis.
    virtual std::string getStringTree() const = 0;

    // gets the antlr4 string representation of the tokenization of the query.
    virtual std::string getTokens() const = 0;

    // get the sql statement
    virtual std::string getStatementString() const = 0;

    // A function to fail in the case of a not-supported query segment. This should be called with a helpful
    // user-visible error message, e.g. "qserv does not support column names in select statements" (although
    // of course it does!). A condition variable is used for convenience, for example you could say
    // `SelectStatement.hasColumns()` so that if it's true then the error will get raised. In the case of an
    // error a detailed message is logged, for debugging, and an adapter_exectuion_error is raised with a
    // message, including the MESSAGE, for the user.
    //
    // function: usually the name of the function where the assert is being executed, most callers pass
    //           __FUNCTION__.
    // condition: the condition that is being asserted. True passes, false logs and throws.
    // message: a message for the log, it is not included in the exception.
    // ctx: the antlr4 context that is used to get the segment of the query that is currently being
    //      processed.
    void assertNotSupported(std::string const& function, bool condition, std::string const& message,
            antlr4::ParserRuleContext* ctx) const {
        if (true == condition) {
            return;
        }
        std::ostringstream msg;
        msg << "Not supported error:";
        msg << getTypeName(this) << "::" << function;
        msg << " messsage:\"" << message << '"';
        msg << ", in query:" << getStatementString();
        msg << ", std::string tree:" << getStringTree();
        msg << ", tokens:" << getTokens();
        LOGS(_log, LOG_LVL_ERROR, msg.str());
        throw parser::adapter_execution_error(
                "Error parsing query, near \"" + getQueryString(ctx) + "\", " + message);
    }


protected:
    /**
     * @brief assert that condition is true, otherwise log a message & throw an adapter_execution_error with
     * the text of the query string that the context represents.
     *
     * @param condition The condition that is being asserted. True passes, false logs and throws.
     * @param messageString A message for the log, it is not included in the exception.
     * @param ctx The antlr4 context that is used to get the segment of the query that is currently being
     *        processed.
     */
    void assert_execution_condition(bool condition, std::string const& messageString,
                                    antlr4::ParserRuleContext* ctx) const {
        if (not (condition)) {
            std::ostringstream msg;
            auto queryString = getQueryString(ctx);
            msg << "Execution condition assertion failure:";
            msg << getTypeName(this) << "::"; // TODO: pass in: << __FUNCTION__;
            msg << " messsage:\"" << messageString << '"';
            msg << ", in query:" << getStatementString();
            msg << ", in or around query segment: '" << queryString << "'";
            msg << ", with adapter stack:" << adapterStackToString();
            msg << ", string tree:" << getStringTree();
            msg << ", tokens:" << getTokens();
            LOGS(_log, LOG_LVL_ERROR, msg.str());
            throw parser::adapter_execution_error("Error parsing query, near \"" + queryString + '"');
        }
    }

    // Used to log (at the trace level) handle... calls, including the class and function name and
    // whatever object or stream of objects (e.g. `valueExpr`, or `valueExpr << " " << functionName` are passed
    // in to CALLBACK_INFO
    template <typename CALLBACK_INFO>
    void trace_callback_info(std::string const& function, CALLBACK_INFO const& callbackInfo) {
        LOGS(_log, LOG_LVL_TRACE, name() << function << " " << callbackInfo);
    }

    static LOG_LOGGER _log;
};


template <typename CBH, typename CTX>
class AdapterT : public Adapter {
public:
    AdapterT(std::shared_ptr<CBH> const& parent, CTX * ctx, ParseListener const * const listener)
    : _ctx(ctx), _parserListener(listener), _parent(parent) {}

protected:
    std::shared_ptr<CBH> lockedParent() {
        std::shared_ptr<CBH> parent = _parent.lock();
        assert_execution_condition(nullptr != parent,
                "Locking weak ptr to parent callback handler returned null", _ctx);
        return parent;
    }

    CTX* _ctx;

    // Used for error messages, uses the _parserListener to get a list of the names of the adapters in the
    // adapter stack,
    std::string adapterStackToString() const { return _parserListener->adapterStackToString(); }
    std::string getStringTree() const { return _parserListener->getStringTree(); }
    std::string getTokens() const { return _parserListener->getTokens(); }
    std::string getStatementString() const { return _parserListener->getStatementString(); }

    std::shared_ptr<ccontrol::UserQueryResources> getQueryResources() const { return _parserListener->getQueryResources(); }

private:
    // Mostly the ParseListener is not used by adapters. It is needed to get the adapter stack list for
    // error messages.
    ParseListener const * const _parserListener;

    std::weak_ptr<CBH> _parent;
};


class RootAdapter :
        public Adapter,
        public DmlStatementCBH {
public:
    RootAdapter()
    : _ctx(nullptr)
    , _parserListener(nullptr)
    {}

    std::shared_ptr<query::SelectStmt> const& getSelectStatement() { return _selectStatement; }

    std::shared_ptr<ccontrol::UserQuery> const& getUserQuery() { return _userQuery; }

    void handleDmlStatement(std::shared_ptr<query::SelectStmt> const& selectStatement) override {
        _selectStatement = selectStatement;
    }

    void handleDmlStatement(std::shared_ptr<ccontrol::UserQuery> const& userQuery) override {
        _userQuery = userQuery;
    }

    void checkContext() const override {
        // check for required tokens:
        assert_execution_condition(_ctx->EOF() != nullptr,
                "Missing context condition: EOF is null.", _ctx);
        // optional:
        // MINUSMINUS (ignored, it indicates a comment)
    }

    virtual void onEnter(QSMySqlParser::RootContext* ctx, ParseListener const * const listener) {
        _ctx = ctx;
        checkContext();
        _parserListener = listener;
    }

    void onExit() override {
        assert_execution_condition(_selectStatement != nullptr || _userQuery != nullptr,
                                   "Could not parse query.", _ctx);
    }

    std::string name() const override { return getTypeName(this); }

    std::string adapterStackToString() const override { return _parserListener->adapterStackToString(); }
    std::string getStringTree() const override { return _parserListener->getStringTree(); }
    std::string getTokens() const override { return _parserListener->getTokens(); }
    std::string getStatementString() const override { return _parserListener->getStatementString(); }

private:
    std::shared_ptr<query::SelectStmt> _selectStatement;
    std::shared_ptr<ccontrol::UserQuery> _userQuery;
    QSMySqlParser::RootContext* _ctx;
    ParseListener const * _parserListener;
};


class DmlStatementAdapter :
        public AdapterT<DmlStatementCBH, QSMySqlParser::DmlStatementContext>,
        public SimpleSelectCBH,
        public CallStatementCBH {
public:
    using AdapterT::AdapterT;

    void handleSelectStatement(std::shared_ptr<query::SelectStmt> const& selectStatement) override {
        assert_execution_condition(_selectStatement == nullptr && _userQuery == nullptr,
                                   "DmlStatementAdapter should be called exactly once.", _ctx);
        _selectStatement = selectStatement;
    }

    void handleCallStatement(std::shared_ptr<ccontrol::UserQuery> const& userQuery) override {
        assert_execution_condition(_selectStatement == nullptr && _userQuery == nullptr,
                                   "DmlStatementAdapter should be called exactly once.", _ctx);
        _userQuery = userQuery;
    }

    void checkContext() const override {
        // there is nothing to check
    }

    void onExit() override {
        if (_selectStatement != nullptr) {
            lockedParent()->handleDmlStatement(_selectStatement);
        } else {
            lockedParent()->handleDmlStatement(_userQuery);
        }
    }

    std::string name () const override { return getTypeName(this); }

private:
    std::shared_ptr<query::SelectStmt> _selectStatement;
    std::shared_ptr<ccontrol::UserQuery> _userQuery;
};


class SimpleSelectAdapter :
        public AdapterT<SimpleSelectCBH, QSMySqlParser::SimpleSelectContext>,
        public QuerySpecificationCBH {
public:
    using AdapterT::AdapterT;

    void handleQuerySpecification(std::shared_ptr<query::SelectList> const& selectList,
                                  std::shared_ptr<query::FromList> const& fromList,
                                  std::shared_ptr<query::WhereClause> const& whereClause,
                                  std::shared_ptr<query::OrderByClause> const& orderByClause,
                                  int limit,
                                  std::shared_ptr<query::GroupByClause> const& groupByClause,
                                  std::shared_ptr<query::HavingClause> const& havingClause,
                                  bool distinct) override {
        _selectList = selectList;
        _fromList = fromList;
        _whereClause = whereClause;
        _orderByClause = orderByClause;
        _limit = limit;
        _groupByClause = groupByClause;
        _havingClause = havingClause;
        _distinct = distinct;
    }

    void checkContext() const override {
        // there is nothing to check
    }

    void onExit() override {
        assert_execution_condition(_selectList != nullptr, "Failed to create a select list.", _ctx);
        auto selectStatement = std::make_shared<query::SelectStmt>(_selectList, _fromList, _whereClause,
                _orderByClause, _groupByClause, _havingClause, _distinct, _limit);
        lockedParent()->handleSelectStatement(selectStatement);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::SelectList> _selectList;
    std::shared_ptr<query::FromList> _fromList;
    std::shared_ptr<query::WhereClause> _whereClause;
    std::shared_ptr<query::OrderByClause> _orderByClause;
    std::shared_ptr<query::GroupByClause> _groupByClause;
    std::shared_ptr<query::HavingClause> _havingClause;
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

    void handleSelectList(std::shared_ptr<query::SelectList> const& selectList) override {
        _selectList = selectList;
    }

    void handleFromClause(std::shared_ptr<query::FromList> const& fromList,
                          std::shared_ptr<query::WhereClause> const& whereClause,
                          std::shared_ptr<query::GroupByClause> const& groupByClause,
                          std::shared_ptr<query::HavingClause> const& havingClause) override {
        _fromList = fromList;
        _whereClause = whereClause;
        _groupByClause = groupByClause;
        _havingClause = havingClause;
    }

    void handleOrderByClause(std::shared_ptr<query::OrderByClause> const& orderByClause) override {
        _orderByClause = orderByClause;
    }

    void handleLimitClause(int limit) override {
        _limit = limit;
    }

    void handleSelectSpec(bool distinct) override {
        _distinct = distinct;
    }

    void checkContext() const override {
        // required:
        assert_execution_condition(_ctx->SELECT() != nullptr, "Context check failure.", _ctx);
    }

    void onExit() override {
        lockedParent()->handleQuerySpecification(_selectList, _fromList, _whereClause, _orderByClause,
                _limit, _groupByClause, _havingClause, _distinct);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::WhereClause> _whereClause;
    std::shared_ptr<query::FromList> _fromList;
    std::shared_ptr<query::SelectList> _selectList;
    std::shared_ptr<query::OrderByClause> _orderByClause;
    std::shared_ptr<query::GroupByClause> _groupByClause;
    std::shared_ptr<query::HavingClause> _havingClause;
    int _limit{lsst::qserv::NOTSET};
    bool _distinct{false};
};


class SelectElementsAdapter :
        public AdapterT<SelectElementsCBH, QSMySqlParser::SelectElementsContext>,
        public SelectColumnElementCBH,
        public SelectFunctionElementCBH,
        public SelectStarElementCBH,
        public SelectExpressionElementCBH {
public:
    using AdapterT::AdapterT;

    void onEnter() override {
        if (_ctx->star != nullptr) {
            _addStarFactor();
        }
    }

    void handleColumnElement(std::shared_ptr<query::ValueExpr> const& columnElement) override {
        _addValueExpr(columnElement);
    }

    void handleSelectFunctionElement(std::shared_ptr<query::ValueExpr> const& selectFunction) override {
        _addSelectAggFunction(selectFunction);
    }

    void handleSelectStarElement(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        _addValueExpr(valueExpr);
    }

    void handleSelectExpressionElement(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        _addValueExpr(valueExpr);
    }

    void checkContext() const override {
        // optional:
        // star
    }

    void onExit() override {
        lockedParent()->handleSelectList(_selectList);
    }

    std::string name() const override { return getTypeName(this); }

private:
    void _addValueExpr(std::shared_ptr<query::ValueExpr> const& valueExpr) const {
        _selectList->addValueExpr(valueExpr);
    }

    void _addStarFactor() const {
        _selectList->addValueExpr(query::ValueExpr::newSimple(
                query::ValueFactor::newStarFactor("")));
    }

    void _addSelectAggFunction(std::shared_ptr<query::ValueExpr> const& func) const {
        _selectList->addValueExpr(func);
    }

    std::shared_ptr<query::SelectList> _selectList{std::make_shared<query::SelectList>()};
};


class FromClauseAdapter :
        public AdapterT<FromClauseCBH, QSMySqlParser::FromClauseContext>,
        public TableSourcesCBH,
        public PredicateExpressionCBH,
        public LogicalExpressionCBH,
        public QservFunctionSpecCBH,
        public GroupByItemCBH,
        public NotExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handleTableSources(std::shared_ptr<std::vector<std::shared_ptr<query::TableRef>>> const& tableRefList) override {
        _tableRefList = tableRefList;
    }

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        _addBoolTerm(boolTerm, childCtx);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        assert_execution_condition(false, "Unhandled valueExpr predicateExpression.", _ctx);
    }

    void handleLogicalExpression(std::shared_ptr<query::LogicalTerm> const& logicalTerm,
            antlr4::ParserRuleContext* childCtx) override {
        trace_callback_info(__FUNCTION__, logicalTerm);
        if (_ctx->whereExpr == childCtx) {
            auto whereClause = _getWhereClause();
            assert_execution_condition(nullptr == whereClause->getRootTerm(),
                    "expected handleLogicalExpression to be called only once.", _ctx);
            whereClause->setRootTerm(logicalTerm);
        } else if (_ctx->havingExpr == childCtx) {
            assert_execution_condition(false,
                    "The having expression is expected to be handled as a Predicate Expression.", _ctx);
        } else {
            assert_execution_condition(false, "This logical expression is not yet supported.", _ctx);
        }
    }

    void handleQservFunctionSpec(std::string const& functionName,
            std::vector<std::shared_ptr<query::ValueFactor>> const& args) override {
        _addQservRestrictor(functionName, args);
    }

    void handleGroupByItem(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        if (nullptr == _groupByClause) {
            _groupByClause = std::make_shared<query::GroupByClause>();
        }
        _groupByClause->addTerm(query::GroupByTerm(valueExpr, ""));
    }

    void handleNotExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        _addBoolTerm(boolTerm, childCtx);
    }

    void checkContext() const override {
        // required:
        assert_execution_condition(_ctx->FROM() != nullptr, "Context check failure.", _ctx);
        // optional:
        // *WHERE();
        // *GROUP();
        // *BY();
        // *HAVING();
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->WITH() == nullptr, "WITH is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->ROLLUP() == nullptr, "ROLLUP is not supported", _ctx);
    }

    void onExit() override {
        std::shared_ptr<query::FromList> fromList = std::make_shared<query::FromList>(_tableRefList);
        lockedParent()->handleFromClause(fromList, _whereClause, _groupByClause, _havingClause);
    }

    std::string name() const override { return getTypeName(this); }

private:
    void _addQservRestrictor(const std::string& function,
                             const std::vector<std::shared_ptr<query::ValueFactor>>& parameters) {
        // Here we extract the args from a vector of ValueFactor::ColumnRef
        // This is a side effect of the current IR, where in most cases a constant string is represented as
        // a column name. But in a QservRestrictor (aka QservFunction) each par is simply represented by a
        // string.
        std::vector<std::string> strParameters;
        for (auto const& valueFactor : parameters) {
            if (query::ValueFactor::CONST != valueFactor->getType()) {
                throw std::logic_error("QServFunctionSpec args are (currently) expected as constVal.");
            }
            strParameters.push_back(valueFactor->getConstVal());
        }

        std::shared_ptr<query::AreaRestrictor> restrictor;
        try {
            if (boost::algorithm::iequals("qserv_areaspec_box", function)) {
                restrictor = std::make_shared<query::AreaRestrictorBox>(strParameters);
            } else if (boost::algorithm::iequals("qserv_areaspec_circle", function)) {
                restrictor = std::make_shared<query::AreaRestrictorCircle>(strParameters);
            } else if (boost::algorithm::iequals("qserv_areaspec_ellipse", function)) {
                restrictor = std::make_shared<query::AreaRestrictorEllipse>(strParameters);
            } else if (boost::algorithm::iequals("qserv_areaspec_poly", function)) {
                restrictor = std::make_shared<query::AreaRestrictorPoly>(strParameters);
            } else {
                throw parser::adapter_execution_error("Unhandled restrictor function: " + function);
            }
        } catch (std::logic_error const& err) {
            throw parser::adapter_execution_error(err.what());
        }
        _getWhereClause()->addAreaRestrictor(restrictor);
    }


    void _addBoolTerm(std::shared_ptr<query::BoolTerm> const& boolTerm, antlr4::ParserRuleContext* childCtx) {
        if (_ctx->whereExpr == childCtx) {
            auto andTerm = std::make_shared<query::AndTerm>(boolTerm);
            auto rootTerm = std::dynamic_pointer_cast<query::LogicalTerm>(_getWhereClause()->getRootTerm());
            if (nullptr == rootTerm) {
                rootTerm = std::make_shared<query::OrTerm>();
                _getWhereClause()->setRootTerm(rootTerm);
            }
            rootTerm->addBoolTerm(andTerm);
        } else if (_ctx->havingExpr == childCtx) {
            assert_execution_condition(nullptr == _havingClause, "The having clause should only be set once.", _ctx);
            auto andTerm = std::make_shared<query::AndTerm>(boolTerm);
            auto orTerm = std::make_shared<query::OrTerm>(andTerm);
            _havingClause = std::make_shared<query::HavingClause>(orTerm);
        } else {
            assert_execution_condition(false, "This predicate expression is not yet supported.", _ctx);
        }
    }

    std::shared_ptr<query::WhereClause> & _getWhereClause() {
        if (nullptr == _whereClause) {
            _whereClause = std::make_shared<query::WhereClause>();
        }
        return _whereClause;
    }

    // I think the first term of a where clause is always an OrTerm, and it needs to be added by default.
    std::shared_ptr<query::WhereClause> _whereClause;
    std::shared_ptr<std::vector<std::shared_ptr<query::TableRef>>> _tableRefList;
    std::shared_ptr<query::GroupByClause> _groupByClause;
    std::shared_ptr<query::HavingClause> _havingClause;
};


class TableSourcesAdapter :
        public AdapterT<TableSourcesCBH, QSMySqlParser::TableSourcesContext>,
        public TableSourceBaseCBH {
public:
    using AdapterT::AdapterT;

    void handleTableSource(std::shared_ptr<query::TableRef> const& tableRef) override {
        _tableRefList->push_back(tableRef);
    }

    void checkContext() const override {
        // nothing to check.
    }

    void onExit() override {
        lockedParent()->handleTableSources(_tableRefList);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<std::vector<std::shared_ptr<query::TableRef>>> _tableRefList{std::make_shared<query::TableRefList>()};
};


class TableSourceBaseAdapter :
        public AdapterT<TableSourceBaseCBH, QSMySqlParser::TableSourceBaseContext>,
        public AtomTableItemCBH,
        public InnerJoinCBH,
        public NaturalJoinCBH {
public:
    using AdapterT::AdapterT;

    void handleAtomTableItem(std::shared_ptr<query::TableRef> const& tableRef) override {
        assert_execution_condition(nullptr == _tableRef, "expeceted one AtomTableItem callback.", _ctx);
        _tableRef = tableRef;
    }

    void handleInnerJoin(std::shared_ptr<query::JoinRef> const& joinRef) override {
        _joinRefs.push_back(joinRef);
    }

    void handleNaturalJoin(std::shared_ptr<query::JoinRef> const& joinRef) override {
        _joinRefs.push_back(joinRef);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        assert_execution_condition(_tableRef != nullptr, "tableRef was not populated.", _ctx);
        _tableRef->addJoins(_joinRefs);
        lockedParent()->handleTableSource(_tableRef);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::TableRef> _tableRef;
    std::vector<std::shared_ptr<query::JoinRef>> _joinRefs;
};


class AtomTableItemAdapter :
        public AdapterT<AtomTableItemCBH, QSMySqlParser::AtomTableItemContext>,
        public TableNameCBH,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    void handleTableName(std::vector<std::string> const& uidlist) override {
        if (uidlist.size() == 1) {
            _table = uidlist.at(0);
        } else if (uidlist.size() == 2) {
            _db = uidlist.at(0);
            _table = uidlist.at(1);
        } else {
            assert_execution_condition(false, "Illegal number of UIDs in table reference.", _ctx);
        }
    }

    void handleUid(std::string const& uidString) override {
        _alias = uidString;
    }

    void checkContext() const override {
        // optional:
        // AS();
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->PARTITION() == nullptr, "PARTITION is not supported", _ctx);
    }

    void onExit() override {
        std::shared_ptr<query::TableRef> tableRef = std::make_shared<query::TableRef>(_db, _table, _alias);
        lockedParent()->handleAtomTableItem(tableRef);
    }

    std::string name() const override { return getTypeName(this); }

protected:
    std::string _db;
    std::string _table;
    std::string _alias;
};


class TableNameAdapter :
        public AdapterT<TableNameCBH, QSMySqlParser::TableNameContext>,
        public FullIdCBH {
public:
    using AdapterT::AdapterT;

    void handleFullId(std::vector<std::string> const& uidlist) override {
        lockedParent()->handleTableName(uidlist);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {}

    std::string name() const override { return getTypeName(this); }
};


class FullIdAdapter :
        public AdapterT<FullIdCBH, QSMySqlParser::FullIdContext>,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    virtual ~FullIdAdapter() {}

    void handleUid(std::string const& str) override {
        _uidlist.push_back(str);
        if (_ctx && _ctx->DOT_ID()) {
            std::string s = _ctx->DOT_ID()->getText();
            if (s.front() == '.') _uidlist.push_back(s.erase(0,1));
            else _uidlist.push_back(s);
        }
    }

    void checkContext() const override {
        // optional:
        // DOT_ID();
    }

    void onExit() override {
        lockedParent()->handleFullId(_uidlist);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::vector<std::string> _uidlist;
};


class FullColumnNameAdapter :
        public AdapterT<FullColumnNameCBH, QSMySqlParser::FullColumnNameContext>,
        public UidCBH,
        public DottedIdCBH {
public:
    using AdapterT::AdapterT;

    void handleUid(std::string const& uidString) override {
        _strings.push_back(uidString);
    }

    void handleDottedId(std::string const& dot_id) override {
        _strings.push_back(dot_id);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        std::shared_ptr<query::ColumnRef> columnRef;
        switch(_strings.size()) {
        case 1:
            // only 1 value is set in strings; it is the column name.
            columnRef = std::make_shared<query::ColumnRef>("", "", _strings[0]);
            break;
        case 2:
            // 2 values are set in strings; they are table and column name.
            columnRef = std::make_shared<query::ColumnRef>("", _strings[0], _strings[1]);
            break;
        case 3:
            // 3 values are set in strings; they are database name, table name, and column name.
            columnRef = std::make_shared<query::ColumnRef>(_strings[0], _strings[1], _strings[2]);
            break;
        default:
            assert_execution_condition(false, "Unhandled number of strings.", _ctx);
        }
        auto valueFactor = query::ValueFactor::newColumnRefFactor(columnRef);
        lockedParent()->handleFullColumnName(valueFactor);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::vector<std::string> _strings;
};


class ConstantExpressionAtomAdapter :
        public AdapterT<ConstantExpressionAtomCBH, QSMySqlParser::ConstantExpressionAtomContext>,
        public ConstantCBH {
public:
    using AdapterT::AdapterT;

    void handleConstant(std::string const& val) override {
        lockedParent()->handleConstantExpressionAtom(query::ValueFactor::newConstFactor(val));
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {}

    std::string name() const override { return getTypeName(this); }
};



class FullColumnNameExpressionAtomAdapter :
        public AdapterT<FullColumnNameExpressionAtomCBH, QSMySqlParser::FullColumnNameExpressionAtomContext>,
        public FullColumnNameCBH {
public:
    using AdapterT::AdapterT;

    void handleFullColumnName(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        lockedParent()->HandleFullColumnNameExpressionAtom(valueFactor);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {}

    std::string name() const override { return getTypeName(this); }
};



class ExpressionAtomPredicateAdapter :
        public AdapterT<ExpressionAtomPredicateCBH, QSMySqlParser::ExpressionAtomPredicateContext>,
        public ConstantExpressionAtomCBH,
        public FullColumnNameExpressionAtomCBH,
        public FunctionCallExpressionAtomCBH,
        public NestedExpressionAtomCBH,
        public MathExpressionAtomCBH,
        public BitExpressionAtomCBH {
public:
    using AdapterT::AdapterT;

    void handleConstantExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        auto valueExpr = query::ValueExpr::newSimple(valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleFunctionCallExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->addValueFactor(valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleMathExpressionAtom(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void HandleFullColumnNameExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->addValueFactor(valueFactor);
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleNestedExpressionAtom(std::shared_ptr<query::BoolTerm> const& boolTerm) override {
        trace_callback_info(__FUNCTION__, *boolTerm);
        lockedParent()->handleExpressionAtomPredicate(boolTerm, _ctx);
    }

    void handleNestedExpressionAtom(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void handleBitExpressionAtom(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        lockedParent()->handleExpressionAtomPredicate(valueExpr, _ctx);
    }

    void checkContext() const override {
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->LOCAL_ID() == nullptr, "LOCAL_ID is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->VAR_ASSIGN() == nullptr, "VAR_ASSIGN is not supported", _ctx);
    }

    void onExit() override {}

    std::string name() const override { return getTypeName(this); }
};


class QservFunctionSpecAdapter :
        public AdapterT<QservFunctionSpecCBH, QSMySqlParser::QservFunctionSpecContext>,
        public ConstantsCBH {
public:
    using AdapterT::AdapterT;

    void handleConstants(std::vector<std::string> const& values) override {
        assert_execution_condition(_args.empty(), "args should be set exactly once.", _ctx);
        for (auto&& value : values) {
            _args.push_back(query::ValueFactor::newConstFactor(value));
        }
    }

    void checkContext() const override {
        // required:
        assert_execution_condition(_ctx->QSERV_AREASPEC_BOX() != nullptr ||
            _ctx->QSERV_AREASPEC_CIRCLE() != nullptr ||
            _ctx->QSERV_AREASPEC_ELLIPSE() != nullptr ||
            _ctx->QSERV_AREASPEC_POLY() != nullptr ||
            _ctx->QSERV_AREASPEC_HULL() != nullptr, "Context check failure.", _ctx);
    }

    void onExit() override {
        lockedParent()->handleQservFunctionSpec(getFunctionName(), _args);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::string getFunctionName() {
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
        assert_execution_condition(false, "could not get qserv function name.", _ctx);
        return ""; // prevent warning: "control reaches end of non-void function"
    }

    std::vector<std::shared_ptr<query::ValueFactor>> _args;
};


// PredicateExpressionAdapter gathers BoolFactors into a BoolFactor (which is a BoolTerm).
class PredicateExpressionAdapter :
        public AdapterT<PredicateExpressionCBH, QSMySqlParser::PredicateExpressionContext>,
        public BinaryComparasionPredicateCBH,
        public BetweenPredicateCBH,
        public InPredicateCBH,
        public ExpressionAtomPredicateCBH,
        public LikePredicateCBH,
        public IsNullPredicateCBH {
public:
    using AdapterT::AdapterT;

    // BinaryComparasionPredicateCBH
    void handleBinaryComparasionPredicate(
            std::shared_ptr<query::CompPredicate> const& comparisonPredicate) override {
        _boolFactorInstance()->addBoolFactorTerm(comparisonPredicate);
    }

    void handleBetweenPredicate(std::shared_ptr<query::BetweenPredicate> const& betweenPredicate) override {
        _boolFactorInstance()->addBoolFactorTerm(betweenPredicate);
    }

    void handleInPredicate(std::shared_ptr<query::InPredicate> const& inPredicate) override {
        _boolFactorInstance()->addBoolFactorTerm(inPredicate);
    }

    void handleExpressionAtomPredicate(std::shared_ptr<query::ValueExpr> const& valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        trace_callback_info(__FUNCTION__, valueExpr);
        _prepValueExpr();
        _valueExpr = valueExpr;
    }

    void handleExpressionAtomPredicate(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        trace_callback_info(__FUNCTION__, boolTerm);
        assert_execution_condition(nullptr == _boolTerm && nullptr == _valueExpr, "unexpected", _ctx);
        _boolTerm = boolTerm;
    }

    void handleLikePredicate(std::shared_ptr<query::LikePredicate> const& likePredicate) override {
        _boolFactorInstance()->addBoolFactorTerm(likePredicate);
    }

    void handleIsNullPredicate(std::shared_ptr<query::NullPredicate> const& nullPredicate) override {
        _boolFactorInstance()->addBoolFactorTerm(nullPredicate);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        assert_execution_condition(nullptr != _valueExpr || nullptr != _boolTerm,
                "PredicateExpressionAdapter was not populated.", _ctx);
        if (_boolTerm != nullptr) {
            lockedParent()->handlePredicateExpression(_boolTerm, _ctx);
        } else {
            lockedParent()->handlePredicateExpression(_valueExpr);
        }
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::BoolFactor> _boolFactorInstance() {
        assert_execution_condition(nullptr == _valueExpr,
                "Can't use PredicateExpressionAdapter for BoolFactor and ValueExpr at the same time.", _ctx);
        if (nullptr == _boolTerm) {
            auto boolFactor = std::make_shared<query::BoolFactor>();
            _boolTerm = boolFactor;
            return boolFactor;
        }
        auto boolFactor = std::dynamic_pointer_cast<query::BoolFactor>(_boolTerm);
        assert_execution_condition(nullptr != boolFactor, "Can't cast boolTerm to a BoolFactor.", _ctx);
        return boolFactor;
    }

    void _prepValueExpr() {
        assert_execution_condition(nullptr == _boolTerm,
                "Can't use PredicateExpressionAdapter for BoolFactor and ValueExpr at the same time.", _ctx);
        assert_execution_condition(nullptr == _valueExpr, "Can only set _valueExpr once.", _ctx);
    }

    std::shared_ptr<query::BoolTerm> _boolTerm;
    std::shared_ptr<query::ValueExpr> _valueExpr;
};


class BinaryComparasionPredicateAdapter :
        public AdapterT<BinaryComparasionPredicateCBH, QSMySqlParser::BinaryComparasionPredicateContext>,
        public ExpressionAtomPredicateCBH,
        public ComparisonOperatorCBH {
public:
    using AdapterT::AdapterT;

    void handleComparisonOperator(std::string const& text) override {
        assert_execution_condition(_comparison.empty(), "comparison must be set only once.", _ctx);
        _comparison = text;
    }

    void handleExpressionAtomPredicate(std::shared_ptr<query::ValueExpr> const& valueExpr,
            antlr4::ParserRuleContext* ctx) override {
        if (_left == nullptr) {
            _left = valueExpr;
        } else if (_right == nullptr) {
            _right = valueExpr;
        } else {
            assert_execution_condition(false, "left and right values must be set only once.", _ctx);
        }
    }

    void handleExpressionAtomPredicate(std::shared_ptr<query::BoolTerm> const& boolFactor,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "unhandled ExpressionAtomPredicate BoolTerm callback.", _ctx);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        assert_execution_condition(_left != nullptr && _right != nullptr,
                "left and right values must both be populated", _ctx);

        auto compPredicate = std::make_shared<query::CompPredicate>();
        compPredicate->left = _left;

        if ("=" == _comparison) {
            compPredicate->op = query::CompPredicate::EQUALS_OP;
        } else if (">" == _comparison) {
            compPredicate->op = query::CompPredicate::GREATER_THAN_OP;
        } else if ("<" == _comparison) {
            compPredicate->op = query::CompPredicate::LESS_THAN_OP;
        } else if ("<>" == _comparison) {
            compPredicate->op = query::CompPredicate::NOT_EQUALS_OP;
        } else if ("!=" == _comparison) {
            compPredicate->op = query::CompPredicate::NOT_EQUALS_OP_ALT;
        } else if ("<=>" == _comparison) {
            compPredicate->op = query::CompPredicate::NULL_SAFE_EQUALS_OP;
        } else if ("<=" == _comparison) {
            compPredicate->op = query::CompPredicate::LESS_THAN_OR_EQUALS_OP;
        } else if (">=" == _comparison) {
            compPredicate->op = query::CompPredicate::GREATER_THAN_OR_EQUALS_OP;
        } else {
            assert_execution_condition(false, "unhandled comparison operator type:" + _comparison, _ctx);
        }

        compPredicate->right = _right;

        lockedParent()->handleBinaryComparasionPredicate(compPredicate);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> _left;
    std::string _comparison;
    std::shared_ptr<query::ValueExpr> _right;
};


class ComparisonOperatorAdapter :
        public AdapterT<ComparisonOperatorCBH, QSMySqlParser::ComparisonOperatorContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        static const char* supportedOps[] {"=", "<", ">", "<>", "!=", ">=", "<=", "<=>"};
        if (std::end(supportedOps) == find(std::begin(supportedOps), std::end(supportedOps), _ctx->getText())) {
            assertNotSupported(__FUNCTION__, false,
                    "Unsupported comparison operator: " + _ctx->getText(), _ctx);
        }
    }

    void onExit() override {
        lockedParent()->handleComparisonOperator(_ctx->getText());
    }

    std::string name() const override { return getTypeName(this); }
};


class CallStatementAdapter :
        public AdapterT<CallStatementCBH, QSMySqlParser::CallStatementContext>,
        public ConstantCBH {
public:
    using AdapterT::AdapterT;

    void handleConstant(std::string const& val) override {
        _value = val;
    }

    void checkContext() const override {
        assert_execution_condition(_ctx->QSERV_MANAGER() != nullptr, "Only CALL QSERV_MANAGER is supported.", _ctx);
    }

    void onExit() override {
        assert_execution_condition(getQueryResources() != nullptr, "UserQueryQservManager requires a valid query config.", _ctx);
        lockedParent()->handleCallStatement(std::make_shared<ccontrol::UserQueryQservManager>(getQueryResources(), _value));
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::string _value;
};


class OrderByClauseAdapter :
        public AdapterT<OrderByClauseCBH, QSMySqlParser::OrderByClauseContext>,
        public OrderByExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handleOrderByExpression(query::OrderByTerm const& orderByTerm) override {
        _orderByClause->addTerm(orderByTerm);
    }

    void checkContext() const override {
        // required:
        assert_execution_condition(_ctx->ORDER() != nullptr, "Context check failure.", _ctx);
        assert_execution_condition(_ctx->BY() != nullptr, "Context check failure.", _ctx);
    }

    void onExit() override {
        lockedParent()->handleOrderByClause(_orderByClause);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::OrderByClause> _orderByClause { std::make_shared<query::OrderByClause>() };
};


class OrderByExpressionAdapter :
        public AdapterT<OrderByExpressionCBH, QSMySqlParser::OrderByExpressionContext>,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void onEnter() override {
        if (_ctx->ASC() == nullptr && _ctx->DESC() != nullptr) {
            _orderBy = query::OrderByTerm::DESC;
        } else if (_ctx->ASC() != nullptr && _ctx->DESC() == nullptr) {
            _orderBy = query::OrderByTerm::ASC;
        } else if (_ctx->ASC() != nullptr && _ctx->DESC() != nullptr) {
            assert_execution_condition(false, "having both ASC and DESC is unhandled.", _ctx);
        }
        // note that query::OrderByTerm::DEFAULT is the default value of _orderBy
    }

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "unexpected BoolFactor callback", _ctx);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        assert_execution_condition(nullptr == _valueExpr, "expected exactly one ValueExpr callback", _ctx);
        assertNotSupported(__FUNCTION__, valueExpr->isFunction() == false,
                "qserv does not support functions in ORDER BY.", _ctx);
        _valueExpr = valueExpr;
    }

    void checkContext() const override {
        // optional:
        // order;
        // ASC();
        // DESC();
    }

    void onExit() override {
        query::OrderByTerm orderByTerm(_valueExpr, _orderBy, "");
        lockedParent()->handleOrderByExpression(orderByTerm);
    }

    std::string name() const override { return getTypeName(this); }

private:
    query::OrderByTerm::Order _orderBy {query::OrderByTerm::DEFAULT};
    std::shared_ptr<query::ValueExpr> _valueExpr;
};


class InnerJoinAdapter :
        public AdapterT<InnerJoinCBH, QSMySqlParser::InnerJoinContext>,
        public AtomTableItemCBH,
        public UidListCBH,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handleAtomTableItem(std::shared_ptr<query::TableRef> const& tableRef) override {
        trace_callback_info(__FUNCTION__, *tableRef);
        assert_execution_condition(nullptr == _tableRef, "expected only one atomTableItem callback.", _ctx);
        _tableRef = tableRef;
    }

    void handleUidList(std::vector<std::string> const& strings) override {
        trace_callback_info(__FUNCTION__, util::printable(strings));
        assert_execution_condition(strings.size() == 1,
            "Current intermediate representation can only handle 1 `using` std::string.", _ctx);
        assert_execution_condition(nullptr == _using, "_using should be set exactly once.", _ctx);
        _using = std::make_shared<query::ColumnRef>("", "", strings[0]);
    }

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        trace_callback_info(__FUNCTION__, *boolTerm);
        assert_execution_condition(nullptr == _on, "Unexpected second BoolTerm callback.", _ctx);
        _on = _getNestedBoolTerm(boolTerm);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        assert_execution_condition(false, "Unexpected PredicateExpression ValueExpr callback.", _ctx);
    }

    void checkContext() const override {
        // required:
        assert_execution_condition(_ctx->JOIN() != nullptr, "Context check failure.", _ctx);
        // optional:
        // INNER();
        // CROSS();
        // ON();
        // USING();
    }

    void onExit() override {
        assert_execution_condition(_tableRef != nullptr, "TableRef was not set.", _ctx);
        query::JoinRef::Type joinType(query::JoinRef::DEFAULT);
        if (_ctx->INNER() != nullptr) {
            joinType = query::JoinRef::INNER;
        } else if (_ctx->CROSS() != nullptr) {
            joinType = query::JoinRef::CROSS;
        }
        std::shared_ptr<query::JoinSpec> joinSpec;
        if (_using != nullptr || _on != nullptr) {
            joinSpec = std::make_shared<query::JoinSpec>(_using, _on);
        }
        auto joinRef = std::make_shared<query::JoinRef>(_tableRef, joinType, false, joinSpec);
        lockedParent()->handleInnerJoin(joinRef);
    }

    std::string name() const override { return getTypeName(this); }

private:

    // in a query such as
    // SELECT count(*) FROM   Object o
    //        INNER JOIN RefObjMatch o2t ON (o.objectIdObjTest = o2t.objectId)
    //        INNER JOIN SimRefObject t ON (o2t.refObjectId = t.refObjectId)
    //        WHERE  closestToObj = 1 OR closestToObj is NULL;,
    // When a BoolFactor is in parenthesis, the NestedExpressionAtomAdapter puts it in a new BoolFactor that
    // has an PassTerm with an open parenthesis, then an Or+And that contains the BoolFactor, and then
    // a PassTerm with close parenthesis. This is the correct IR for parenthesis (a "nestedExpression") in
    // the WHERE clause. However, our IR does NOT expect the BoolFactor to be put in this sort of structure
    // in the JOIN clause, so we must extract the contained BoolFactor in this case. This function does that.
    std::shared_ptr<query::BoolTerm> _getNestedBoolTerm(std::shared_ptr<query::BoolTerm> const& boolTerm) {
        auto boolFactor = std::dynamic_pointer_cast<query::BoolFactor>(boolTerm);
        if (nullptr == boolFactor) {
            return boolTerm;
        }
        if (boolFactor->_terms.size() != 3) {
            return boolFactor;
        }
        auto lhsPassTerm = std::dynamic_pointer_cast<query::PassTerm>(boolFactor->_terms[0]);
        if (nullptr == lhsPassTerm || lhsPassTerm->_text != "(") {
            return boolFactor;
        }
        auto rhsPassTerm = std::dynamic_pointer_cast<query::PassTerm>(boolFactor->_terms[2]);
        if (nullptr == rhsPassTerm || rhsPassTerm->_text != ")") {
            return boolFactor;
        }
        auto boolTermFactor = std::dynamic_pointer_cast<query::BoolTermFactor>(boolFactor->_terms[1]);
        if (nullptr == boolTermFactor) {
            return boolFactor;
        }
        auto orTerm = std::dynamic_pointer_cast<query::OrTerm>(boolTermFactor->_term);
        if (nullptr == orTerm) {
            return boolFactor;
        }
        if (orTerm->_terms.size() != 1) {
            return boolFactor;
        }
        auto andTerm = std::dynamic_pointer_cast<query::AndTerm>(orTerm->_terms[0]);
        if (nullptr == andTerm) {
            return boolFactor;
        }
        if (andTerm->_terms.size() != 1) {
            return boolFactor;
        }
        return andTerm->_terms[0];
    }

    std::shared_ptr<query::ColumnRef> _using;
    std::shared_ptr<query::TableRef> _tableRef;
    std::shared_ptr<query::BoolTerm> _on;
};


class NaturalJoinAdapter :
        public AdapterT<NaturalJoinCBH, QSMySqlParser::NaturalJoinContext>,
        public AtomTableItemCBH {
public:
    using AdapterT::AdapterT;

    void handleAtomTableItem(std::shared_ptr<query::TableRef> const& tableRef) override {
        assert_execution_condition(nullptr == _tableRef, "expected only one atomTableItem callback.", _ctx);
        _tableRef = tableRef;
    }

    void checkContext() const override {
        // required:
        assert_execution_condition(_ctx->NATURAL() != nullptr, "Context check failure.", _ctx);
        assert_execution_condition(_ctx->JOIN() != nullptr, "Context check failure.", _ctx);
        // optional:
        // LEFT();
        // RIGHT();
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->OUTER() == nullptr, "OUTER join is not handled.", _ctx);
    }

    void onExit() override {
        assert_execution_condition(_tableRef != nullptr, "TableRef was not set.", _ctx);
        query::JoinRef::Type joinType(query::JoinRef::DEFAULT);
        if (_ctx->LEFT() != nullptr) {
            joinType = query::JoinRef::LEFT;
        } else if (_ctx->RIGHT() != nullptr) {
            joinType = query::JoinRef::RIGHT;
        }
        auto joinRef = std::make_shared<query::JoinRef>(_tableRef, joinType, true, nullptr);
        lockedParent()->handleNaturalJoin(joinRef);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::TableRef> _tableRef;
};


class SelectSpecAdapter :
        public AdapterT<SelectSpecCBH, QSMySqlParser::SelectSpecContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        // optional:
        // DISTINCT();
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->ALL() == nullptr,
                "ALL is not supported.", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->DISTINCTROW() == nullptr,
                "DISTINCTROW is not supported.", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->HIGH_PRIORITY() == nullptr,
                "HIGH_PRIORITY", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->STRAIGHT_JOIN() == nullptr,
                "STRAIGHT_JOIN is not supported.", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SQL_SMALL_RESULT() == nullptr,
                "SQL_SMALL_RESULT is not supported.", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SQL_BIG_RESULT() == nullptr,
                "SQL_BIG_RESULT is not supported.", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SQL_BUFFER_RESULT() == nullptr,
                "SQL_BUFFER_RESULT is not supported.", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SQL_CACHE() == nullptr,
                "SQL_CACHE", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SQL_NO_CACHE() == nullptr,
                "SQL_NO_CACHE is not supported.", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SQL_CALC_FOUND_ROWS() == nullptr,
                "SQL_CALC_FOUND_ROWS is not supported.", _ctx);
    }

    void onExit() override {
        lockedParent()->handleSelectSpec(_ctx->DISTINCT() != nullptr);
    }

    std::string name() const override { return getTypeName(this); }
};


class SelectStarElementAdapter :
        public AdapterT<SelectStarElementCBH, QSMySqlParser::SelectStarElementContext>,
        public FullIdCBH {
public:
    using AdapterT::AdapterT;

    void handleFullId(std::vector<std::string> const& uidlist) override {
        assert_execution_condition(nullptr == _valueExpr, "_valueExpr should only be set once.", _ctx);
        assert_execution_condition(uidlist.size() == 1, "Star Elements must be 'tableName.*'", _ctx);
        _valueExpr = std::make_shared<query::ValueExpr>();
        _valueExpr->addValueFactor(query::ValueFactor::newStarFactor(uidlist[0]));
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        lockedParent()->handleSelectStarElement(_valueExpr);
    }

    std::string name() const override { return getTypeName(this); }
private:
    std::shared_ptr<query::ValueExpr> _valueExpr;
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

    void handleUid(std::string const& uidString) override {
        // Uid is expected to be the aliasName in `functionCall AS aliasName` or `functionCall aliasName`
        assert_execution_condition(_asName.empty(), "Second call to handleUid.", _ctx);
        _asName = uidString;
    }

    void handleAggregateFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        assert_execution_condition(nullptr == _functionValueFactor, "should only be called once.",
                _ctx);
        _functionValueFactor = valueFactor;
    }

    void handleUdfFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        assert_execution_condition(nullptr == _functionValueFactor, "should only be set once.",
                _ctx);
        _functionValueFactor = valueFactor;
    }

    void handleScalarFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        assert_execution_condition(nullptr == _functionValueFactor, "should only be set once.",
                _ctx);
        _functionValueFactor = valueFactor;
    }

    void checkContext() const override {
        // optional:
        // AS();
    }

    void onExit() override {
        assert_execution_condition(nullptr != _functionValueFactor,
                "function value factor not populated.", _ctx);
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->addValueFactor(_functionValueFactor);
        valueExpr->setAlias(_asName);
        lockedParent()->handleSelectFunctionElement(valueExpr);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::string _asName;
    std::shared_ptr<query::ValueFactor> _functionValueFactor;
};


class SelectExpressionElementAdapter :
        public AdapterT<SelectExpressionElementCBH, QSMySqlParser::SelectExpressionElementContext>,
        public PredicateExpressionCBH, public UidCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "unexpected call to handlePredicateExpression(BoolTerm).", _ctx);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        assert_execution_condition(nullptr == _valueExpr, "valueExpr must be set only once in SelectExpressionElementAdapter.", _ctx);
        _valueExpr = valueExpr;
    }

    void handleUid(std::string const& uidString) override {
        _alias = uidString;
    }

    void checkContext() const override {
        // optional:
        // AS();
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->LOCAL_ID() == nullptr, "LOCAL_ID is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->VAR_ASSIGN() == nullptr, "VAR_ASSIGN is not supported", _ctx);
    }

    void onExit() override {
        assert_execution_condition(nullptr != _valueExpr, "valueExpr must be set in SelectExpressionElementAdapter.", _ctx);
        if (_valueExpr != nullptr && not _alias.empty()) _valueExpr->setAlias(_alias);
        lockedParent()->handleSelectExpressionElement(_valueExpr);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> _valueExpr;
    std::string _alias;
};


class GroupByItemAdapter :
        public AdapterT<GroupByItemCBH, QSMySqlParser::GroupByItemContext>,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "Unexpected PredicateExpression BoolTerm callback.", _ctx);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        _valueExpr = valueExpr;
    }

    void checkContext() const override {
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->ASC() == nullptr, "ASC is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->DESC() == nullptr, "DESC is not supported", _ctx);
    }

    void onExit() override {
        assert_execution_condition(_valueExpr != nullptr, "GroupByItemAdapter not populated.", _ctx);
        lockedParent()->handleGroupByItem(_valueExpr);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> _valueExpr;
};


class LimitClauseAdapter :
        public AdapterT<LimitClauseCBH, QSMySqlParser::LimitClauseContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        // we use checkContext here to verify that `limit` is set and `offset` is not set. Since they both
        // have decimalLiteral values and we ignore DecimalLiteral (and just extract the value directly where
        // it is used (at least for now), we verify that the size of the decimalLiteral vector is exactly
        // one.

        // required:
        assert_execution_condition(_ctx->LIMIT() != nullptr, "Context check failure.", _ctx);
        assert_execution_condition(_ctx->limit != nullptr, "Context check failure.", _ctx);
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->offset == nullptr && _ctx->OFFSET() == nullptr,
                "offset is not supported", _ctx);
    }

    void onExit() override {
        assert_execution_condition(_ctx->limit != nullptr,
                "Could not get a decimalLiteral context to read limit.", _ctx);
        lockedParent()->handleLimitClause(atoi(_ctx->limit->getText().c_str()));
    }

    std::string name() const override { return getTypeName(this); }
};



class SimpleIdAdapter :
        public AdapterT<SimpleIdCBH, QSMySqlParser::SimpleIdContext>,
        public FunctionNameBaseCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionNameBase(std::string const& name) override {
        // for all callbacks to SimpleIdAdapter are dropped and the value is fetched from the text value
        // of the context on exit.
    }

    void checkContext() const override {
        // since we expect either a basic ID, a function name, or a keyword as ID, reject anything else.
        if (_ctx->ID() != nullptr) {
            return;
        } else if (_ctx->functionNameBase() != nullptr) {
            return;
        } else if (_ctx->keywordsCanBeId() != nullptr) {
            LOGS(_log, LOG_LVL_WARN, __FUNCTION__ <<
                    " reusing keyword as ID: " << _ctx->getText());
            return;
        }
        assertNotSupported(__FUNCTION__, false, "Unsupported SimpleId", _ctx);
    }

    void onExit() override {
        lockedParent()->handleSimpleId(_ctx->getText());
    }

    std::string name() const override { return getTypeName(this); }
};


class DottedIdAdapter :
        public AdapterT<DottedIdCBH, QSMySqlParser::DottedIdContext>,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        assert_execution_condition((_ctx->DOT_ID() != nullptr) != (_ctx->uid() != nullptr),
                "Context check failure: exactly one of DOT_ID and uid should be non-null.", _ctx);
    }

    void handleUid(std::string const& uidString) override {
        _id = uidString;
    }

    void onExit() override {
        if (_id.empty()) {
            _id = _ctx->getText();
            assert_execution_condition(_id.find('.') == 0, "DOT_ID text is expected to start with a dot", _ctx);
            _id.erase(0, 1);
        }
        lockedParent()->handleDottedId(_id);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::string _id;
};


class NullNotnullAdapter :
        public AdapterT<NullNotnullCBH, QSMySqlParser::NullNotnullContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        // required:
        assert_execution_condition(
                _ctx->NULL_LITERAL() != nullptr || _ctx->NULL_SPEC_LITERAL() != nullptr,
                "Context check failure.", _ctx);
        // optional:
        // NOT();
    }

    void onExit() override {
        lockedParent()->handleNullNotnull(_ctx->NOT() != nullptr);
    }

    std::string name() const override { return getTypeName(this); }
};


class SelectColumnElementAdapter :
        public AdapterT<SelectColumnElementCBH, QSMySqlParser::SelectColumnElementContext>,
        public FullColumnNameCBH,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    void handleFullColumnName(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        assert_execution_condition(nullptr == _valueFactor,
                "handleFullColumnName should be called once.", _ctx);
        _valueFactor = valueFactor;
    }

    void handleUid(std::string const& uidString) override {
        assert_execution_condition(_alias.empty(), "handleUid should be called once.", _ctx);
        _alias = uidString;
    }

    void checkContext() const override {
        // optional:
        // AS();
    }

    void onExit() override {
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->addValueFactor(_valueFactor);
        valueExpr->setAlias(_alias);
        lockedParent()->handleColumnElement(valueExpr);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueFactor> _valueFactor;
    std::string _alias;
};


class UidAdapter :
        public AdapterT<UidCBH, QSMySqlParser::UidContext>,
        public SimpleIdCBH {
public:
    using AdapterT::AdapterT;

    void handleSimpleId(std::string const& val) override {
        _val = val;
    }

    void checkContext() const override {
        // onExit handles the variety of combinations of members of UidContext.
    }

    void onExit() override {
        // Fetching the string from a Uid shortcuts a large part of the syntax tree defined under Uid
        // (see QSMySqlParser.g4). If Adapters for any nodes in the tree below Uid are implemented then
        // it will have to be handled and this shortcut may not be taken.
        if (_val.empty()) {
            assert_execution_condition(_ctx->REVERSE_QUOTE_ID() != nullptr ||
                    _ctx->CHARSET_REVERSE_QOUTE_STRING() != nullptr,
                   "If value is not set by callback then one of the terminal nodes should be populated.",
                    _ctx);
            _val = _ctx->getText();
            assert_execution_condition((_val.find('`') == 0) && (_val.rfind('`') == _val.size()-1),
                    "REVERSE QUOTE values should begin and end with a backtick(`).", _ctx);
            _val.erase(_val.begin());
            _val.erase(--(_val.end()));
        }
        assertNotSupported(__FUNCTION__, _val.find('_') != 0,
                "Identifiers in Qserv may not start with an underscore.", _ctx);
        lockedParent()->handleUid(_val);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::string _val;
};


class ConstantAdapter :
        public AdapterT<ConstantCBH, QSMySqlParser::ConstantContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        // no context checking is done here; the text of the ID is fetched from the context and passed to the
        // handler.
    }

    void onExit() override {
        lockedParent()->handleConstant(_ctx->getText());
    }

    std::string name() const override { return getTypeName(this); }
};


class UidListAdapter :
        public AdapterT<UidListCBH, QSMySqlParser::UidListContext>,
        public UidCBH {
public:
    using AdapterT::AdapterT;

    void handleUid(std::string const& uidString) override {
        _strings.push_back(uidString);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        if (false == _strings.empty()) {
            lockedParent()->handleUidList(_strings);
        }
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::vector<std::string> _strings;
};


class ExpressionsAdapter :
        public AdapterT<ExpressionsCBH, QSMySqlParser::ExpressionsContext>,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "Unhandled PredicateExpression with BoolTerm.", _ctx);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        _expressions.push_back(valueExpr);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        lockedParent()->handleExpressions(_expressions);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::vector<std::shared_ptr<query::ValueExpr>> _expressions;
};


class ConstantsAdapter :
        public AdapterT<ConstantsCBH, QSMySqlParser::ConstantsContext>,
        public ConstantCBH {
public:
    using AdapterT::AdapterT;

    void handleConstant(std::string const& val) override {
        _values.push_back(val);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        lockedParent()->handleConstants(_values);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::vector<std::string> _values;
};


class AggregateFunctionCallAdapter :
        public AdapterT<AggregateFunctionCallCBH, QSMySqlParser::AggregateFunctionCallContext>,
        public AggregateWindowedFunctionCBH {
public:
    using AdapterT::AdapterT;

    void handleAggregateWindowedFunction(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        lockedParent()->handleAggregateFunctionCall(valueFactor);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {}

    std::string name() const override { return getTypeName(this); }
};


class ScalarFunctionCallAdapter :
        public AdapterT<ScalarFunctionCallCBH, QSMySqlParser::ScalarFunctionCallContext>,
        public ScalarFunctionNameCBH,
        public FunctionArgsCBH {
public:
    using AdapterT::AdapterT;

    void handleScalarFunctionName(std::string const& name) override {
        assert_execution_condition(_name.empty(), "name should be set once.", _ctx);
        _name = name;
    }

    void handleFunctionArgs(std::vector<std::shared_ptr<query::ValueExpr>> const& valueExprs) override {
        assert_execution_condition(_valueExprs.empty(), "FunctionArgs should be set once.", _ctx);
        _valueExprs = valueExprs;
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        assert_execution_condition(_valueExprs.empty() == false && _name.empty() == false,
                "valueExprs or name is not populated.", _ctx);
        auto funcExpr = query::FuncExpr::newWithArgs(_name, _valueExprs);
        auto valueFactor = query::ValueFactor::newFuncFactor(funcExpr);
        lockedParent()->handleScalarFunctionCall(valueFactor);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::vector<std::shared_ptr<query::ValueExpr>> _valueExprs;
    std::string _name;
};


class UdfFunctionCallAdapter :
        public AdapterT<UdfFunctionCallCBH, QSMySqlParser::UdfFunctionCallContext>,
        public FullIdCBH,
        public FunctionArgsCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionArgs(std::vector<std::shared_ptr<query::ValueExpr>> const& valueExprs) override {
        // This is only expected to be called once.
        // Of course the valueExpr may have more than one valueFactor.
        assert_execution_condition(_args.empty(), "Args already assigned.", _ctx);
        _args = valueExprs;
    }

    // FullIdCBH
    void handleFullId(std::vector<std::string> const& uidlist) override {
        assert_execution_condition(_functionName.empty(), "Function name already assigned.", _ctx);
        assert_execution_condition(uidlist.size() == 1, "Function name invalid", _ctx);
        _functionName = uidlist.at(0);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        assert_execution_condition(!_functionName.empty(), "Function name unpopulated", _ctx);
        assert_execution_condition(!_args.empty(), "Function arguments unpopulated", _ctx);
        auto funcExpr = query::FuncExpr::newWithArgs(_functionName, _args);
        auto valueFactor = query::ValueFactor::newFuncFactor(funcExpr);
        lockedParent()->handleUdfFunctionCall(valueFactor);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::vector<std::shared_ptr<query::ValueExpr>> _args;
    std::string _functionName;
};


class AggregateWindowedFunctionAdapter :
        public AdapterT<AggregateWindowedFunctionCBH, QSMySqlParser::AggregateWindowedFunctionContext>,
        public FunctionArgCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionArg(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        assert_execution_condition(nullptr == _valueFactor,
                "currently ValueFactor can only be set once.", _ctx);
        _valueFactor = valueFactor;
    }

    void checkContext() const override {
        // optional:
        // AVG();
        // MAX();
        // MIN();
        // SUM();
        // COUNT();
        // starArg;

        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->aggregator == nullptr, "aggregator is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->ALL() == nullptr, "ALL is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->DISTINCT() == nullptr, "DISTINCT is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->separator == nullptr, "separator is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SEPARATOR() == nullptr, "SEPARATOR is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->functionArgs() == nullptr,
                "functionArgs (plural) is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->BIT_AND() == nullptr, "BIT_AND is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->BIT_OR() == nullptr, "BIT_OR is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->BIT_XOR() == nullptr, "BIT_XOR is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->STD() == nullptr, "STD is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->STDDEV() == nullptr, "STDDEV is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->STDDEV_POP() == nullptr, "STDDEV_POP is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->STDDEV_SAMP() == nullptr,
                "STDDEV_SAMP is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->VAR_POP() == nullptr, "VAR_POP is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->VAR_SAMP() == nullptr, "VAR_SAMP is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->VARIANCE() == nullptr, "VARIANCE is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->GROUP_CONCAT() == nullptr,
                "GROUP_CONCAT is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->ORDER() == nullptr, "ORDER is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->BY() == nullptr, "BY is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->STRING_LITERAL() == nullptr, "STRING_LITERAL is not supported", _ctx);
    }

    void onExit() override {
        std::shared_ptr<query::FuncExpr> funcExpr;
        if (_ctx->COUNT() && _ctx->starArg) {
            std::string table;
            auto starFactor = query::ValueFactor::newStarFactor(table);
            auto starParExpr = std::make_shared<query::ValueExpr>();
            starParExpr->addValueFactor(starFactor);
            funcExpr = query::FuncExpr::newArg1(_ctx->COUNT()->getText(), starParExpr);
        } else if (_ctx->AVG() || _ctx->MAX() || _ctx->MIN() || _ctx->SUM() || _ctx->COUNT() ) {
            auto param = std::make_shared<query::ValueExpr>();
            assert_execution_condition(nullptr != _valueFactor, "ValueFactor must be populated.", _ctx);
            param->addValueFactor(_valueFactor);
            antlr4::tree::TerminalNode * terminalNode = nullptr;
            if (_ctx->AVG()) {
                terminalNode = _ctx->AVG();
            } else if (_ctx->MAX()) {
                terminalNode = _ctx->MAX();
            } else if (_ctx->MIN()) {
                terminalNode = _ctx->MIN();
            } else if (_ctx->SUM()) {
                terminalNode = _ctx->SUM();
            } else if (_ctx->COUNT()) {
                terminalNode = _ctx->COUNT();
            } else {
                assert_execution_condition(false, "Unhandled function type", _ctx);
            }
            funcExpr = query::FuncExpr::newArg1(terminalNode->getText(), param);
        } else {
            assert_execution_condition(false, "Unhandled exit", _ctx);
        }
        auto aggValueFactor = query::ValueFactor::newAggFactor(funcExpr);
        lockedParent()->handleAggregateWindowedFunction(aggValueFactor);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueFactor> _valueFactor;
};


class ScalarFunctionNameAdapter :
        public AdapterT<ScalarFunctionNameCBH, QSMySqlParser::ScalarFunctionNameContext>,
        public FunctionNameBaseCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionNameBase(std::string const& name) override {
        _name = name;
    }

    void checkContext() const override {
        // required:
        assert_execution_condition(_ctx->functionNameBase() != nullptr, "Context check failure.", _ctx);
        // not supported:
        assertNotSupported(__FUNCTION__, _ctx->ASCII() == nullptr, "ASCII is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->CURDATE() == nullptr, "CURDATE is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->CURRENT_DATE() == nullptr, "CURRENT_DATE is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->CURRENT_TIME() == nullptr, "CURRENT_TIME is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->CURRENT_TIMESTAMP() == nullptr, "CURRENT_TIMESTAMP is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->CURTIME() == nullptr, "CURTIME is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->DATE_ADD() == nullptr, "DATE_ADD is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->DATE_SUB() == nullptr, "DATE_SUB is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->IF() == nullptr, "IF is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->INSERT() == nullptr, "INSERT is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->LOCALTIME() == nullptr, "LOCALTIME is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->LOCALTIMESTAMP() == nullptr, "LOCALTIMESTAMP is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->MID() == nullptr, "MID is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->NOW() == nullptr, "NOW is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->REPLACE() == nullptr, "REPLACE is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SUBSTR() == nullptr, "SUBSTR is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SUBSTRING() == nullptr, "SUBSTRING is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->SYSDATE() == nullptr, "SYSDATE is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->TRIM() == nullptr, "TRIM is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->UTC_DATE() == nullptr, "UTC_DATE is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->UTC_TIME() == nullptr, "UTC_TIME is not supported", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->UTC_TIMESTAMP() == nullptr, "UTC_TIMESTAMP is not supported", _ctx);
    }

    void onExit() override {
        std::string str;
        if (_name.empty()) {
            _name = _ctx->getText();
        }
        assert_execution_condition(_name.empty() == false,
                "not populated; expected a callback from functionNameBase", _ctx);
        lockedParent()->handleScalarFunctionName(_name);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::string _name;
};


class FunctionArgsAdapter :
        public AdapterT<FunctionArgsCBH, QSMySqlParser::FunctionArgsContext>,
        public ConstantCBH,
        public FullColumnNameCBH,
        public ScalarFunctionCallCBH,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handleConstant(std::string const& val) override {
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->addValueFactor(query::ValueFactor::newConstFactor(val));
        _args.push_back(valueExpr);
    }

    void handleFullColumnName(std::shared_ptr<query::ValueFactor> const& columnName) override {
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->addValueFactor(columnName);
        _args.push_back(valueExpr);
    }

    void handleScalarFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->addValueFactor(valueFactor);
        _args.push_back(valueExpr);
    }

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "Unhandled PredicateExpression with BoolTerm.", _ctx);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        _args.push_back(valueExpr);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        lockedParent()->handleFunctionArgs(_args);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::vector<std::shared_ptr<query::ValueExpr>> _args;
};


class FunctionArgAdapter :
        public AdapterT<FunctionArgCBH, QSMySqlParser::FunctionArgContext>,
        public FullColumnNameCBH {
public:
    using AdapterT::AdapterT;

    void handleFullColumnName(std::shared_ptr<query::ValueFactor> const& columnName) override {
        assert_execution_condition(nullptr == _valueFactor,
                "Expected exactly one callback; valueFactor should be NULL.", _ctx);
        _valueFactor = columnName;
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        lockedParent()->handleFunctionArg(_valueFactor);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueFactor> _valueFactor;
};


class NotExpressionAdapter :
        public AdapterT<NotExpressionCBH, QSMySqlParser::NotExpressionContext>,
        public PredicateExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(nullptr == _boolFactor, "BoolFactor already set.", _ctx);
        _boolFactor = std::dynamic_pointer_cast<query::BoolFactor>(boolTerm);
        assert_execution_condition(nullptr != _boolFactor, "Could not cast BoolTerm to a BoolFactor.", _ctx);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        assert_execution_condition(false, "Unhandled PredicateExpression with ValueExpr.", _ctx);
    }

    void checkContext() const override {
        // testing notOperator includes testing NotExpressionContext::NOT(), this is done in onExit.
    }

    void onExit() override {
        _boolFactor->setHasNot(nullptr != _ctx->notOperator);
        lockedParent()->handleNotExpression(_boolFactor, _ctx);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::BoolFactor> _boolFactor;
};


class LogicalExpressionAdapter :
        public AdapterT<LogicalExpressionCBH, QSMySqlParser::LogicalExpressionContext>,
        public LogicalExpressionCBH,
        public PredicateExpressionCBH,
        public LogicalOperatorCBH,
        public QservFunctionSpecCBH,
        public NotExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        trace_callback_info(__FUNCTION__, *boolTerm);
        _terms.push_back(boolTerm);
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        assert_execution_condition(false, "Unhandled PredicateExpression with ValueExpr.", _ctx);
    }

    void handleQservFunctionSpec(std::string const& functionName,
            std::vector<std::shared_ptr<query::ValueFactor>> const& args) override {
        // qserv query IR handles qserv restrictor functions differently than the and/or bool tree that
        // handles the rest of the where clause, pass the function straight up to the parent.
        trace_callback_info(__FUNCTION__, "");
        lockedParent()->handleQservFunctionSpec(functionName, args);
    }

    void handleLogicalOperator(LogicalOperatorCBH::OperatorType operatorType) override {
        trace_callback_info(__FUNCTION__, LogicalOperatorCBH::OperatorTypeToStr(operatorType));
        assert_execution_condition(false == _logicalOperatorIsSet,
                "logical operator must be set only once.", _ctx);
        _logicalOperatorIsSet = true;
        _logicalOperatorType = operatorType;
    }

    void handleLogicalExpression(std::shared_ptr<query::LogicalTerm> const& logicalTerm,
            antlr4::ParserRuleContext* childCtx) override {
        trace_callback_info(__FUNCTION__, logicalTerm);
        _terms.push_back(logicalTerm);
    }

    void handleNotExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        _terms.push_back(boolTerm);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        assert_execution_condition(_logicalOperatorIsSet, "logicalOperator is not set.", _ctx);
        std::shared_ptr<query::LogicalTerm> logicalTerm;
        switch (_logicalOperatorType) {
            case LogicalOperatorCBH::AND: {
                logicalTerm = std::make_shared<query::AndTerm>();
                for (auto term : _terms) {
                    if (false == logicalTerm->merge(*term)) {
                        logicalTerm->addBoolTerm(term);
                    }
                }
                break;
            }

            case LogicalOperatorCBH::OR: {
                auto orTerm = std::make_shared<query::OrTerm>();
                logicalTerm = orTerm;
                for (auto term : _terms) {
                    if (false == logicalTerm->merge(*term)) {
                        logicalTerm->addBoolTerm(std::make_shared<query::AndTerm>(term));
                    }
                }
                break;
            }

            default:
                assert_execution_condition(false, "unhandled logical operator.", _ctx);
        }
        lockedParent()->handleLogicalExpression(logicalTerm, _ctx);
    }

    std::string name() const override { return getTypeName(this); }

    friend std::ostream& operator<<(std::ostream& os, const LogicalExpressionAdapter& logicalExpressionAdapter) {
        os << "LogicalExpressionAdapter(";
        os << util::printable(logicalExpressionAdapter._terms);
        return os;
    }

private:
    std::vector<std::shared_ptr<query::BoolTerm>> _terms;
    LogicalOperatorCBH::OperatorType _logicalOperatorType;
    bool _logicalOperatorIsSet = false;
};




class InPredicateAdapter :
        public AdapterT<InPredicateCBH, QSMySqlParser::InPredicateContext>,
        public ExpressionAtomPredicateCBH,
        public ExpressionsCBH {
public:
    using AdapterT::AdapterT;

    void handleExpressionAtomPredicate(std::shared_ptr<query::ValueExpr> const& valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(_ctx->predicate() == childCtx, "callback from unexpected element.", _ctx);
        assert_execution_condition(nullptr == _predicate, "Predicate should be set exactly once.", _ctx);
        _predicate = valueExpr;
    }

    void handleExpressionAtomPredicate(std::shared_ptr<query::BoolTerm> const& boolFactor,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "unhandled ExpressionAtomPredicate BoolTerm callback.", _ctx);
    }

    void handleExpressions(std::vector<std::shared_ptr<query::ValueExpr>> const& valueExprs) override {
        assert_execution_condition(_expressions.empty(), "expressions should be set exactly once.", _ctx);
        _expressions = valueExprs;
    }

    void checkContext() const override {
        // optional:
        // NOT()
    }

    void onExit() override {
        assert_execution_condition(false == _expressions.empty() && _predicate != nullptr,
                "InPredicateAdapter was not fully populated.", _ctx);
        auto inPredicate = std::make_shared<query::InPredicate>(_predicate, _expressions, _ctx->NOT() != nullptr);
        lockedParent()->handleInPredicate(inPredicate);
    }

    friend std::ostream& operator<<(std::ostream& os, const InPredicateAdapter& inPredicateAdapter) {
        os << "InPredicateAdapter(";
        os << "predicate:" << inPredicateAdapter._predicate;
        os << ", expressions:" << util::printable(inPredicateAdapter._expressions);
        return os;
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> _predicate;
    std::vector<std::shared_ptr<query::ValueExpr>> _expressions;
};


class BetweenPredicateAdapter :
        public AdapterT<BetweenPredicateCBH, QSMySqlParser::BetweenPredicateContext>,
        public ExpressionAtomPredicateCBH {
public:
    using AdapterT::AdapterT;

    void handleExpressionAtomPredicate(std::shared_ptr<query::ValueExpr> const& valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        if (childCtx == _ctx->val) {
            assert_execution_condition(nullptr == _val, "val should be set exactly once.", _ctx);
            _val = valueExpr;
            return;
        }
        if (childCtx == _ctx->min) {
            assert_execution_condition(nullptr == _min, "min should be set exactly once.", _ctx);
            _min = valueExpr;
            return;
        }
        if (childCtx == _ctx->max) {
            assert_execution_condition(nullptr == _max, "max should be set exactly once.", _ctx);
            _max = valueExpr;
            return;
        }
    }

    void handleExpressionAtomPredicate(std::shared_ptr<query::BoolTerm> const& boolFactor,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "unhandled ExpressionAtomPredicate BoolTerm callback.", _ctx);
    }

    void checkContext() const override {
        // optional:
        // NOT()
    }

    void onExit() override {
        assert_execution_condition(nullptr != _val && nullptr != _min && nullptr != _max,
                "val, min, and max must all be set.", _ctx);
        auto betweenPredicate = std::make_shared<query::BetweenPredicate>(_val, _min, _max, _ctx->NOT() != nullptr);
        lockedParent()->handleBetweenPredicate(betweenPredicate);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> _val;
    std::shared_ptr<query::ValueExpr> _min;
    std::shared_ptr<query::ValueExpr> _max;
};


class IsNullPredicateAdapter :
        public AdapterT<IsNullPredicateCBH, QSMySqlParser::IsNullPredicateContext>,
        public ExpressionAtomPredicateCBH,
        public NullNotnullCBH {
public:
    using AdapterT::AdapterT;

    void handleExpressionAtomPredicate(std::shared_ptr<query::ValueExpr> const& valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(nullptr == _valueExpr,
                "Expected the ValueExpr to be set once.", _ctx);
        _valueExpr = valueExpr;
    }

    void handleExpressionAtomPredicate(std::shared_ptr<query::BoolTerm> const& boolFactor,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false,
                "unexpected call to handleExpressionAtomPredicate.", _ctx);
    }

    void handleNullNotnull(bool isNotNull) override {
        _isNotNull = isNotNull;
    }

    void checkContext() const override {
        // IS is implicit, and other elements are handled via adapters.
    }

    void onExit() override {
        assert_execution_condition(_valueExpr != nullptr, "IsNullPredicateAdapter was not populated.", _ctx);
        auto np = std::make_shared<query::NullPredicate>(_valueExpr, _isNotNull);
        lockedParent()->handleIsNullPredicate(np);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> _valueExpr;
    bool _isNotNull {false};
};


class LikePredicateAdapter :
        public AdapterT<LikePredicateCBH, QSMySqlParser::LikePredicateContext>,
        public ExpressionAtomPredicateCBH {
public:
    using AdapterT::AdapterT;

    void handleExpressionAtomPredicate(std::shared_ptr<query::ValueExpr> const& valueExpr,
            antlr4::ParserRuleContext* childCtx) override {
        if (nullptr == _valueExprA) {
            _valueExprA = valueExpr;
        } else if (nullptr == _valueExprB) {
            _valueExprB = valueExpr;
        } else {
            assert_execution_condition(false, "Expected to be called back exactly twice.", _ctx);
        }
    }

    void handleExpressionAtomPredicate(std::shared_ptr<query::BoolTerm> const& boolFactor,
            antlr4::ParserRuleContext* childCtx) override {
        assert_execution_condition(false, "Unhandled BoolTerm callback.", _ctx);
    }

    void checkContext() const override {
        assertNotSupported(__FUNCTION__, _ctx->ESCAPE() == nullptr, "ESCAPE is not supported.", _ctx);
        assertNotSupported(__FUNCTION__, _ctx->STRING_LITERAL() == nullptr,
                "STRING_LITERAL is not supported", _ctx);
    }

    void onExit() override {
        assert_execution_condition(_valueExprA != nullptr && _valueExprB != nullptr,
                "LikePredicateAdapter was not fully populated.", _ctx);
        auto likePredicate = std::make_shared<query::LikePredicate>();
        likePredicate->value = _valueExprA;
        likePredicate->charValue = _valueExprB;
        likePredicate->hasNot = _ctx->NOT() != nullptr;
        lockedParent()->handleLikePredicate(likePredicate);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> _valueExprA;
    std::shared_ptr<query::ValueExpr> _valueExprB;
};


class NestedExpressionAtomAdapter :
        public AdapterT<NestedExpressionAtomCBH, QSMySqlParser::NestedExpressionAtomContext>,
        public PredicateExpressionCBH,
        public LogicalExpressionCBH {
public:
    using AdapterT::AdapterT;

    void handlePredicateExpression(std::shared_ptr<query::BoolTerm> const& boolTerm,
            antlr4::ParserRuleContext* childCtx) override {
        trace_callback_info(__FUNCTION__, *boolTerm);
        assert_execution_condition(nullptr == _valueExpr && nullptr == _boolTerm,
                "unexpected boolTerm callback.", _ctx);
        auto boolFactor = std::dynamic_pointer_cast<query::BoolFactor>(boolTerm);
        assert_execution_condition(nullptr != boolFactor, "could not cast boolTerm to a BoolFactor.", _ctx);
        auto orBoolFactor = std::make_shared<query::BoolFactor>(
                std::make_shared<query::BoolTermFactor>(
                    std::make_shared<query::OrTerm>(
                        std::make_shared<query::AndTerm>(boolFactor))));
        orBoolFactor->addParenthesis();
        _boolTerm = orBoolFactor;
    }

    void handlePredicateExpression(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        trace_callback_info(__FUNCTION__, *valueExpr);
        assert_execution_condition(nullptr == _valueExpr && nullptr == _boolTerm,
                "unexpected ValueExpr callback.", _ctx);
        _valueExpr = valueExpr;
    }

    void handleLogicalExpression(std::shared_ptr<query::LogicalTerm> const& logicalTerm,
            antlr4::ParserRuleContext* childCtx) override {
        trace_callback_info(__FUNCTION__, *logicalTerm);
        assert_execution_condition(nullptr == _valueExpr && nullptr == _boolTerm,
                "unexpected LogicalTerm callback.", _ctx);
        auto boolFactor = std::make_shared<query::BoolFactor>(std::make_shared<query::BoolTermFactor>(logicalTerm));
        boolFactor->addParenthesis();
        _boolTerm = boolFactor;
    }

    void handleQservFunctionSpec(std::string const& functionName,
            std::vector<std::shared_ptr<query::ValueFactor>> const& args) override {
        assertNotSupported(__FUNCTION__, false, "Qserv functions may not appear in nested contexts.", _ctx);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        if (nullptr != _boolTerm) {
            auto boolFactor = std::dynamic_pointer_cast<query::BoolFactor>(_boolTerm);
            lockedParent()->handleNestedExpressionAtom(_boolTerm);
        } else if (nullptr != _valueExpr) {
            lockedParent()->handleNestedExpressionAtom(_valueExpr);
        }
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> _valueExpr;
    std::shared_ptr<query::BoolTerm> _boolTerm;
};


class MathExpressionAtomAdapter :
        public AdapterT<MathExpressionAtomCBH, QSMySqlParser::MathExpressionAtomContext>,
        public MathOperatorCBH,
        public FunctionCallExpressionAtomCBH,
        public FullColumnNameExpressionAtomCBH,
        public ConstantExpressionAtomCBH,
        public NestedExpressionAtomCBH,
        public MathExpressionAtomCBH {
public:
    using AdapterT::AdapterT;

    void handleFunctionCallExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        _getValueExpr()->addValueFactor(valueFactor);
    }

    void handleMathOperator(MathOperatorCBH::OperatorType operatorType) override {
        switch (operatorType) {
        default:
            assert_execution_condition(false, "Unhandled operatorType.", _ctx);
            break;

        case MathOperatorCBH::SUBTRACT: {
            bool success = _getValueExpr()->addOp(query::ValueExpr::MINUS);
            assert_execution_condition(success,
                    "Failed to add an operator to valueExpr.", _ctx);
            break;
        }

        case MathOperatorCBH::ADD: {
            bool success = _getValueExpr()->addOp(query::ValueExpr::PLUS);
            assert_execution_condition(success,
                    "Failed to add an operator to valueExpr.", _ctx);
            break;
        }

        case MathOperatorCBH::DIVIDE: {
            bool success = _getValueExpr()->addOp(query::ValueExpr::DIVIDE);
            assert_execution_condition(success,
                    "Failed to add an operator to valueExpr.", _ctx);
            break;
        }

        case MathOperatorCBH::MULTIPLY: {
            bool success = _getValueExpr()->addOp(query::ValueExpr::MULTIPLY);
            assert_execution_condition(success,
                    "Failed to add an operator to valueExpr.", _ctx);
            break;
        }

        case MathOperatorCBH::DIV: {
            bool success = _getValueExpr()->addOp(query::ValueExpr::DIV);
            assert_execution_condition(success,
                    "Failed to add an operator to valueExpr.", _ctx);
            break;
        }

        case MathOperatorCBH::MOD: {
            bool success = _getValueExpr()->addOp(query::ValueExpr::MOD);
            assert_execution_condition(success,
                    "Failed to add an operator to valueExpr.", _ctx);
            break;
        }

        case MathOperatorCBH::MODULO: {
            bool success = _getValueExpr()->addOp(query::ValueExpr::MODULO);
            assert_execution_condition(success,
                    "Failed to add an operator to valueExpr.", _ctx);
            break;
        }
        }
    }

    void HandleFullColumnNameExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        _getValueExpr()->addValueFactor(valueFactor);
    }

    void handleConstantExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        _getValueExpr()->addValueFactor(valueFactor);
    }

    void handleNestedExpressionAtom(std::shared_ptr<query::BoolTerm> const& boolTerm) override {
        assert_execution_condition(false, "unexpected boolTerm callback.", _ctx);
    }

    void handleNestedExpressionAtom(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        auto valueFactor = query::ValueFactor::newExprFactor(valueExpr);
        _getValueExpr()->addValueFactor(valueFactor);
    }

    void handleMathExpressionAtom(std::shared_ptr<query::ValueExpr> const& valueExpr) override {
        // for now, make the assumption that in a case where there is more than one operator to add, that
        // the first call will be a MathExpressionAtom callback which populates _valueExpr, and later calls
        // will be ValueFactor callbacks. If that's NOT the case and a second MathExpressionAtom callback
        // might happen, or a ValueFactor callback might happen before a MathExpressionAtom callback then
        // this algorithm may have to be rewritten; this funciton may need to pass a vector of ValueFactors
        // as the callback argument, instead of a ValueExpr that contains a vector of ValueFactors.
        assert_execution_condition(nullptr == _valueExpr, "expected _valueExpr to be null.", _ctx);
        _valueExpr = valueExpr;
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        assert_execution_condition(_valueExpr != nullptr, "valueExpr not populated.", _ctx);
        lockedParent()->handleMathExpressionAtom(_valueExpr);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueExpr> const& _getValueExpr() {
        if (nullptr == _valueExpr) {
            _valueExpr = std::make_shared<query::ValueExpr>();
        }
        return _valueExpr;
    }

    std::shared_ptr<query::ValueExpr> _valueExpr;
};


class FunctionCallExpressionAtomAdapter :
        public AdapterT<FunctionCallExpressionAtomCBH, QSMySqlParser::FunctionCallExpressionAtomContext>,
        public UdfFunctionCallCBH,
        public ScalarFunctionCallCBH,
        public AggregateFunctionCallCBH
        {
public:
    using AdapterT::AdapterT;

    void handleUdfFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        assert_execution_condition(_valueFactor == nullptr, "the valueFactor must be set only once.", _ctx);
        _valueFactor = valueFactor;
    }

    void handleScalarFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        assert_execution_condition(_valueFactor == nullptr, "the valueFactor must be set only once.", _ctx);
        _valueFactor = valueFactor;
    }

    void handleAggregateFunctionCall(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        assert_execution_condition(_valueFactor == nullptr, "the valueFactor must be set only once.", _ctx);
        _valueFactor = valueFactor;
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        lockedParent()->handleFunctionCallExpressionAtom(_valueFactor);
    }

    std::string name() const override { return getTypeName(this); }

private:
    std::shared_ptr<query::ValueFactor> _valueFactor;
};


class BitExpressionAtomAdapter :
        public AdapterT<BitExpressionAtomCBH, QSMySqlParser::BitExpressionAtomContext>,
        public FullColumnNameExpressionAtomCBH,
        public BitOperatorCBH,
        public ConstantExpressionAtomCBH {
public:
    using AdapterT::AdapterT;

    void HandleFullColumnNameExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        _setValueFactor(valueFactor);
    }

    void handleBitOperator(BitOperatorCBH::OperatorType operatorType) override {
        assert_execution_condition(false == _didSetOp, "op is already set.", _ctx);
        _operator = operatorType;
        _didSetOp = true;
    }

    void handleConstantExpressionAtom(std::shared_ptr<query::ValueFactor> const& valueFactor) override {
        _setValueFactor(valueFactor);
    }

    void checkContext() const override {
        // nothing to check
    }

    void onExit() override {
        assert_execution_condition(nullptr != _left && nullptr != _right && true == _didSetOp,
                "Not all values were populated.", _ctx);
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->addValueFactor(_left);
        auto valueExprOp = _translateOperator(_operator);
        valueExpr->addOp(valueExprOp);
        valueExpr->addValueFactor(_right);
        lockedParent()->handleBitExpressionAtom(valueExpr);
    }

    std::string name() const override { return getTypeName(this); }

private:
    query::ValueExpr::Op _translateOperator(BitOperatorCBH::OperatorType op) {
        switch(op) {
            case BitOperatorCBH::LEFT_SHIFT: return query::ValueExpr::BIT_SHIFT_LEFT;
            case BitOperatorCBH::RIGHT_SHIFT: return query::ValueExpr::BIT_SHIFT_RIGHT;
            case BitOperatorCBH::AND: return query::ValueExpr::BIT_AND;
            case BitOperatorCBH::XOR: return query::ValueExpr::BIT_XOR;
            case BitOperatorCBH::OR: return query::ValueExpr::BIT_OR;
        }
        assert_execution_condition(false, "Failed to translate token from BitOperatorCBH to ValueExpr.", _ctx);
        return query::ValueExpr::NONE;
    }

    void _setValueFactor(std::shared_ptr<query::ValueFactor> const& valueFactor) {
        if (nullptr == _left) {
            _left = valueFactor;
        } else if (nullptr == _right) {
            _right = valueFactor;
        } else {
            assert_execution_condition(false, "Left and Right are already set.", _ctx);
        }
    }

    std::shared_ptr<query::ValueFactor> _left;
    std::shared_ptr<query::ValueFactor> _right;
    bool _didSetOp{false};
    BitOperatorCBH::OperatorType _operator;
};


class LogicalOperatorAdapter :
        public AdapterT<LogicalOperatorCBH, QSMySqlParser::LogicalOperatorContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        // we don't support all the possible operators indicated by the grammar.
        // onExit will throw if a supported operator is not found.
    }

    void onExit() override {
        if (_ctx->AND() != nullptr || _ctx->getText() == "&&") {
            // Qserv IR is not set up to treat AND and && differently, up to and inlcuding that the AndTerm
            // automatically serializes itself to "AND" (i.e. not to lower case "and" or any other form). If
            // it becomes important to handle different forms of the lexical AND differently we can add it,
            // but for now it seems unnecessary.
            lockedParent()->handleLogicalOperator(LogicalOperatorCBH::AND);
        } else if (_ctx->OR() != nullptr || _ctx->getText() == "||") {
            lockedParent()->handleLogicalOperator(LogicalOperatorCBH::OR);
        } else {
            assert_execution_condition(false, "unhandled logical operator", _ctx);
        }
    }

    std::string name() const override { return getTypeName(this); }
};


class BitOperatorAdapter :
        public AdapterT<BitOperatorCBH, QSMySqlParser::BitOperatorContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        // all cases are handled in onExit
    }

    void onExit() override {
        if (_ctx->getText() == "<<") {
            lockedParent()->handleBitOperator(BitOperatorCBH::LEFT_SHIFT);
        } else if (_ctx->getText() == ">>") {
            lockedParent()->handleBitOperator(BitOperatorCBH::RIGHT_SHIFT);
        } else if (_ctx->getText() == "&") {
            lockedParent()->handleBitOperator(BitOperatorCBH::AND);
        } else if (_ctx->getText() == "|") {
            lockedParent()->handleBitOperator(BitOperatorCBH::OR);
        } else if (_ctx->getText() == "^") {
            lockedParent()->handleBitOperator(BitOperatorCBH::XOR);
        } else {
            assert_execution_condition(false, "unhandled bit operator", _ctx);
        }
    }

    std::string name() const override { return getTypeName(this); }
};


class MathOperatorAdapter :
        public AdapterT<MathOperatorCBH, QSMySqlParser::MathOperatorContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        // we don't support all the possible operators indicated by the grammar.
        // onExit will throw if a supported operator is not found.
    }

    void onExit() override {
        if (_ctx->getText() == "-") {
            lockedParent()->handleMathOperator(MathOperatorCBH::SUBTRACT);
        } else if (_ctx->getText() == "+") {
            lockedParent()->handleMathOperator(MathOperatorCBH::ADD);
        } else if (_ctx->getText() == "/") {
            lockedParent()->handleMathOperator(MathOperatorCBH::DIVIDE);
        } else if (_ctx->getText() == "*") {
            lockedParent()->handleMathOperator(MathOperatorCBH::MULTIPLY);
        } else if (_ctx->DIV() != nullptr) {
            lockedParent()->handleMathOperator(MathOperatorCBH::DIV);
        } else if (_ctx->MOD() != nullptr) {
            lockedParent()->handleMathOperator(MathOperatorCBH::MOD);
        } else if (_ctx->getText() == "%") {
            lockedParent()->handleMathOperator(MathOperatorCBH::MODULO);
        } else {
            assertNotSupported(__FUNCTION__, false, "Unhandled operator type:" + _ctx->getText(), _ctx);
        }
    }

    std::string name() const override { return getTypeName(this); }
};



class FunctionNameBaseAdapter :
        public AdapterT<FunctionNameBaseCBH, QSMySqlParser::FunctionNameBaseContext> {
public:
    using AdapterT::AdapterT;

    void checkContext() const override {
        // there's something like 300 possible functions. I'd like to know which ones we support and only
        // allow those ones through. However, we often use keywords (including SQL function names) as ID
        // and so this is not a good place to restrict function names because it may be that the token that
        // parsed as a FunctionNameBase is actually an ID, like a column name.
        // Filtering for valid functions has to happen in qana.
    }

    void onExit() override {
        lockedParent()->handleFunctionNameBase(_ctx->getText());
    }

    std::string name() const override { return getTypeName(this); }
};

}}} // lsst::qserv::ccontrol


#endif // LSST_QSERV_CCONTROL_PARSEADAPTERS_H
