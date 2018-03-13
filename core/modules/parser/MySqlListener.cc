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

#include "MySqlListener.h"

#include "lsst/log/Log.h"
#include "parser/ValueExprFactory.h"
#include "parser/ValueFactorFactory.h"
#include "query/BoolTerm.h"
#include "query/FromList.h"
#include "query/Predicate.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/SqlSQL2Tokens.h"
#include "query/TableRef.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "SelectListFactory.h"

#include <cxxabi.h>
#include <sstream>
#include <vector>

using namespace std;


// This macro creates the enterXXX and exitXXX function definitions, for functions declared in
// MySqlListener.h; the enter function pushes the adapter onto the stack (with parent from top of the stack),
// and the exit function pops the adapter from the top of the stack.
#define ENTER_EXIT_PARENT(NAME) \
void MySqlListener::enter##NAME(MySqlParser::NAME##Context * ctx) { \
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__); \
    pushAdapterStack<NAME##CBH, NAME##Adapter>(ctx); \
} \
\
void MySqlListener::exit##NAME(MySqlParser::NAME##Context * ctx) { \
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__); \
    popAdapterStack<NAME##Adapter>(); \
} \


#define UNHANDLED(NAME) \
void MySqlListener::enter##NAME(MySqlParser::NAME##Context * ctx) { \
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " is UNHANDLED"); \
    std::ostringstream msg; \
    msg << "enter" << #NAME << " not supported."; \
    throw MySqlListener::adapter_order_error(msg.str()); \
} \
\
void MySqlListener::exit##NAME(MySqlParser::NAME##Context * ctx) {}\


#define IGNORED(NAME) \
void MySqlListener::enter##NAME(MySqlParser::NAME##Context * ctx) { \
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " is IGNORED"); \
} \
\
void MySqlListener::exit##NAME(MySqlParser::NAME##Context * ctx) {\
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__ << " is IGNORED"); \
} \



namespace lsst {
namespace qserv {
namespace parser {

LOG_LOGGER _log = LOG_GET("lsst.qserv.MySqlListener");


/// Callback Handler classes

class BaseCBH {
public:
    virtual ~BaseCBH() {}
};


class DmlStatementCBH : public BaseCBH {
public:
    virtual void handleDmlStatement(shared_ptr<query::SelectStmt> selectStatement) = 0;
};


class SimpleSelectCBH : public BaseCBH {
public:
    virtual void handleSelectStatement(shared_ptr<query::SelectStmt> selectStatement) = 0;
};


class QuerySpecificationCBH : public BaseCBH {
public:
    virtual void handleQuerySpecification(shared_ptr<query::SelectList> selectList,
                                          shared_ptr<query::FromList> fromList,
                                          shared_ptr<query::WhereClause> whereClause) = 0;
};


class SelectElementsCBH : public BaseCBH {
public:
    virtual void handleSelectList(shared_ptr<query::SelectList> selectList) = 0;
};


class FullColumnNameCBH : public BaseCBH {
public:
    virtual void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) = 0;
};


class TableNameCBH : public BaseCBH {
public:
    virtual void handleTableName(const std::string& string) = 0;
};


class FromClauseCBH : public BaseCBH {
public:
    virtual void handleFromClause(shared_ptr<query::FromList> fromList,
                                  shared_ptr<query::WhereClause> whereClause) = 0;
};


class TableSourcesCBH : public BaseCBH {
public:
    virtual void handleTableSources(query::TableRefListPtr tableRefList) = 0;
};

class TableSourceBaseCBH : public BaseCBH {
public:
    virtual void handleTableSource(shared_ptr<query::TableRef> tableRef) = 0;
};


class AtomTableItemCBH : public BaseCBH {
public:
    virtual void handleAtomTableItem(shared_ptr<query::TableRef> tableRef) = 0;
};


class UidCBH : public BaseCBH {
public:
    virtual void handleUidString(const std::string& string) = 0;
};


class FullIdCBH : public BaseCBH {
public:
    virtual void handleFullIdString(const std::string& string) = 0;
};


class ConstantExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleConstantExpressionAtom(const string& text) = 0;
};


class ExpressionAtomPredicateCBH : public BaseCBH {
public:
    virtual void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr> valueExpr) = 0;
};


class ComparisonOperatorCBH : public BaseCBH {
public:
    virtual void handleComparisonOperator(const string& text) = 0;
};


class SelectColumnElementCBH : public BaseCBH {
public:
    virtual void handleColumnElement(shared_ptr<query::ValueExpr> columnElement) = 0;
};


class FullColumnNameExpressionAtomCBH : public BaseCBH {
public:
    virtual void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) = 0;

};


class BinaryComparasionPredicateCBH : public BaseCBH {
public:
    virtual ~BinaryComparasionPredicateCBH() {}
    virtual void handleOrTerm(shared_ptr<query::OrTerm> orTerm) = 0;
};


class PredicateExpressionCBH : public BaseCBH {
public:
    virtual void handleOrTerm(shared_ptr<query::OrTerm> orTerm, antlr4::ParserRuleContext* childCtx) = 0;
};


class ConstantCBH : public BaseCBH {
public:
    virtual void handleConstant(const std::string& val) = 0;
};


/// Adapter classes


class Adapter {
public:
    Adapter(shared_ptr<BaseCBH> parent, antlr4::ParserRuleContext* ctx)
    : _ctx(ctx)
    , _parent(parent)
    {}
    virtual ~Adapter() {}

    // onEnter is called just after the Adapter is pushed onto the context stack
    virtual void onEnter() {}

    // onExit is called just before the Adapter is popped from the context stack
    virtual void onExit() {}

protected:
    template <typename CBH>
    shared_ptr<CBH> lockedParent() {
        shared_ptr<BaseCBH> lockedParent = _parent.lock();
        if (nullptr == lockedParent) {
            throw MySqlListener::adapter_execution_error(
                    "Locking weak ptr to parent callback handler returned null");
        }
        auto castedParent = dynamic_pointer_cast<CBH>(lockedParent);
        if (nullptr == castedParent) {
            throw MySqlListener::adapter_execution_error(
                    "Casting ptr to parent callback handler returned null.");
        }
        return castedParent;
    }


    antlr4::ParserRuleContext* _ctx;
    weak_ptr<BaseCBH> _parent;
};


class RootAdapter : public Adapter, public DmlStatementCBH {
public:
    RootAdapter() : Adapter(nullptr, nullptr) {}

    shared_ptr<query::SelectStmt> getSelectStatement() { return _selectStatement; }

    void handleDmlStatement(shared_ptr<query::SelectStmt> selectStatement) override {
        _selectStatement = selectStatement;
    }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
};


class DmlStatementAdapter : public Adapter, public SimpleSelectCBH {
public:
    DmlStatementAdapter(shared_ptr<DmlStatementCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleSelectStatement(shared_ptr<query::SelectStmt> selectStatement) override {
        _selectStatement = selectStatement;
    }

    void onExit() override {
        lockedParent<DmlStatementCBH>()->handleDmlStatement(_selectStatement);
    }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
};


class SimpleSelectAdapter : public Adapter, public QuerySpecificationCBH {
public:
    SimpleSelectAdapter(shared_ptr<SimpleSelectCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleQuerySpecification(shared_ptr<query::SelectList> selectList,
                                  shared_ptr<query::FromList> fromList,
                                  shared_ptr<query::WhereClause> whereClause) override {
        _selectList = selectList;
        _fromList = fromList;
        _whereClause = whereClause;
    }

    void onExit() override {
        shared_ptr<query::SelectStmt> selectStatement = make_shared<query::SelectStmt>();
        selectStatement->setSelectList(_selectList);
        selectStatement->setFromList(_fromList);
        selectStatement->setWhereClause(_whereClause);
        selectStatement->setLimit(_limit);
        lockedParent<SimpleSelectCBH>()->handleSelectStatement(selectStatement);
    }

private:
    shared_ptr<query::SelectList> _selectList;
    shared_ptr<query::FromList> _fromList;
    shared_ptr<query::WhereClause> _whereClause;
    int _limit{lsst::qserv::NOTSET};
};


class QuerySpecificationAdapter : public Adapter, public SelectElementsCBH, public FromClauseCBH {
public:
    QuerySpecificationAdapter(shared_ptr<QuerySpecificationCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleSelectList(shared_ptr<query::SelectList> selectList) override {
        _selectList = selectList;
    }

    void handleFromClause(shared_ptr<query::FromList> fromList,
                          shared_ptr<query::WhereClause> whereClause) override {
        _fromList = fromList;
        _whereClause = whereClause;
    }

    void onExit() override {
        auto parent = _parent.lock();
        if (parent) {
        lockedParent<QuerySpecificationCBH>()->handleQuerySpecification(_selectList, _fromList, _whereClause);
        }
    }

private:
    shared_ptr<query::WhereClause> _whereClause;
    shared_ptr<query::FromList> _fromList;
    shared_ptr<query::SelectList> _selectList;
};


class SelectElementsAdapter : public Adapter, public SelectColumnElementCBH {
public:
    SelectElementsAdapter(shared_ptr<SelectElementsCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleColumnElement(shared_ptr<query::ValueExpr> columnElement) override {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << "adding column to the ValueExprPtrVector: " <<
                columnElement);
        SelectListFactory::addValueExpr(_selectList, columnElement);
    }

    void onExit() override {
        lockedParent<SelectElementsCBH>()->handleSelectList(_selectList);
    }

private:
    std::shared_ptr<query::SelectList> _selectList{std::make_shared<query::SelectList>()};
};


class FromClauseAdapter : public Adapter, public TableSourcesCBH, public PredicateExpressionCBH {
public:
    FromClauseAdapter(shared_ptr<FromClauseCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleTableSources(query::TableRefListPtr tableRefList) override {
        _tableRefList = tableRefList;
    }

    void handleOrTerm(shared_ptr<query::OrTerm> orTerm, antlr4::ParserRuleContext* childCtx) override {
        MySqlParser::FromClauseContext* ctx = dynamic_cast<MySqlParser::FromClauseContext*>(_ctx);
        if (nullptr == ctx) {
            throw MySqlListener::adapter_order_error(
                    "FromClauseAdapter's _ctx could not be cast to a FromClauseContext.");
        }
        if (ctx->whereExpr == childCtx) {
            if (_whereClause->getRootTerm()) {
                std::ostringstream msg;
                msg << "unexpected call to " << __FUNCTION__ << " when orTerm is already populated.";
                LOGS(_log, LOG_LVL_ERROR, msg.str());
                throw MySqlListener::adapter_execution_error(msg.str());
            }
            _whereClause->setRootTerm(orTerm);
        }
    }

    void onExit() override {
        shared_ptr<query::FromList> fromList = make_shared<query::FromList>(_tableRefList);
        lockedParent<FromClauseCBH>()->handleFromClause(fromList, _whereClause);
    }

private:
    shared_ptr<query::WhereClause> _whereClause{std::make_shared<query::WhereClause>()};
    query::TableRefListPtr _tableRefList;
};


class TableSourcesAdapter : public Adapter, public TableSourceBaseCBH {
public:
    TableSourcesAdapter(shared_ptr<TableSourcesCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleTableSource(shared_ptr<query::TableRef> tableRef) override {
        _tableRefList->push_back(tableRef);
    }

    void onExit() override {
        lockedParent<TableSourcesCBH>()->handleTableSources(_tableRefList);
    }

private:
    query::TableRefListPtr _tableRefList{make_shared<query::TableRefList>()};
};


class TableSourceBaseAdapter : public Adapter, public AtomTableItemCBH {
public:
    TableSourceBaseAdapter(shared_ptr<TableSourceBaseCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleAtomTableItem(shared_ptr<query::TableRef> tableRef) override {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << tableRef);
        _tableRef = tableRef;
    }

    void onExit() override {
        lockedParent<TableSourceBaseCBH>()->handleTableSource(_tableRef);
    }

private:
    shared_ptr<query::TableRef> _tableRef;
};


class AtomTableItemAdapter : public Adapter, public TableNameCBH {
public:
    AtomTableItemAdapter(shared_ptr<AtomTableItemCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleTableName(const std::string& string) override {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << string);
        _table = string;
    }

    void onExit() override {
        shared_ptr<query::TableRef> tableRef = make_shared<query::TableRef>(_db, _table, _alias);
        lockedParent<AtomTableItemCBH>()->handleAtomTableItem(tableRef);
    }

protected:
    std::string _db;
    std::string _table;
    std::string _alias;
};


class TableNameAdapter : public Adapter , public FullIdCBH {
public:
    TableNameAdapter(shared_ptr<TableNameCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleFullIdString(const std::string& string) override {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << string);
        lockedParent<TableNameCBH>()->handleTableName(string);
    }

protected:
    weak_ptr<TableNameCBH> _parent;
};


class FullIdAdapter : public Adapter, public UidCBH {
public:
    FullIdAdapter(shared_ptr<FullIdCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    virtual ~FullIdAdapter() {}

    void handleUidString(const std::string& string) override {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << string);
        lockedParent<FullIdCBH>()->handleFullIdString(string);
    }

protected:
    weak_ptr<FullIdCBH> _parent;
};


class FullColumnNameAdapter : public Adapter, public UidCBH {
public:
    FullColumnNameAdapter(shared_ptr<FullColumnNameCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleUidString(const std::string& string) override {
        LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
        auto valueFactor = ValueFactorFactory::newColumnColumnFactor("", "", string);
        auto valueExpr = std::make_shared<query::ValueExpr>();
        ValueExprFactory::addValueFactor(valueExpr, valueFactor);
        lockedParent<FullColumnNameCBH>()->handleFullColumnName(valueExpr);
    }
};


class ConstantExpressionAtomAdapter : public Adapter, public ConstantCBH {
public:
    ConstantExpressionAtomAdapter(shared_ptr<ConstantExpressionAtomCBH> parent,
                                  antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleConstant(const string& text) override {
        lockedParent<ConstantExpressionAtomCBH>()->handleConstantExpressionAtom(text);
    }
};


class FullColumnNameExpressionAtomAdapter : public Adapter, public FullColumnNameCBH {
public:
    FullColumnNameExpressionAtomAdapter(shared_ptr<FullColumnNameExpressionAtomCBH> parent,
                                        antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) override {
        LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
        lockedParent<FullColumnNameExpressionAtomCBH>()->handleFullColumnName(columnValueExpr);
    }
};


class ExpressionAtomPredicateAdapter : public Adapter, public ConstantExpressionAtomCBH,
        public FullColumnNameExpressionAtomCBH {
public:
    ExpressionAtomPredicateAdapter(shared_ptr<ExpressionAtomPredicateCBH> parent,
                                   antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleConstantExpressionAtom(const string& text) override {
        query::ValueExpr::FactorOp factorOp;
        factorOp.factor =  query::ValueFactor::newConstFactor(text);
        auto valueExpr = std::make_shared<query::ValueExpr>();
        valueExpr->getFactorOps().push_back(factorOp);
        lockedParent<ExpressionAtomPredicateCBH>()->handleExpressionAtomPredicate(valueExpr);
    }

    void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) override {
        LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
        lockedParent<ExpressionAtomPredicateCBH>()->handleExpressionAtomPredicate(columnValueExpr);
    }
};


class PredicateExpressionAdapter : public Adapter, public BinaryComparasionPredicateCBH {
public:
    PredicateExpressionAdapter(shared_ptr<PredicateExpressionCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleOrTerm(shared_ptr<query::OrTerm> orTerm) override {
        _orTerm = orTerm;
    }

    void onExit() {
        if (!_orTerm) {
            return; // todo; raise here?
        }
        lockedParent<PredicateExpressionCBH>()->handleOrTerm(_orTerm, _ctx);
    }

private:
    shared_ptr<query::OrTerm> _orTerm;
};


class BinaryComparasionPredicateAdapter : public Adapter, public ExpressionAtomPredicateCBH,
        public ComparisonOperatorCBH {
public:
    BinaryComparasionPredicateAdapter(shared_ptr<BinaryComparasionPredicateCBH> parent,
                                      antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleComparisonOperator(const string& text) override {
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__ << text);
        if (_comparison.empty()) {
            _comparison = text;
        } else {
            std::ostringstream msg;
            msg << "unexpected call to " << __FUNCTION__ <<
                    " when comparison value is already populated:" << _comparison;
            LOGS(_log, LOG_LVL_ERROR, msg.str());
            throw MySqlListener::adapter_execution_error(msg.str());
        }
    }

    void handleExpressionAtomPredicate(shared_ptr<query::ValueExpr> valueExpr) override {
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__);
        if (_left == nullptr) {
            _left = valueExpr;
        } else if (_right == nullptr) {
            _right = valueExpr;
        } else {
            std::ostringstream msg;
            msg << "unexpected call to " << __FUNCTION__ <<
                    " when left and right values are already populated:" << _left << ", " << _right;
            LOGS(_log, LOG_LVL_ERROR, msg.str());
            throw MySqlListener::adapter_execution_error(msg.str());
        }
    }

    void onExit() {
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__ << " " << _left << " " << _comparison << " " << _right);

        if (_left == nullptr || _right == nullptr) {
            std::ostringstream msg;
            msg << "unexpected call to " << __FUNCTION__ <<
                    " when left and right values are not both populated:" << _left << ", " << _right;
            throw MySqlListener::adapter_execution_error(msg.str());
        }

        auto compPredicate = std::make_shared<query::CompPredicate>();
        compPredicate->left = _left;

        // We need to remove the coupling between the query classes and the parser classes, in this case where
        // the query classes use the integer token types instead of some other system. For now this if/else
        // block allows us to go from the token string to the SqlSQL2Tokens type defined by the antlr2/3
        // grammar and used by the query objects.
        if (_comparison.compare(string("=")) == 0) {
            compPredicate->op = SqlSQL2Tokens::EQUALS_OP;
        } else {
            std::ostringstream msg;
            msg << "unhandled comparison operator in BinaryComparasionPredicateAdapter: " << _comparison;
            LOGS(_log, LOG_LVL_ERROR, msg.str());
            throw MySqlListener::adapter_execution_error(msg.str());
        }

        compPredicate->right = _right;

        auto boolFactor = std::make_shared<query::BoolFactor>();
        boolFactor->_terms.push_back(compPredicate);

        auto orTerm = std::make_shared<query::OrTerm>();
        orTerm->_terms.push_back(boolFactor);

        lockedParent<BinaryComparasionPredicateCBH>()->handleOrTerm(orTerm);
    }

private:
    shared_ptr<query::ValueExpr> _left;
    string _comparison;
    shared_ptr<query::ValueExpr> _right;
};


class ComparisonOperatorAdapter : public Adapter {
public:
    ComparisonOperatorAdapter(shared_ptr<ComparisonOperatorCBH> parent,
            MySqlParser::ComparisonOperatorContext* ctx)
    : Adapter(parent, ctx),  _comparisonOperatorCtx(ctx) {}

    void onExit() override {
        lockedParent<ComparisonOperatorCBH>()->handleComparisonOperator(_comparisonOperatorCtx->getText());
    }

private:
    MySqlParser::ComparisonOperatorContext * _comparisonOperatorCtx;
};


class SelectColumnElementAdapter : public Adapter, public FullColumnNameCBH {
public:
    SelectColumnElementAdapter(shared_ptr<SelectColumnElementCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(parent, ctx) {}

    void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) override {
        lockedParent<SelectColumnElementCBH>()->handleColumnElement(columnValueExpr);
    }
};


class UidAdapter : public Adapter {
public:
    UidAdapter(shared_ptr<UidCBH> parent, MySqlParser::UidContext* ctx)
    : Adapter(parent, ctx), _uidContext(ctx) {}

    void onExit() override {
        LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
        // Fetching the string from a Uid shortcuts a large part of the syntax tree defined under Uid
        // (see MySqlParser.g4). If Adapters for any nodes in the tree below Uid are implemented then
        // it will have to be handled and this shortcut may not be taken.
        lockedParent<UidCBH>()->handleUidString(_uidContext->getText());
    }

private:
    MySqlParser::UidContext* _uidContext;
};


class ConstantAdapter : public Adapter {
public:
    ConstantAdapter(shared_ptr<ConstantCBH> parent, MySqlParser::ConstantContext* ctx)
    : Adapter(parent, ctx), _constantContext(ctx) {}

    void onExit() override {
        lockedParent<ConstantCBH>()->handleConstant(_constantContext->getText());
    }

private:
    MySqlParser::ConstantContext* _constantContext;
};


/// MySqlListener impl


MySqlListener::MySqlListener() {
    _rootAdapter = std::make_shared<RootAdapter>();
    _adapterStack.push(_rootAdapter);
}


std::shared_ptr<query::SelectStmt> MySqlListener::getSelectStatement() const {
    return _rootAdapter->getSelectStatement();
}


// Create and push an Adapter onto the context stack, using the current top of the stack as a callback handler
// for the new Adapter. Returns the new Adapter.
template<typename ParentCBH, typename ChildAdapter, typename Context>
std::shared_ptr<ChildAdapter> MySqlListener::pushAdapterStack(Context * ctx) {
    auto p = std::dynamic_pointer_cast<ParentCBH>(_adapterStack.top());
    if (nullptr == p) {
        int status;
        std::ostringstream msg;
        msg << "can't acquire expected Adapter " <<
                abi::__cxa_demangle(typeid(ParentCBH).name(),0,0,&status) <<
                " from top of listenerStack.";
        LOGS(_log, LOG_LVL_ERROR, msg.str());
        throw adapter_order_error(msg.str());
    }
    auto childAdapter = std::make_shared<ChildAdapter>(p, ctx);
    _adapterStack.push(childAdapter);
    childAdapter->onEnter();
    return childAdapter;
}


template<typename ChildAdapter>
void MySqlListener::popAdapterStack() {
    shared_ptr<Adapter> adapterPtr = _adapterStack.top();
    adapterPtr->onExit();
    _adapterStack.pop();
    shared_ptr<ChildAdapter> derivedPtr = dynamic_pointer_cast<ChildAdapter>(adapterPtr);
    if (nullptr == derivedPtr) {
        int status;
        LOGS(_log, LOG_LVL_ERROR, "Top of listenerStack was not of expected type. " <<
                "Expected: " << abi::__cxa_demangle(typeid(ChildAdapter).name(),0,0,&status) <<
                " Actual: " << abi::__cxa_demangle(typeid(adapterPtr).name(),0,0,&status) <<
                " Are there out of order or unhandled listener exits?");
        // might want to throw here...?
    }
}


// might want to use this in popAdapterStack?
template<typename ChildAdapter>
std::shared_ptr<ChildAdapter> MySqlListener::adapterStackTop() const {
    shared_ptr<Adapter> adapterPtr = _adapterStack.top();
    shared_ptr<ChildAdapter> derivedPtr = dynamic_pointer_cast<ChildAdapter>(adapterPtr);
    if (nullptr == derivedPtr) {
        int status;
        LOGS(_log, LOG_LVL_ERROR, "Top of listenerStack was not of expected type. " <<
                "Expected: " << abi::__cxa_demangle(typeid(ChildAdapter).name(),0,0,&status) <<
                " Actual: " << abi::__cxa_demangle(typeid(adapterPtr).name(),0,0,&status));
        // might want to throw here?
    }
    return derivedPtr;
}


// MySqlListener class methods


void MySqlListener::enterRoot(MySqlParser::RootContext * ctx) {
    // root is pushed by the ctor (and popped by the dtor)
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
}


void MySqlListener::exitRoot(MySqlParser::RootContext * ctx) {
    // root is pushed by the ctor (and popped by the dtor)
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
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
UNHANDLED(SelectFunctionElement)
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
UNHANDLED(Expressions)
UNHANDLED(ExpressionsWithDefaults)
UNHANDLED(Constants)
UNHANDLED(SimpleStrings)
UNHANDLED(UserVariables)
UNHANDLED(DefaultValue)
UNHANDLED(ExpressionOrDefault)
UNHANDLED(IfExists)
UNHANDLED(IfNotExists)
UNHANDLED(SpecificFunctionCall)
UNHANDLED(AggregateFunctionCall)
UNHANDLED(ScalarFunctionCall)
UNHANDLED(UdfFunctionCall)
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
UNHANDLED(AggregateWindowedFunction)
UNHANDLED(ScalarFunctionName)
UNHANDLED(PasswordFunctionClause)
UNHANDLED(FunctionArgs)
UNHANDLED(FunctionArg)
UNHANDLED(IsExpression)
UNHANDLED(NotExpression)
UNHANDLED(LogicalExpression)
UNHANDLED(SoundsLikePredicate)
UNHANDLED(InPredicate)
UNHANDLED(SubqueryComparasionPredicate)
UNHANDLED(BetweenPredicate)
UNHANDLED(IsNullPredicate)
UNHANDLED(LikePredicate)
UNHANDLED(RegexpPredicate)
UNHANDLED(UnaryExpressionAtom)
UNHANDLED(CollateExpressionAtom)
UNHANDLED(SubqueryExpessionAtom)
UNHANDLED(MysqlVariableExpressionAtom)
UNHANDLED(NestedExpressionAtom)
UNHANDLED(NestedRowExpressionAtom)
UNHANDLED(MathExpressionAtom)
UNHANDLED(IntervalExpressionAtom)
UNHANDLED(ExistsExpessionAtom)
UNHANDLED(FunctionCallExpressionAtom)
UNHANDLED(BinaryExpressionAtom)
UNHANDLED(BitExpressionAtom)
UNHANDLED(UnaryOperator)
UNHANDLED(LogicalOperator)
UNHANDLED(BitOperator)
UNHANDLED(MathOperator)
UNHANDLED(CharsetNameBase)
UNHANDLED(TransactionLevelBase)
UNHANDLED(PrivilegesBase)
UNHANDLED(IntervalTypeBase)
UNHANDLED(DataTypeBase)
UNHANDLED(KeywordsCanBeId)
UNHANDLED(FunctionNameBase)

}}} // namespace lsst::qserv::parser
