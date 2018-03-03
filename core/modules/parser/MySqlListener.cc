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

namespace lsst {
namespace qserv {
namespace parser {

LOG_LOGGER _log = LOG_GET("lsst.qserv.MySqlListener");


/// Callback Handler classes

// NullCBH is a class that can't be instantiated, it exists as a placeholder for
// Adapters that don't use their _parent, so that they may have a weak_ptr to a CBH that is a nullptr.
class NullCBH {
private:
    NullCBH() {}
};


class DMLStatementCBH {
public:
    virtual ~DMLStatementCBH() {}
    virtual void handleDMLStatement(shared_ptr<query::SelectStmt> selectStatement) = 0;
};


class SelectStatementCBH {
public:
    virtual ~SelectStatementCBH() {}
    virtual void handleSelectStatement(shared_ptr<query::SelectStmt> selectStatement) = 0;
};


class QuerySpecificationCBH {
public:
    virtual ~QuerySpecificationCBH() {}
    virtual void handleQuerySpecification(shared_ptr<query::SelectList> selectList,
                                          shared_ptr<query::FromList> fromList,
                                          shared_ptr<query::WhereClause> whereClause) = 0;
};


class SelectElementsCBH {
public:
    virtual ~SelectElementsCBH() {}
    virtual void handleSelectList(shared_ptr<query::SelectList> selectList) = 0;
};


class FullColumnNameCBH {
public:
    virtual ~FullColumnNameCBH() {}
    virtual void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) = 0;
};


class TableNameCBH {
public:
    virtual ~TableNameCBH() {}
    virtual void handleTableName(const std::string& string) = 0;
};


class FromClauseCBH {
public:
    virtual ~FromClauseCBH() {}
    virtual void handleFromClause(shared_ptr<query::FromList> fromList, shared_ptr<query::WhereClause> whereClause) = 0;
};


class TableSourcesCBH {
public:
    virtual ~TableSourcesCBH() {}
    virtual void handleTableSources(query::TableRefListPtr tableRefList) = 0;
};

class TableSourceCBH {
public:
    virtual ~TableSourceCBH() {}
    virtual void handleTableSource(shared_ptr<query::TableRef> tableRef) = 0;
};


class TableSourceItemCBH {
public:
    virtual ~TableSourceItemCBH() {}
    virtual void handleTableSourceItem(shared_ptr<query::TableRef> tableRef) = 0;
};


class UidCBH {
public:
    virtual ~UidCBH() {}
    virtual void handleUidString(const std::string& string) = 0;
};


class FullIdCBH {
public:
    virtual ~FullIdCBH() {}
    virtual void handleFullIdString(const std::string& string) = 0;
};


class DecimalLiteralCBH {
public:
    virtual ~DecimalLiteralCBH() {}
    virtual void handleDecimalLiteral(const string& text) = 0;
};


class StringLiteralCBH {
public:
    virtual ~StringLiteralCBH() {}
};


class ConstantExpressionAtomCBH {
public:
    virtual ~ConstantExpressionAtomCBH() {}
    virtual void handleDecimalLiteral(const string& text) = 0;
};


class ExpressionAtomPredicateCBH {
public:
    virtual ~ExpressionAtomPredicateCBH() {}
    virtual void handleValueExpr(shared_ptr<query::ValueExpr> valueExpr) = 0;
};


class ComparisonOperatorCBH {
public:
    virtual ~ComparisonOperatorCBH() {}
    virtual void handleComparisonOperator(const string& text) = 0;
};


class SelectColumnElementCBH {
public:
    virtual ~SelectColumnElementCBH() {}
    virtual void handleColumnElement(shared_ptr<query::ValueExpr> columnElement) = 0;
};


class FullColumnNameExpressionAtomCBH {
public:
    virtual ~FullColumnNameExpressionAtomCBH() {}
    virtual void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) = 0;

};


class BinaryComparasionPredicateCBH {
public:
    virtual ~BinaryComparasionPredicateCBH() {}
    virtual void handleOrTerm(shared_ptr<query::OrTerm> orTerm) = 0;
};


class PredicateExpressionCBH {
public:
    virtual ~PredicateExpressionCBH() {}
    virtual void handleOrTerm(shared_ptr<query::OrTerm> orTerm, antlr4::ParserRuleContext* childCtx) = 0;
};

/// Adapter classes


class Adapter {
public:
    Adapter(antlr4::ParserRuleContext* ctx) : _ctx(ctx) {}
    virtual ~Adapter() {}

    // onExit is called when the Adapter is being popped from the context stack
    virtual void onExit() {}
protected:
    antlr4::ParserRuleContext* _ctx;
    weak_ptr<NullCBH> _parent;
};

namespace {


class RootAdapter : public Adapter, public DMLStatementCBH {
public:
    RootAdapter(antlr4::ParserRuleContext* ctx) : Adapter(ctx) {}

    shared_ptr<query::SelectStmt> getSelectStatement() { return _selectStatement; }

    virtual void handleDMLStatement(shared_ptr<query::SelectStmt> selectStatement) {
        _selectStatement = selectStatement;
    }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
};


// Between Root and DMLStatement, there are skipped statements: `sqlStatements` and `sqlStatement`.
// Adapters and enter/exit handlers for these may need to be implemented, TBD.


class DMLStatementAdapter : public Adapter, public SelectStatementCBH {
public:
    DMLStatementAdapter(shared_ptr<DMLStatementCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleSelectStatement(shared_ptr<query::SelectStmt> selectStatement) {
        _selectStatement = selectStatement;
    }

    void onExit() override {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleDMLStatement(_selectStatement);
        }
    }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
    weak_ptr<DMLStatementCBH> _parent;
};


class SelectStatmentAdapter : public Adapter, public QuerySpecificationCBH {
public:
    SelectStatmentAdapter(shared_ptr<SelectStatementCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleQuerySpecification(shared_ptr<query::SelectList> selectList,
                                          shared_ptr<query::FromList> fromList,
                                          shared_ptr<query::WhereClause> whereClause) {
        _selectList = selectList;
        _fromList = fromList;
        _whereClause = whereClause;
    }

    void onExit() override {
        auto parent = _parent.lock();
        if (parent) {
            shared_ptr<query::SelectStmt> selectStatement = make_shared<query::SelectStmt>();
            selectStatement->setSelectList(_selectList);
            selectStatement->setFromList(_fromList);
            selectStatement->setWhereClause(_whereClause);
            parent->handleSelectStatement(selectStatement);
        }
    }

private:
    shared_ptr<query::SelectList> _selectList;
    shared_ptr<query::FromList> _fromList;
    shared_ptr<query::WhereClause> _whereClause;
    weak_ptr<SelectStatementCBH> _parent;
};


class QuerySpecificationAdapter : public Adapter, public SelectElementsCBH, public FromClauseCBH {
public:
    QuerySpecificationAdapter(shared_ptr<QuerySpecificationCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleSelectList(shared_ptr<query::SelectList> selectList) {
        _selectList = selectList;
    }

    virtual void handleFromClause(shared_ptr<query::FromList> fromList, shared_ptr<query::WhereClause> whereClause) {
        _fromList = fromList;
        _whereClause = whereClause;
    }

    void onExit() override {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleQuerySpecification(_selectList, _fromList, _whereClause);
        }
    }

private:
    shared_ptr<query::WhereClause> _whereClause;
    shared_ptr<query::FromList> _fromList;
    shared_ptr<query::SelectList> _selectList;
    weak_ptr<QuerySpecificationCBH> _parent;
};


class SelectElementsAdapter : public Adapter, public SelectColumnElementCBH {
public:
    SelectElementsAdapter(shared_ptr<SelectElementsCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleColumnElement(shared_ptr<query::ValueExpr> columnElement) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << "adding column to the ValueExprPtrVector: " << columnElement);
        SelectListFactory::addValueExpr(_selectList, columnElement);
    }

    void onExit() override {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleSelectList(_selectList);
        }
    }

private:
    std::shared_ptr<query::SelectList> _selectList{std::make_shared<query::SelectList>()};
    weak_ptr<SelectElementsCBH> _parent;
};


class FromClauseAdapter : public Adapter, public TableSourcesCBH, public PredicateExpressionCBH {
public:
    FromClauseAdapter(shared_ptr<FromClauseCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleTableSources(query::TableRefListPtr tableRefList) {
        _tableRefList = tableRefList;
    }

    virtual void handleOrTerm(shared_ptr<query::OrTerm> orTerm, antlr4::ParserRuleContext* childCtx) {
        MySqlParser::FromClauseContext* ctx = dynamic_cast<MySqlParser::FromClauseContext*>(_ctx);
        if (nullptr == ctx) {
            throw MySqlListener::adapter_order_error("FromClauseAdapter's _ctx could not be cast to a FromClauseContext.");
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
        auto parent = _parent.lock();
        if (parent) {
            shared_ptr<query::FromList> fromList = make_shared<query::FromList>(_tableRefList);
            parent->handleFromClause(fromList, _whereClause);
        }
    }

private:
    shared_ptr<query::WhereClause> _whereClause{std::make_shared<query::WhereClause>()};
    query::TableRefListPtr _tableRefList;
    weak_ptr<FromClauseCBH> _parent;
};


class TableSourcesAdapter : public Adapter, public TableSourceCBH {
public:
    TableSourcesAdapter(shared_ptr<TableSourcesCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleTableSource(shared_ptr<query::TableRef> tableRef) {
        _tableRefList->push_back(tableRef);
    }

    void onExit() override {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleTableSources(_tableRefList);
        }
    }

private:
    query::TableRefListPtr _tableRefList{make_shared<query::TableRefList>()};
    weak_ptr<TableSourcesCBH> _parent;
};


class TableSourceAdapter : public Adapter, public TableSourceItemCBH{
public:
    TableSourceAdapter(shared_ptr<TableSourceCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleTableSourceItem(shared_ptr<query::TableRef> tableRef) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << tableRef);
        _tableRef = tableRef;
    }

    void onExit() override {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleTableSource(_tableRef);
        }
    }

private:
    shared_ptr<query::TableRef> _tableRef;
    weak_ptr<TableSourceCBH> _parent;
};


class TableSourceItemAdapter : public Adapter, public TableNameCBH {
public:
    TableSourceItemAdapter(shared_ptr<TableSourceItemCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleTableName(const std::string& string) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << string);
        _table = string;
    }

    void onExit() override {
        auto parent = _parent.lock();
        if (parent) {
            shared_ptr<query::TableRef> tableRef = make_shared<query::TableRef>(_db, _table, _alias);
            parent->handleTableSourceItem(tableRef);
        }
    }

protected:
    weak_ptr<TableSourceItemCBH> _parent;
    std::string _db;
    std::string _table;
    std::string _alias;
};


class TableNameAdapter : public Adapter , public FullIdCBH {
public:
    TableNameAdapter(shared_ptr<TableNameCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleFullIdString(const std::string& string) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << string);
        auto parent = _parent.lock();
        if (parent) {
            parent->handleTableName(string);
        }
    }

protected:
    weak_ptr<TableNameCBH> _parent;
};


class DecimalLiteralAdapter : public Adapter {
public:
    DecimalLiteralAdapter(shared_ptr<DecimalLiteralCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    void onEnter(MySqlParser::DecimalLiteralContext * ctx) {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleDecimalLiteral(ctx->DECIMAL_LITERAL()->getText());
        }
    }

private:
    weak_ptr<DecimalLiteralCBH> _parent;
};


class StringLiteralAdapter : public Adapter {
public:
    StringLiteralAdapter(shared_ptr<StringLiteralCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

private:
    weak_ptr<StringLiteralCBH> _parent;
};


class FullIdAdapter : public Adapter, public UidCBH {
public:
    FullIdAdapter(shared_ptr<FullIdCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual ~FullIdAdapter() {}

    virtual void handleUidString(const std::string& string) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << string);
        auto parent = _parent.lock();
        if (parent) {
            parent->handleFullIdString(string);
        }
    }

protected:
    weak_ptr<FullIdCBH> _parent;
};


class FullColumnNameAdapter : public Adapter, public UidCBH {
public:
    FullColumnNameAdapter(shared_ptr<FullColumnNameCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleUidString(const std::string& string) {
        LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
        auto parent = _parent.lock();
        if (parent) {
            auto valueFactor = ValueFactorFactory::newColumnColumnFactor("", "", string);
            auto valueExpr = std::make_shared<query::ValueExpr>();
            ValueExprFactory::addValueFactor(valueExpr, valueFactor);
            parent->handleFullColumnName(valueExpr);
        }
    }

protected:
    weak_ptr<FullColumnNameCBH> _parent;
};


class ConstantExpressionAtomAdapter : public Adapter, public DecimalLiteralCBH {
public:
    ConstantExpressionAtomAdapter(shared_ptr<ConstantExpressionAtomCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleDecimalLiteral(const string& text) override {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleDecimalLiteral(text);
        }
    }

private:
    weak_ptr<ConstantExpressionAtomCBH> _parent;
};


class FullColumnNameExpressionAtomAdapter : public Adapter, public FullColumnNameCBH {
public:
    FullColumnNameExpressionAtomAdapter(shared_ptr<FullColumnNameExpressionAtomCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) {
        LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
        auto parent = _parent.lock();
        if (parent) {
            parent->handleFullColumnName(columnValueExpr);
        }
    }

private:
    weak_ptr<FullColumnNameExpressionAtomCBH> _parent;
};


class ExpressionAtomPredicateAdapter : public Adapter, public ConstantExpressionAtomCBH,
        public FullColumnNameExpressionAtomCBH {
public:
    ExpressionAtomPredicateAdapter(shared_ptr<ExpressionAtomPredicateCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    void handleDecimalLiteral(const string& text) override {
        auto parent = _parent.lock();
        if (parent) {
            query::ValueExpr::FactorOp factorOp;
            factorOp.factor =  query::ValueFactor::newConstFactor(text);
            auto valueExpr = std::make_shared<query::ValueExpr>();
            valueExpr->getFactorOps().push_back(factorOp);
            parent->handleValueExpr(valueExpr);
        }
    }

    void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) override {
        LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
        auto parent = _parent.lock();
        if (parent) {
            parent->handleValueExpr(columnValueExpr);
        }
    }


private:
    weak_ptr<ExpressionAtomPredicateCBH> _parent;
};


class PredicateExpressionAdapter : public Adapter, public BinaryComparasionPredicateCBH {
public:
    PredicateExpressionAdapter(shared_ptr<PredicateExpressionCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleOrTerm(shared_ptr<query::OrTerm> orTerm) {
        _orTerm = orTerm;
    }

    void onExit() {
        auto parent = _parent.lock();
        if (!parent || !_orTerm) {
            return;
        }
        parent->handleOrTerm(_orTerm, _ctx);
    }

private:
    shared_ptr<query::OrTerm> _orTerm;
    weak_ptr<PredicateExpressionCBH> _parent;
};


class BinaryComparasionPredicateAdapter : public Adapter, public ExpressionAtomPredicateCBH,
        public ComparisonOperatorCBH {
public:
    BinaryComparasionPredicateAdapter(shared_ptr<BinaryComparasionPredicateCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

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

    void handleValueExpr(shared_ptr<query::ValueExpr> valueExpr) override {
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

        auto parent = _parent.lock();
        if (!parent) {
            return;
        }

        // todo test that _left and _right are not null?

        auto compPredicate = std::make_shared<query::CompPredicate>();
        compPredicate->left = _left;

        // We need to remove the coupling between the query classes and the parser classes, in this case where
        // the query classes use the integer token types instead of some other system. For now this switch
        // statement allows us to go from the token string to the SqlSQL2Tokens type defined by the antlr2/3
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

        parent->handleOrTerm(orTerm);
    }

private:
    shared_ptr<query::ValueExpr> _left;
    string _comparison;
    shared_ptr<query::ValueExpr> _right;
    weak_ptr<BinaryComparasionPredicateCBH> _parent;
};


class ComparisonOperatorAdapter : public Adapter {
public:
    ComparisonOperatorAdapter(shared_ptr<ComparisonOperatorCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    void onEnter(MySqlParser::ComparisonOperatorContext * ctx) {
        auto parent = _parent.lock();
        if (parent != nullptr) {
            parent->handleComparisonOperator(ctx->getText());
        }
    }

private:
    weak_ptr<ComparisonOperatorCBH> _parent;
};


class SelectColumnElementAdapter : public Adapter, public FullColumnNameCBH {
public:
    SelectColumnElementAdapter(shared_ptr<SelectColumnElementCBH> parent, antlr4::ParserRuleContext* ctx)
    : Adapter(ctx), _parent(parent) {}

    virtual void handleFullColumnName(shared_ptr<query::ValueExpr> columnValueExpr) {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleColumnElement(columnValueExpr);
        }
    }

private:
    weak_ptr<SelectColumnElementCBH> _parent;
};

} // end namespace


// Create and push an Adapter onto the context stack, using the current top of the stack as a callback handler
// for the new Adapter. Returns the new Adapter.
template<typename ParentCBH, typename ChildAdapter>
std::shared_ptr<ChildAdapter> MySqlListener::pushAdapterStack(antlr4::ParserRuleContext* ctx) {
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
    return childAdapter;
}


// Create and push an Adapter onto the context stack. Does not install a callback handler into the Adapter.
template<typename ChildAdapter>
std::shared_ptr<ChildAdapter> MySqlListener::pushAdapterStack(antlr4::ParserRuleContext* ctx) {
    auto childAdapter = std::make_shared<ChildAdapter>(ctx);
    _adapterStack.push(childAdapter);
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
                " Are there out of order or unhandled listener exits?"); // todo add some type names
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
    // since there's no parent listener on the stack for a root listener, we don't use the template push
    // function, we just push the first item onto the stack by hand like so:
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    _rootAdapter = pushAdapterStack<RootAdapter>(ctx);
}


void MySqlListener::exitRoot(MySqlParser::RootContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    auto rootAdapter = adapterStackTop<RootAdapter>();
    popAdapterStack<RootAdapter>();
    _selectStatement = rootAdapter->getSelectStatement();
}


void MySqlListener::enterDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<DMLStatementCBH, DMLStatementAdapter>(ctx);
}


void MySqlListener::exitDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<DMLStatementAdapter>();
}


void MySqlListener::enterSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<SelectStatementCBH, SelectStatmentAdapter>(ctx);
}


void MySqlListener::exitSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<SelectStatmentAdapter>();
}


void MySqlListener::enterQuerySpecification(MySqlParser::QuerySpecificationContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<QuerySpecificationCBH, QuerySpecificationAdapter>(ctx);
}


void MySqlListener::exitQuerySpecification(MySqlParser::QuerySpecificationContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<QuerySpecificationAdapter>();
}


void MySqlListener::enterSelectElements(MySqlParser::SelectElementsContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<SelectElementsCBH, SelectElementsAdapter>(ctx);
}


void MySqlListener::exitSelectElements(MySqlParser::SelectElementsContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<SelectElementsAdapter>();
}


void MySqlListener::enterSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<SelectColumnElementCBH, SelectColumnElementAdapter>(ctx);
}


void MySqlListener::exitSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<SelectColumnElementAdapter>();
}


void MySqlListener::enterFromClause(MySqlParser::FromClauseContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<FromClauseCBH, FromClauseAdapter>(ctx);
}


void MySqlListener::exitFromClause(MySqlParser::FromClauseContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<FromClauseAdapter>();
}


void MySqlListener::enterTableSources(MySqlParser::TableSourcesContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<TableSourcesCBH, TableSourcesAdapter>(ctx);
}


void MySqlListener::exitTableSources(MySqlParser::TableSourcesContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<TableSourcesAdapter>();
}


void MySqlListener::enterTableSourceBase(MySqlParser::TableSourceBaseContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<TableSourceCBH, TableSourceAdapter>(ctx);
}


void MySqlListener::exitTableSourceBase(MySqlParser::TableSourceBaseContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<TableSourceAdapter>();
}


void MySqlListener::enterAtomTableItem(MySqlParser::AtomTableItemContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<TableSourceItemCBH, TableSourceItemAdapter>(ctx);
}


void MySqlListener::exitAtomTableItem(MySqlParser::AtomTableItemContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<TableSourceItemAdapter>();
}


void MySqlListener::enterTableName(MySqlParser::TableNameContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<TableNameCBH, TableNameAdapter>(ctx);
}


void MySqlListener::exitTableName(MySqlParser::TableNameContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<TableNameAdapter>();
}


void MySqlListener::enterFullColumnName(MySqlParser::FullColumnNameContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<FullColumnNameCBH, FullColumnNameAdapter>(ctx);
}


void MySqlListener::exitFullColumnName(MySqlParser::FullColumnNameContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<FullColumnNameAdapter>();
}



void MySqlListener::enterFullId(MySqlParser::FullIdContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<FullIdCBH, FullIdAdapter>(ctx);
}


void MySqlListener::exitFullId(MySqlParser::FullIdContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<FullIdAdapter>();
}


void MySqlListener::enterUid(MySqlParser::UidContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    shared_ptr<UidCBH> handler = adapterStackTop<UidCBH>();
    if (handler && ctx && ctx->simpleId() && ctx->simpleId()->ID()) {
        handler->handleUidString(ctx->simpleId()->ID()->getText());
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Unhandled UID: " << ctx->simpleId()->ID()->getText());
    }
}


void MySqlListener::exitUid(MySqlParser::UidContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
}


void MySqlListener::enterDecimalLiteral(MySqlParser::DecimalLiteralContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<DecimalLiteralCBH, DecimalLiteralAdapter>(ctx);
    auto adapter = adapterStackTop<DecimalLiteralAdapter>();
    if (adapter) {
        adapter->onEnter(ctx);
    }
}


void MySqlListener::exitDecimalLiteral(MySqlParser::DecimalLiteralContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<DecimalLiteralAdapter>();
}


void MySqlListener::enterStringLiteral(MySqlParser::StringLiteralContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<StringLiteralCBH, StringLiteralAdapter>(ctx);
}


void MySqlListener::exitStringLiteral(MySqlParser::StringLiteralContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<StringLiteralAdapter>();
}


ENTER_EXIT_PARENT(PredicateExpression)


void MySqlListener::enterExpressionAtomPredicate(MySqlParser::ExpressionAtomPredicateContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<ExpressionAtomPredicateCBH, ExpressionAtomPredicateAdapter>(ctx);
}


void MySqlListener::exitExpressionAtomPredicate(MySqlParser::ExpressionAtomPredicateContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<ExpressionAtomPredicateAdapter>();
}


ENTER_EXIT_PARENT(BinaryComparasionPredicate)


void MySqlListener::enterConstantExpressionAtom(MySqlParser::ConstantExpressionAtomContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<ConstantExpressionAtomCBH, ConstantExpressionAtomAdapter>(ctx);
}


void MySqlListener::exitConstantExpressionAtom(MySqlParser::ConstantExpressionAtomContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<ConstantExpressionAtomAdapter>();
}


ENTER_EXIT_PARENT(FullColumnNameExpressionAtom)


void MySqlListener::enterComparisonOperator(MySqlParser::ComparisonOperatorContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<ComparisonOperatorCBH, ComparisonOperatorAdapter>(ctx);
    auto adapter = adapterStackTop<ComparisonOperatorAdapter>();
    if (adapter) {
        adapter->onEnter(ctx);
    }
}


void MySqlListener::exitComparisonOperator(MySqlParser::ComparisonOperatorContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<ComparisonOperatorAdapter>();
}



}}} // namespace lsst::qserv::parser
