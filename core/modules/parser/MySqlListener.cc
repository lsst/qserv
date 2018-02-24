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
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "SelectListFactory.h"

#include <cxxabi.h>
#include <vector>

using namespace std;

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


class QuerySpecificationCBH {
public:
    virtual ~QuerySpecificationCBH() {}
    virtual void handleSelectList(shared_ptr<query::SelectList> selectList) = 0;
};


class SelectElementsCBH {
public:
    virtual ~SelectElementsCBH() {}
    virtual void handleSelectList(shared_ptr<query::SelectList> selectList) = 0;
};


class FullColumnNameCBH {
public:
    virtual ~FullColumnNameCBH() {}
    virtual void handleFullColumnName(shared_ptr<query::ValueExpr> column) = 0;
};


class TableNameCBH {
public:
    virtual ~TableNameCBH() {}
    virtual void handleTableName(const std::string& string) = 0;
};


class TableSourceItemCBH {
public:
    virtual ~TableSourceItemCBH() {}
    virtual void handleTableSourceItem(const std::string& string) = 0;
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


/// Adapter classes


class Adapter {
public:
    virtual ~Adapter() {}

    // onExit is called when the Adapter is being popped from the context stack
    virtual void onExit() {}
protected:
    weak_ptr<NullCBH> _parent;
};

namespace {

class RootAdapter : public Adapter {
public:
    RootAdapter() {}
    ~RootAdapter() {}
private:
};


class DMLAdapter : public Adapter {
public:
    DMLAdapter() {}
};


class SelectStatmentAdapter : public Adapter, public QuerySpecificationCBH {
public:
    SelectStatmentAdapter() : _selectStatement(make_shared<query::SelectStmt>()) {}

    virtual void handleSelectList(shared_ptr<query::SelectList> selectList) {
        _selectStatement->setSelectList(selectList);
    }

    virtual void onExit() {
        LOGS(_log, LOG_LVL_DEBUG, "SelectStatement: " << *_selectStatement);
    }

private:
    shared_ptr<query::SelectStmt> _selectStatement;
};


class QuerySpecificationAdapter : public Adapter, public SelectElementsCBH {
public:
    QuerySpecificationAdapter(shared_ptr<QuerySpecificationCBH> parent) : _parent(parent) {}

    virtual void handleSelectList(shared_ptr<query::SelectList> selectList) {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleSelectList(selectList);
        }
    }

private:
    weak_ptr<QuerySpecificationCBH> _parent;
};


class SelectElementsAdapter : public Adapter, public FullColumnNameCBH {
public:
    SelectElementsAdapter(shared_ptr<SelectElementsCBH> parent) : _parent(parent) {}

    virtual void handleFullColumnName(shared_ptr<query::ValueExpr> column) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << "adding column to the ValueExprPtrVector: " << column);
        SelectListFactory::addValueExpr(_selectList, column);
    }

    virtual void onExit() {
        auto parent = _parent.lock();
        if (parent) {
            parent->handleSelectList(_selectList);
        }
    }

private:
    std::shared_ptr<query::SelectList> _selectList{std::make_shared<query::SelectList>()};
    weak_ptr<SelectElementsCBH> _parent;
};


class FromClauseAdapter : public Adapter {
public:
    FromClauseAdapter() {}
    virtual ~FromClauseAdapter() {}
};


class TableSourcesAdapter : public Adapter {
public:
    TableSourcesAdapter() {}
};


class TableSourceAdapter : public Adapter, public TableSourceItemCBH{
public:
    TableSourceAdapter() {}
    virtual void handleTableSourceItem(const std::string& tableSourceItem) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << tableSourceItem <<
                " todo this is where the table source would get added to the list of table sources in the qyery");
    }
};


class TableSourceItemAdapter : public Adapter, public TableNameCBH {
public:
    TableSourceItemAdapter(shared_ptr<TableSourceItemCBH> parent) : _parent(parent) {}
    virtual void handleTableName(const std::string& string) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << string);
        auto parent = _parent.lock();
        if (parent) {
            parent->handleTableSourceItem(string);
        }
    }
protected:
    weak_ptr<TableSourceItemCBH> _parent;
};


class TableNameAdapter : public Adapter , public FullIdCBH {
public:
    TableNameAdapter(shared_ptr<TableNameCBH> parent) : _parent(parent) {}
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


class FullIdAdapter : public Adapter, public UidCBH {
public:
    FullIdAdapter(shared_ptr<FullIdCBH> parent) : _parent(parent) {}
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
    FullColumnNameAdapter(shared_ptr<FullColumnNameCBH> parent) : _parent(parent) {}
    virtual void handleUidString(const std::string& string) {
        LOGS(_log, LOG_LVL_ERROR, __PRETTY_FUNCTION__ << " " << string);
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


} // end namespace


// Create and push an Adapter onto the context stack, using the current top of the stack as a callback handler
// for the new Adapter. Returns the new Adapter.
template<typename ParentCBH, typename ChildAdapter>
std::shared_ptr<ChildAdapter> MySqlListener::pushAdapterStack() {
    auto p = std::dynamic_pointer_cast<ParentCBH>(_adapterStack.top());
    if (nullptr == p) {
            int status;
            LOGS(_log, LOG_LVL_ERROR, "can't acquire expected Adapter " <<
                    abi::__cxa_demangle(typeid(ParentCBH).name(),0,0,&status) <<
                    " from top of listenerStack."); // todo add some type names
        // might want to throw here...?
        return nullptr;
    }
    auto childAdapter = std::make_shared<ChildAdapter>(p);
    _adapterStack.push(childAdapter);
    return childAdapter;
}


// Create and push an Adapter onto the context stack. Does not install a callback handler into the Adapter.
template<typename ChildAdapter>
std::shared_ptr<ChildAdapter> MySqlListener::pushAdapterStack() {
    auto childAdapter = std::make_shared<ChildAdapter>();
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
    _rootAdapter = pushAdapterStack<RootAdapter>();
}


void MySqlListener::exitRoot(MySqlParser::RootContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<RootAdapter>();
}


void MySqlListener::enterDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<DMLAdapter>();
}


void MySqlListener::exitDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<DMLAdapter>();
}


void MySqlListener::enterSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<SelectStatmentAdapter>();
}


void MySqlListener::exitSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<SelectStatmentAdapter>();
}


void MySqlListener::enterQuerySpecification(MySqlParser::QuerySpecificationContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<QuerySpecificationCBH, QuerySpecificationAdapter>();
}


void MySqlListener::exitQuerySpecification(MySqlParser::QuerySpecificationContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<QuerySpecificationAdapter>();
}


void MySqlListener::enterSelectElements(MySqlParser::SelectElementsContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<SelectElementsCBH, SelectElementsAdapter>();
}


void MySqlListener::exitSelectElements(MySqlParser::SelectElementsContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<SelectElementsAdapter>();
}


void MySqlListener::enterSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<FullColumnNameCBH, FullColumnNameAdapter>();
}


void MySqlListener::exitSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<FullColumnNameAdapter>();
}


void MySqlListener::enterFromClause(MySqlParser::FromClauseContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<FromClauseAdapter>();
}


void MySqlListener::exitFromClause(MySqlParser::FromClauseContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<FromClauseAdapter>();
}


void MySqlListener::enterTableSources(MySqlParser::TableSourcesContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<TableSourcesAdapter>();
}


void MySqlListener::exitTableSources(MySqlParser::TableSourcesContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<TableSourcesAdapter>();
}


void MySqlListener::enterTableSourceBase(MySqlParser::TableSourceBaseContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<TableSourceAdapter>();
}


void MySqlListener::exitTableSourceBase(MySqlParser::TableSourceBaseContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<TableSourceAdapter>();
}


void MySqlListener::enterAtomTableItem(MySqlParser::AtomTableItemContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<TableSourceItemCBH, TableSourceItemAdapter>();
}


void MySqlListener::exitAtomTableItem(MySqlParser::AtomTableItemContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<TableSourceItemAdapter>();
}


void MySqlListener::enterTableName(MySqlParser::TableNameContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<TableNameCBH, TableNameAdapter>();
}


void MySqlListener::exitTableName(MySqlParser::TableNameContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<TableNameAdapter>();
}


void MySqlListener::enterFullId(MySqlParser::FullIdContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<FullIdCBH, FullIdAdapter>();
}


void MySqlListener::exitFullId(MySqlParser::FullIdContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<FullIdAdapter>();
}


void MySqlListener::enterUid(MySqlParser::UidContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    shared_ptr<UidCBH> handler = adapterStackTop<UidCBH>();
    if (handler) {
        handler->handleUidString(ctx->simpleId()->ID()->getText());
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Unhandled UID: " << ctx->simpleId()->ID()->getText());
    }
}


void MySqlListener::exitUid(MySqlParser::UidContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
}



}}} // namespace lsst::qserv::parser
