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
#include "query/SelectStmt.h"

#include <cxxabi.h>
#include <vector>

using namespace std;

namespace lsst {
namespace qserv {
namespace parser {

LOG_LOGGER _log = LOG_GET("lsst.qserv.MySqlListener");

/// Callback Handler classes

class UidCBH {
public:
    virtual ~UidCBH() {}
    virtual void handleUidString(const std::string& string) = 0;
};

/// Adapter classes

class Adapter {
public:
    virtual ~Adapter() {}
};

namespace {

class RootAdapter : public Adapter {
public:
    query::SelectStmt::Ptr selectStatement();
private:
    query::SelectStmt::Ptr _selectStatement;
};


class DMLAdapter : public Adapter {
};


class SelectAdapter : public Adapter {
};


class SelectElementsAdapter : public Adapter {
};


class SelectColumnElementAdapter : public Adapter, public UidCBH {
    virtual void handleUidString(const std::string& string) {
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__ << string);
    }
};





} // end namespace


template<typename ParentAdapter, typename ChildAdapter>
std::shared_ptr<ChildAdapter> MySqlListener::pushAdapterStack() {
    auto p = std::dynamic_pointer_cast<ParentAdapter>(_adapterStack.top());
    if (nullptr == p) {
            int status;
            LOGS(_log, LOG_LVL_ERROR, "can't acquire expected Adapter " <<
                    abi::__cxa_demangle(typeid(ParentAdapter).name(),0,0,&status) <<
                    " from top of listenerStack."); // todo add some type names
        // might want to throw here...?
        return nullptr;
    }
    auto childAdapter = std::make_shared<ChildAdapter>();
    _adapterStack.push(childAdapter);
    return childAdapter;
}


template<typename ChildAdapter>
void MySqlListener::popAdapterStack() {
    shared_ptr<Adapter> listenerPtr = _adapterStack.top();
    _adapterStack.pop();
    shared_ptr<ChildAdapter> derivedPtr = dynamic_pointer_cast<ChildAdapter>(listenerPtr);
    if (nullptr == derivedPtr) {
        int status;
        LOGS(_log, LOG_LVL_ERROR, "Top of listenerStack was not of expected type. " <<
                "Expected: " << abi::__cxa_demangle(typeid(ChildAdapter).name(),0,0,&status) <<
                " Actual: " << abi::__cxa_demangle(typeid(listenerPtr).name(),0,0,&status) <<
                " Are there out of order or unhandled listener exits?"); // todo add some type names
        // might want to throw here...?
    }
}


// might want to use this in popAdapterStack?
template<typename ChildAdapter>
std::shared_ptr<ChildAdapter> MySqlListener::adapterStackTop() const {
    shared_ptr<Adapter> listenerPtr = _adapterStack.top();
    shared_ptr<ChildAdapter> derivedPtr = dynamic_pointer_cast<ChildAdapter>(listenerPtr);
    if (nullptr == derivedPtr) {
        int status;
        LOGS(_log, LOG_LVL_ERROR, "Top of listenerStack was not of expected type. " <<
                "Expected: " << abi::__cxa_demangle(typeid(ChildAdapter).name(),0,0,&status) <<
                " Actual: " << abi::__cxa_demangle(typeid(listenerPtr).name(),0,0,&status));
        // might want to throw here?
    }
    return derivedPtr;
}


void MySqlListener::enterRoot(MySqlParser::RootContext * ctx) {
    // since there's no parent listener on the stack for a root listener, we don't use the template push
    // function, we just push the first item onto the stack by hand like so:
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    _rootAdapter = std::make_shared<RootAdapter>();
    _adapterStack.push(_rootAdapter);
}


void MySqlListener::exitRoot(MySqlParser::RootContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<RootAdapter>();
}


void MySqlListener::enterDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<RootAdapter, DMLAdapter>();
}


void MySqlListener::exitDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<DMLAdapter>();
}


void MySqlListener::enterSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<DMLAdapter, SelectAdapter>();
}


void MySqlListener::exitSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<SelectAdapter>();
}


void MySqlListener::enterSelectElements(MySqlParser::SelectElementsContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<SelectAdapter, SelectElementsAdapter>();
}


void MySqlListener::exitSelectElements(MySqlParser::SelectElementsContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<SelectElementsAdapter>();
}


void MySqlListener::enterSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushAdapterStack<SelectElementsAdapter, SelectColumnElementAdapter>();
}


void MySqlListener::exitSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popAdapterStack<SelectColumnElementAdapter>();
}


void MySqlListener::enterUid(MySqlParser::UidContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    shared_ptr<UidCBH> handler = adapterStackTop<UidCBH>();
//    shared_ptr<SelectColumnElementAdapter> handler = adapterStackTop<SelectColumnElementAdapter>();
}


void MySqlListener::exitUid(MySqlParser::UidContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);

}



}}} // namespace lsst::qserv::parser
