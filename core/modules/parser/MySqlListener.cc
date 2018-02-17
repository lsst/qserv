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

/// Listener classes

class Listener {
public:
    virtual ~Listener() {}
    typedef std::shared_ptr<Listener> Ptr;
};

namespace {

class RootListener : public Listener {
public:
    query::SelectStmt::Ptr selectStatement();
private:
    query::SelectStmt::Ptr _selectStatement;
};


class DMLListener : public Listener {
};


class SelectListener : public Listener {
};


class SelectElementsListener : public Listener {
};


class SelectColumnElementListener : public Listener, public UidCBH {
    virtual void handleUidString(const std::string& string) {
        LOGS(_log, LOG_LVL_ERROR, __FUNCTION__ << string);
    }
};





} // end namespace


template<typename ParentListener, typename ChildListener>
std::shared_ptr<ChildListener> MySqlListener::pushListenerStack() {
    auto p = std::dynamic_pointer_cast<ParentListener>(_listenerStack.top());
    if (nullptr == p) {
            int status;
            LOGS(_log, LOG_LVL_ERROR, "can't acquire expected Listener " <<
                    abi::__cxa_demangle(typeid(ParentListener).name(),0,0,&status) <<
                    " from top of listenerStack."); // todo add some type names
        // might want to throw here...?
        return nullptr;
    }
    auto childListener = std::make_shared<ChildListener>();
    _listenerStack.push(childListener);
    return childListener;
}


template<typename ChildListener>
void MySqlListener::popListenerStack() {
    shared_ptr<Listener> listenerPtr = _listenerStack.top();
    _listenerStack.pop();
    shared_ptr<ChildListener> derivedPtr = dynamic_pointer_cast<ChildListener>(listenerPtr);
    if (nullptr == derivedPtr) {
        int status;
        LOGS(_log, LOG_LVL_ERROR, "Top of listenerStack was not of expected type. " <<
                "Expected: " << abi::__cxa_demangle(typeid(ChildListener).name(),0,0,&status) <<
                " Actual: " << abi::__cxa_demangle(typeid(listenerPtr).name(),0,0,&status) <<
                " Are there out of order or unhandled listener exits?"); // todo add some type names
        // might want to throw here...?
    }
}


// might want to use this in popListenerStack?
template<typename ChildListener>
std::shared_ptr<ChildListener> MySqlListener::listenerStackTop() const {
    shared_ptr<Listener> listenerPtr = _listenerStack.top();
    shared_ptr<ChildListener> derivedPtr = dynamic_pointer_cast<ChildListener>(listenerPtr);
    if (nullptr == derivedPtr) {
        int status;
        LOGS(_log, LOG_LVL_ERROR, "Top of listenerStack was not of expected type. " <<
                "Expected: " << abi::__cxa_demangle(typeid(ChildListener).name(),0,0,&status) <<
                " Actual: " << abi::__cxa_demangle(typeid(listenerPtr).name(),0,0,&status));
        // might want to throw here?
    }
    return derivedPtr;
}


void MySqlListener::enterRoot(MySqlParser::RootContext * ctx) {
    // since there's no parent listener on the stack for a root listener, we don't use the template push
    // function, we just push the first item onto the stack by hand like so:
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    _rootListener = std::make_shared<RootListener>();
    _listenerStack.push(_rootListener);
}


void MySqlListener::exitRoot(MySqlParser::RootContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popListenerStack<RootListener>();
}


void MySqlListener::enterDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushListenerStack<RootListener, DMLListener>();
}


void MySqlListener::exitDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popListenerStack<DMLListener>();
}


void MySqlListener::enterSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushListenerStack<DMLListener, SelectListener>();
}


void MySqlListener::exitSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popListenerStack<SelectListener>();
}


void MySqlListener::enterSelectElements(MySqlParser::SelectElementsContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushListenerStack<SelectListener, SelectElementsListener>();
}


void MySqlListener::exitSelectElements(MySqlParser::SelectElementsContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popListenerStack<SelectElementsListener>();
}


void MySqlListener::enterSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    pushListenerStack<SelectElementsListener, SelectColumnElementListener>();
}


void MySqlListener::exitSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    popListenerStack<SelectColumnElementListener>();
}


void MySqlListener::enterUid(MySqlParser::UidContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);
    shared_ptr<UidCBH> handler = listenerStackTop<UidCBH>();
//    shared_ptr<SelectColumnElementListener> handler = listenerStackTop<SelectColumnElementListener>();
}


void MySqlListener::exitUid(MySqlParser::UidContext * ctx) {
    LOGS(_log, LOG_LVL_DEBUG, __FUNCTION__);

}



}}} // namespace lsst::qserv::parser
