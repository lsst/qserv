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

#include <vector>


namespace lsst {
namespace qserv {
namespace parser {

LOG_LOGGER _log = LOG_GET("lsst.qserv.MySqlListener");

class ListenContext {
public:
    virtual ~ListenContext() {}
    typedef std::shared_ptr<ListenContext> Ptr;
};

namespace {

class RootContext : public ListenContext {
public:
    typedef std::shared_ptr<RootContext> Ptr;
    query::SelectStmt::Ptr selectStatement();
private:
    query::SelectStmt::Ptr _selectStatement;
};


class DMLContext : public ListenContext {
public:
    typedef std::shared_ptr<DMLContext> Ptr;
};


class SelectContext : public ListenContext {
public:
    typedef std::shared_ptr<SelectContext> Ptr;
};


class SelectColumnContext : public ListenContext {
public:
    typedef std::shared_ptr<SelectColumnContext> Ptr;

    void addColumnName(const std::string& columnName) { _columns.push_back(columnName); }
private:
    std::vector<std::string> _columns;
};


} // end namespace


void MySqlListener::enterRoot(MySqlParser::RootContext * ctx) {
    _rootContext = std::make_shared<RootContext>();
    _contextStack.push(_rootContext);
}


void MySqlListener::exitRoot(MySqlParser::RootContext * ctx) {
    _contextStack.pop();
}


void MySqlListener::enterDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    _contextStack.push(std::make_shared<DMLContext>());
}


void MySqlListener::exitDmlStatement(MySqlParser::DmlStatementContext * ctx) {
    _contextStack.pop();
}


void MySqlListener::enterSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    auto p = std::dynamic_pointer_cast<DMLContext>(_contextStack.top());
    if (nullptr == p) {
        LOGS(_log, LOG_LVL_ERROR, "enterSimpleSelect can't acquire a DMLContext.");
        // might want to throw here...?
        return;
    }
    _contextStack.push(std::make_shared<SelectContext>());
}


void MySqlListener::exitSimpleSelect(MySqlParser::SimpleSelectContext * ctx) {
    _contextStack.pop();
}


void MySqlListener::enterSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    auto p = std::dynamic_pointer_cast<SelectContext>(_contextStack.top());
    if (nullptr == p) {
        LOGS(_log, LOG_LVL_ERROR, "enterSelectColumnElement can't acquire the SelectContext.");
        // might want to throw here...?
        return;
    }
    _contextStack.push(std::make_shared<SelectColumnContext>());
}


void MySqlListener::exitSelectColumnElement(MySqlParser::SelectColumnElementContext * ctx) {
    _contextStack.pop();
}


void MySqlListener::enterFullColumnName(MySqlParser::FullColumnNameContext * ctx) {
    auto p = std::dynamic_pointer_cast<SelectColumnContext>(_contextStack.top());
    if (nullptr == p) {
        LOGS(_log, LOG_LVL_ERROR, "enterSelectColumnElement can't acquire the SelectContext.");
        // might want to throw here...?
        return;
    }
    p->addColumnName(ctx->uid()->simpleId()->ID()->getText());
}


void MySqlListener::exitFullColumnName(MySqlParser::FullColumnNameContext * ctx) {
    // currently we don't add a context for FullColumnName
}



}}} // namespace lsst::qserv::parser
