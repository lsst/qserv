// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_PARSER_MYSQLLISTENER_H
#define LSST_QSERV_PARSER_MYSQLLISTENER_H

#include "parser/MySqlParser.h" // included for contexts. They *could* be forward declared.
#include "parser/MySqlParserBaseListener.h"

#include <memory>
#include <stack>

namespace lsst {
namespace qserv {
namespace query{
    class SelectStmt;
}
namespace parser {

class Adapter;

class MySqlListener : public MySqlParserBaseListener {
public:
    MySqlListener() {}
    virtual ~MySqlListener() {}

protected:
    virtual void enterRoot(MySqlParser::RootContext * /*ctx*/) override;
    virtual void exitRoot(MySqlParser::RootContext * /*ctx*/) override;
    virtual void enterDmlStatement(MySqlParser::DmlStatementContext * /*ctx*/) override;
    virtual void exitDmlStatement(MySqlParser::DmlStatementContext * /*ctx*/) override;

    // entering a SELECT statement
    virtual void enterSimpleSelect(MySqlParser::SimpleSelectContext * /*ctx*/) override;
    virtual void exitSimpleSelect(MySqlParser::SimpleSelectContext * /*ctx*/) override;

    virtual void enterSelectElements(MySqlParser::SelectElementsContext * /*ctx*/) override;
    virtual void exitSelectElements(MySqlParser::SelectElementsContext * /*ctx*/) override;

    virtual void enterSelectColumnElement(MySqlParser::SelectColumnElementContext * /*ctx*/) override;
    virtual void exitSelectColumnElement(MySqlParser::SelectColumnElementContext * /*ctx*/) override;

    virtual void enterFromClause(MySqlParser::FromClauseContext * /*ctx*/) override;
    virtual void exitFromClause(MySqlParser::FromClauseContext * /*ctx*/) override;

    virtual void enterTableSources(MySqlParser::TableSourcesContext * /*ctx*/) override;
    virtual void exitTableSources(MySqlParser::TableSourcesContext * /*ctx*/) override;

    virtual void enterTableSourceBase(MySqlParser::TableSourceBaseContext * /*ctx*/) override;
    virtual void exitTableSourceBase(MySqlParser::TableSourceBaseContext * /*ctx*/) override;

    virtual void enterAtomTableItem(MySqlParser::AtomTableItemContext * /*ctx*/) override;
    virtual void exitAtomTableItem(MySqlParser::AtomTableItemContext * /*ctx*/) override;

    virtual void enterTableName(MySqlParser::TableNameContext * /*ctx*/) override;
    virtual void exitTableName(MySqlParser::TableNameContext * /*ctx*/) override;

    virtual void enterFullId(MySqlParser::FullIdContext * /*ctx*/) override;
    virtual void exitFullId(MySqlParser::FullIdContext * /*ctx*/) override;

    virtual void enterUid(MySqlParser::UidContext * /*ctx*/) override;
    virtual void exitUid(MySqlParser::UidContext * /*ctx*/) override;


private:
    // Adapter is a base class for a stack of adapter objects. Adapters implement appropriate API for
    // the kinds of children that may be assigned to them. The stack represents execution state of the call
    // to listen. The root object (separate from the stack) will end up owning the parsed query.
    std::stack<std::shared_ptr<Adapter>> _adapterStack;
    std::shared_ptr<Adapter> _rootAdapter;

    template<typename ParentAdapter, typename ChildAdapter>
    std::shared_ptr<ChildAdapter> pushAdapterStack();

    template<typename ChildAdapter>
    void popAdapterStack();

    template<typename ChildAdapter>
    std::shared_ptr<ChildAdapter> adapterStackTop() const;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_MYSQLLISTENER_H
