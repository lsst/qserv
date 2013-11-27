// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
#ifndef LSST_QSERV_PARSER_SELECTFACTORY_H
#define LSST_QSERV_PARSER_SELECTFACTORY_H
/**
  * @file SelectFactory.h
  *
  * @brief SelectFactory maintains parse state so that a SelectStmt can be built
  * from a parse tree. SelectListFactory depends on some of this state.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <list>
#include <map>
#include <antlr/AST.hpp>
#include <boost/shared_ptr.hpp>

// Impl
#include <boost/make_shared.hpp>

// Forward
class SqlSQL2Parser;

namespace lsst {
namespace qserv {

namespace query {
    // Forward
    class FromList;
    class SelectStmt;
    class WhereClause;
}
        
namespace parser {

// Forward
class ParseAliasMap;
class ColumnRefNodeMap;
class SelectListFactory;
class FromFactory;
class WhereFactory;
class ModFactory;
class ValueExprFactory;

/// SelectFactory is responsible for constructing a SelectStmt (including
/// SelectList, FromClause, WhereClause, etc.) from ANTLR parse actions
class SelectFactory {
public:
    SelectFactory();
    void attachTo(SqlSQL2Parser& p);

    boost::shared_ptr<query::SelectStmt> getStatement();

    boost::shared_ptr<SelectListFactory> getSelectListFactory() {
        return _slFactory; }
    boost::shared_ptr<FromFactory> getFromFactory() {
        return _fFactory; }
    boost::shared_ptr<WhereFactory> getWhereFactory() {
        return _wFactory; }

private:
    void _attachShared(SqlSQL2Parser& p);

    // parse-domain state
    boost::shared_ptr<ParseAliasMap> _columnAliases;
    boost::shared_ptr<ParseAliasMap> _tableAliases;
    boost::shared_ptr<ColumnRefNodeMap> _columnRefNodeMap;

    // delegates
    boost::shared_ptr<SelectListFactory> _slFactory;
    boost::shared_ptr<FromFactory> _fFactory;
    boost::shared_ptr<WhereFactory> _wFactory;
    boost::shared_ptr<ModFactory> _mFactory;
    boost::shared_ptr<ValueExprFactory> _vFactory;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_SELECTFACTORY_H
