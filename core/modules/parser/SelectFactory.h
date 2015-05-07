// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
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
  * @file
  *
  * @brief SelectFactory maintains parse state so that a SelectStmt can be built
  * from a parse tree. SelectListFactory depends on some of this state.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <map>
#include <memory>

// Third-party headers
#include <antlr/AST.hpp>

// Forward declarations
class SqlSQL2Parser;
namespace lsst {
namespace qserv {
namespace parser {
    class ColumnRefNodeMap;
    class FromFactory;
    class ModFactory;
    class ParseAliasMap;
    class SelectListFactory;
    class ValueExprFactory;
    class WhereFactory;
}
namespace query {
    class FromList;
    class SelectStmt;
    class WhereClause;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {

/// SelectFactory is responsible for constructing a SelectStmt (including
/// SelectList, FromClause, WhereClause, etc.) from ANTLR parse actions
class SelectFactory {
public:
    SelectFactory();
    void attachTo(SqlSQL2Parser& p);

    std::shared_ptr<query::SelectStmt> getStatement();

    std::shared_ptr<SelectListFactory> getSelectListFactory() {
        return _slFactory; }
    std::shared_ptr<FromFactory> getFromFactory() {
        return _fFactory; }
    std::shared_ptr<WhereFactory> getWhereFactory() {
        return _wFactory; }

    void setDistinct(bool d) { _hasDistinct = d; }
private:
    void _attachShared(SqlSQL2Parser& p);

    // parse-domain state
    std::shared_ptr<ParseAliasMap> _columnAliases;
    std::shared_ptr<ParseAliasMap> _tableAliases;
    std::shared_ptr<ColumnRefNodeMap> _columnRefNodeMap;

    // delegates
    bool _hasDistinct;
    std::shared_ptr<SelectListFactory> _slFactory;
    std::shared_ptr<FromFactory> _fFactory;
    std::shared_ptr<WhereFactory> _wFactory;
    std::shared_ptr<ModFactory> _mFactory;
    std::shared_ptr<ValueExprFactory> _vFactory;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_SELECTFACTORY_H
