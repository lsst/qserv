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
#ifndef LSST_QSERV_PARSER_SELECTLISTFACTORY_H
#define LSST_QSERV_PARSER_SELECTLISTFACTORY_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <map>
#include <memory>
#include <stdexcept>

// Third-party headers
#include <antlr/AST.hpp>

// Forward declarations
class SqlSQL2Parser;
namespace lsst {
namespace qserv {
namespace parser {
    class ParseAliasMap;
    class ValueExprFactory;
}
namespace query {
    class SelectList;
    class ValueExpr;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {

typedef std::shared_ptr<query::ValueExpr> ValueExprPtr;
typedef std::vector<ValueExprPtr> ValueExprPtrVector;

/// SelectListFactory maintains parse state so that a SelectList can be built
/// from a ANTLR parse tree nodes. It populates some state for SelectFactory.
class SelectListFactory {
public:
    static void addValueExpr(std::shared_ptr<query::SelectList> selectList, ValueExprPtr valueExpr);

    std::shared_ptr<query::SelectList> getProduct();
    void import(antlr::RefAST selectRoot);
    void importStar(antlr::RefAST asterisk);

    static void addSelectAggFunction(std::shared_ptr<query::SelectList>& selectList,
                                     std::shared_ptr<query::ValueExpr>& func);

private:
    friend class SelectFactory;

    class SelectStarH;
    friend class SelectStarH;
    class ColumnAliasH;

    // For "friends"
    SelectListFactory(std::shared_ptr<ValueExprFactory> vf);
    void attachTo(SqlSQL2Parser& p); ///< For column alias handling

    // Really private
    void _addSelectColumn(antlr::RefAST expr);
    void _addSelectStar(antlr::RefAST child=antlr::RefAST());
    ValueExprPtr _newColumnExpr(antlr::RefAST expr);
    ValueExprPtr _newSetFctSpec(antlr::RefAST expr);

    // Delegate handlers
    std::shared_ptr<ColumnAliasH> _columnAliasH;

    // data
    std::shared_ptr<ParseAliasMap> _columnAliases;
    std::shared_ptr<ValueExprFactory> _vFactory;
    std::shared_ptr<ValueExprPtrVector> _valueExprList;

};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_SELECTLISTFACTORY_H

