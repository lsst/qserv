// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
// ValueExprFactory constructs ValueExpr instances from antlr nodes.

#ifndef LSST_QSERV_PARSER_VALUEEXPRFACTORY_H
#define LSST_QSERV_PARSER_VALUEEXPRFACTORY_H
/**
  * @file
  *
  * @brief ValueExprFactory makes ValueExpr objects.
  *
  * @author Daniel L. Wang, SLAC
  */

// Third-party headers
#include <antlr/AST.hpp>
#include <memory>

// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class ValueExpr;
}
namespace parser {
    class ColumnRefNodeMap;
    class ValueFactorFactory;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {

/// ValueExprFactory is a factory for making ValueExpr objects
class ValueExprFactory {
public:
    ValueExprFactory(std::shared_ptr<ColumnRefNodeMap> cMap);
    std::shared_ptr<query::ValueExpr> newExpr(antlr::RefAST a);

private:
    std::shared_ptr<ValueFactorFactory> _valueFactorFactory;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_VALUEEXPRFACTORY_H
