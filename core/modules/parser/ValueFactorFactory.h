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

#ifndef LSST_QSERV_PARSER_VALUEFACTORFACTORY_H
#define LSST_QSERV_PARSER_VALUEFACTORFACTORY_H

/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <memory>

// Third-party headers
#include <antlr/AST.hpp>

// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class ValueFactor;
}
namespace parser {
    class ColumnRefNodeMap;
    class ValueExprFactory;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace parser {

/// ValueFactorFactory constructs ValueFactor instances from antlr nodes.
class ValueFactorFactory {
public:
    ValueFactorFactory(std::shared_ptr<ColumnRefNodeMap> cMap,
                       ValueExprFactory& exprFactory);
    std::shared_ptr<query::ValueFactor> newFactor(antlr::RefAST a);

private:
    std::shared_ptr<query::ValueFactor> _newColumnFactor(antlr::RefAST t);
    std::shared_ptr<query::ValueFactor> _newSetFctSpec(antlr::RefAST expr);
    std::shared_ptr<query::ValueFactor> _newFunctionSpecFactor(antlr::RefAST fspec);
    std::shared_ptr<query::ValueFactor> _newSubFactor(antlr::RefAST s);
    std::shared_ptr<ColumnRefNodeMap> _columnRefNodeMap;
    /// For handling nested expressions
    ValueExprFactory& _exprFactory;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_VALUEFACTORFACTORY_H
