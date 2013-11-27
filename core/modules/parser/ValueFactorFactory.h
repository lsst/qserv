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

#ifndef LSST_QSERV_PARSER_VALUEFACTORFACTORY_H
#define LSST_QSERV_PARSER_VALUEFACTORFACTORY_H

/**
  * @file ValueFactor.h
  *
  * @author Daniel L. Wang, SLAC
  */
#include <boost/shared_ptr.hpp>
#include <antlr/AST.hpp>

namespace lsst {
namespace qserv {

namespace query {
    // Forward
    class ValueFactor;
}

namespace parser {

// Forward
class ColumnRefNodeMap;

/// ValueFactorFactory constructs ValueFactor instances from antlr nodes.
class ValueFactorFactory {
public:
    ValueFactorFactory(boost::shared_ptr<ColumnRefNodeMap> cMap);
    boost::shared_ptr<query::ValueFactor> newFactor(antlr::RefAST a);

private:
    boost::shared_ptr<query::ValueFactor> _newColumnFactor(antlr::RefAST t);
    boost::shared_ptr<query::ValueFactor> _newSetFctSpec(antlr::RefAST expr);

    boost::shared_ptr<ColumnRefNodeMap> _columnRefNodeMap;
};

}}} // namespace lsst::qserv::parser

#endif // LSST_QSERV_PARSER_VALUEFACTORFACTORY_H
