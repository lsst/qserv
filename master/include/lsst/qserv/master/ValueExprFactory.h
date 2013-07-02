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
// ValueExprFactory constructs ValueExpr instances from antlr nodes.

#ifndef LSST_QSERV_MASTER_VALUEEXPRFACTORY_H
#define LSST_QSERV_MASTER_VALUEEXPRFACTORY_H
/**
  * @file ValueExprFactory.h
  *
  * @brief ValueExprFactory makes ValueExpr objects.
  *
  * @author Daniel L. Wang, SLAC
  */
#include <boost/shared_ptr.hpp>
#include <antlr/AST.hpp>

namespace lsst {
namespace qserv {
namespace master {
// Forward
class ColumnRefMap;
class ValueExpr;
class ValueFactorFactory;
/// ValueExprFactory is a factory for making ValueExpr objects
class ValueExprFactory {
public:
    ValueExprFactory(boost::shared_ptr<ColumnRefMap> cMap);
    boost::shared_ptr<ValueExpr> newExpr(antlr::RefAST a);
                                         
private:
    boost::shared_ptr<ValueFactorFactory> _valueFactorFactory;
};

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_VALUEEXPRFACTORY_H

