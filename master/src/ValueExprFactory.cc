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
/**
  * @file ValueExpr.cc
  *
  * @brief ValueExprFactory constructs ValueExpr instances from ANTLR subtrees.
  *
  * @author Daniel L. Wang, SLAC
  */ 
#include "lsst/qserv/master/ValueExprFactory.h"
#include "lsst/qserv/master/ValueFactorFactory.h"
#include "lsst/qserv/master/ColumnRefH.h"
#include "lsst/qserv/master/ValueExpr.h" // For ValueExpr, FuncExpr
#include "lsst/qserv/master/ValueFactor.h" // For ValueFactor
#include "lsst/qserv/master/ParseException.h" // 
#include "SqlSQL2TokenTypes.hpp" // antlr-generated


// namespace modifiers
namespace qMaster = lsst::qserv::master;
using antlr::RefAST;

////////////////////////////////////////////////////////////////////////
// Anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {

} // anonymous
namespace lsst { namespace qserv { namespace master {
////////////////////////////////////////////////////////////////////////
// ValueExprFactory implementation
////////////////////////////////////////////////////////////////////////
ValueExprFactory::ValueExprFactory(boost::shared_ptr<ColumnRefMap> cMap) 
    : _valueFactorFactory(new ValueFactorFactory(cMap)) {
}

// VALUE_EXP                     //
// |      \                      //
// TERM   (TERM_OP TERM)*        //
boost::shared_ptr<ValueExpr> 
ValueExprFactory::newExpr(antlr::RefAST a) {
    boost::shared_ptr<ValueExpr> expr(new ValueExpr);
    //std::cout << walkIndentedString(a) << std::endl;
    while(a.get()) {
        ValueExpr::FactorOp newFactorOp;
        //std::cout << "factor: " << tokenText(a) << std::endl;
        newFactorOp.factor = _valueFactorFactory->newFactor(a);
        RefAST op = a->getNextSibling();
        if(op.get()) { // No more ops?
            //std::cout << "expected op: " << tokenText(op) << std::endl;
            int eType = op->getType();
            switch(op->getType()) {
            case SqlSQL2TokenTypes::PLUS_SIGN:
                newFactorOp.op = ValueExpr::PLUS;
                break;
            case SqlSQL2TokenTypes::MINUS_SIGN:
                newFactorOp.op = ValueExpr::MINUS;
                break;
            case SqlSQL2TokenTypes::ASTERISK:
                newFactorOp.op = ValueExpr::MULTIPLY;
                break;
            case SqlSQL2TokenTypes::SOLIDUS:
                newFactorOp.op = ValueExpr::DIVIDE;
                break;
            default: 
                newFactorOp.op = ValueExpr::UNKNOWN;
                throw ParseException("unhandled factor_op type:", op);
            }
            a = op->getNextSibling();
        } else {
            newFactorOp.op = ValueExpr::NONE;
            a = RefAST(); // set to NULL.
        }
        expr->_factorOps.push_back(newFactorOp);
    }
#if 0
    std::cout << "Imported expr: ";
    std::copy(expr->_factorOps.begin(), expr->_factorOps.end(), 
              std::ostream_iterator<ValueExpr::FactorOp>(std::cout, ","));
    std::cout << std::endl;
#endif
    return expr;
}
}}} // lsst::qserv::master

