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
/**
  * @file
  *
  * @brief ValueExprFactory constructs ValueExpr instances from ANTLR subtrees.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "parser/ValueExprFactory.h"

// Local headers
#include "log/Logger.h"
#include "parser/ValueFactorFactory.h"
#include "parser/ColumnRefH.h"
#include "query/ValueExpr.h" // For ValueExpr, FuncExpr
#include "query/ValueFactor.h" // For ValueFactor
#include "parser/ParseException.h" //
#include "SqlSQL2TokenTypes.hpp" // antlr-generated


using antlr::RefAST;

namespace lsst {
namespace qserv {
namespace parser {

////////////////////////////////////////////////////////////////////////
// ValueExprFactory implementation
////////////////////////////////////////////////////////////////////////
ValueExprFactory::ValueExprFactory(boost::shared_ptr<ColumnRefNodeMap> cMap)
    : _valueFactorFactory(new ValueFactorFactory(cMap)) {
}

// VALUE_EXP                     //
// |      \                      //
// TERM   (TERM_OP TERM)*        //
/// @param first child of VALUE_EXP node.
boost::shared_ptr<query::ValueExpr>
ValueExprFactory::newExpr(antlr::RefAST a) {
    boost::shared_ptr<query::ValueExpr> expr(new query::ValueExpr);
    //LOGGER_INF << walkIndentedString(a) << std::endl;
    while(a.get()) {
        query::ValueExpr::FactorOp newFactorOp;
        RefAST op = a->getNextSibling();
        newFactorOp.factor = _valueFactorFactory->newFactor(a);
        if(op.get()) { // No more ops?
            //LOGGER_INF << "expected op: " << tokenText(op) << std::endl;
            int eType = op->getType();
            switch(eType) {
            case SqlSQL2TokenTypes::PLUS_SIGN:
                newFactorOp.op = query::ValueExpr::PLUS;
                break;
            case SqlSQL2TokenTypes::MINUS_SIGN:
                newFactorOp.op = query::ValueExpr::MINUS;
                break;
            case SqlSQL2TokenTypes::ASTERISK:
                newFactorOp.op = query::ValueExpr::MULTIPLY;
                break;
            case SqlSQL2TokenTypes::SOLIDUS:
                newFactorOp.op = query::ValueExpr::DIVIDE;
                break;
            default:
                newFactorOp.op = query::ValueExpr::UNKNOWN;
                throw ParseException("unhandled factor_op type:", op);
            }
            a = op->getNextSibling();
        } else {
            newFactorOp.op = query::ValueExpr::NONE;
            a = RefAST(); // set to NULL.
        }
        expr->_factorOps.push_back(newFactorOp);
    }
#if 0
    LOGGER_INF << "Imported expr: ";
    std::copy(expr->_factorOps.begin(), expr->_factorOps.end(),
        std::ostream_iterator<query::ValueExpr::FactorOp>(LOG_STRM(Info), ","));
    LOGGER_INF << std::endl;
#endif
    return expr;
}
}}} // namespace lsst::qserv::parser
