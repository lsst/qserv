// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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

// Class header
#include "parser/ValueExprFactory.h"

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "parser/ValueFactorFactory.h"
#include "parser/ColumnRefH.h"
#include "query/ValueExpr.h" // For ValueExpr, FuncExpr
#include "query/ValueFactor.h" // For ValueFactor
#include "parser/ParseException.h" //
#include "parser/SqlSQL2TokenTypes.hpp" // antlr-generated


using antlr::RefAST;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.parser.ValueExprFactory");
}

namespace lsst {
namespace qserv {
namespace parser {


void ValueExprFactory::addValueFactor(std::shared_ptr<query::ValueExpr> valueExpr,
                                      std::shared_ptr<query::ValueFactor> valueFactor) {
    query::ValueExpr::FactorOp factorOp;
    factorOp.factor = valueFactor;
    valueExpr->_factorOps.push_back(factorOp);
}


////////////////////////////////////////////////////////////////////////
// ValueExprFactory implementation
////////////////////////////////////////////////////////////////////////
ValueExprFactory::ValueExprFactory(std::shared_ptr<ColumnRefNodeMap> cMap)
    : _valueFactorFactory(new ValueFactorFactory(cMap, *this)) {
}


std::shared_ptr<query::ValueExpr>
ValueExprFactory::newOperationFuncExpr(std::shared_ptr<query::FuncExpr> lhs,
                                       query::ValueExpr::Op op,
                                       std::shared_ptr<query::FuncExpr> rhs) {
    auto valueExpr = std::make_shared<query::ValueExpr>();
    query::ValueExpr::FactorOp lhsFactorOp;
    lhsFactorOp.op = op;
    lhsFactorOp.factor = query::ValueFactor::newFuncFactor(lhs);
    valueExpr->_factorOps.push_back(lhsFactorOp);
    query::ValueExpr::FactorOp rhsFactorOp;
    rhsFactorOp.op = query::ValueExpr::NONE;
    rhsFactorOp.factor = query::ValueFactor::newFuncFactor(rhs);
    valueExpr->_factorOps.push_back(rhsFactorOp);
    return valueExpr;
}



// VALUE_EXP                     //
// |      \                      //
// TERM   (TERM_OP TERM)*        //
/// @param first child of VALUE_EXP node.
std::shared_ptr<query::ValueExpr>
ValueExprFactory::newExpr(antlr::RefAST a) {
    std::shared_ptr<query::ValueExpr> expr = std::make_shared<query::ValueExpr>();
    while(a.get()) {
        query::ValueExpr::FactorOp newFactorOp;
        RefAST op = a->getNextSibling();
        newFactorOp.factor = _valueFactorFactory->newFactor(a);
        if (op.get()) { // No more ops?
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
    if (LOG_CHECK_LVL(_log, _LOG_LVL_DEBUG)) {
        std::stringstream ss;
        std::copy(expr->_factorOps.begin(), expr->_factorOps.end(),
                  std::ostream_iterator<query::ValueExpr::FactorOp>(ss, ","));
        LOGS(_log, LOG_LVL_DEBUG, "Imported expr: " << ss.str());
    }
#endif
    if (expr->isFactor() && expr->getAlias().empty()) {
        // Singleton factor? Check inside for optimization opportunities.
        if (expr->getFactor()->getType() == query::ValueFactor::EXPR) {
            // Pop the value expr out.
            return expr->getFactorOps().front().factor->getExpr();
        }
    }
    return expr;
}
}}} // namespace lsst::qserv::parser
