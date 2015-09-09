// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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
  * @brief ValueFactorFactory constructs ValueFactor instances from
  * ANTLR subtrees.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "parser/ValueFactorFactory.h"

// System headers
#include <stdexcept>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "parser/ColumnRefH.h"
#include "parser/parseTreeUtil.h"
#include "parser/ParseException.h"
#include "parser/SqlSQL2TokenTypes.hpp"
#include "parser/ValueExprFactory.h"   // For expression nesting
#include "query/ColumnRef.h"
#include "query/FuncExpr.h"
#include "query/ValueExpr.h"   // For ValueExpr
#include "query/ValueFactor.h" // For ValueFactor

// namespace modifiers
using antlr::RefAST;

////////////////////////////////////////////////////////////////////////
// Anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {

inline RefAST walkToSiblingBefore(RefAST node, int typeId) {
    RefAST last = node;
    for(; node.get(); node = node->getNextSibling()) {
        if(node->getType() == typeId) return last;
        last = node;
    }
    return RefAST();
}

inline std::string getSiblingStringBounded(RefAST left, RefAST right) {
    lsst::qserv::parser::CompactPrintVisitor<RefAST> p;
    for(; left.get(); left = left->getNextSibling()) {
        p(left);
        if(left == right) break;
    }
    return p.result;
}
} // anonymous

namespace lsst {
namespace qserv {
namespace parser {

std::shared_ptr<query::ValueFactor>
newConstFactor(RefAST t) {
    return query::ValueFactor::newConstFactor(walkTreeString(t));
}

////////////////////////////////////////////////////////////////////////
// ValueFactorFactory implementation
////////////////////////////////////////////////////////////////////////
ValueFactorFactory::ValueFactorFactory(
    std::shared_ptr<ColumnRefNodeMap> cMap,
    ValueExprFactory& exprFactory)
    : _columnRefNodeMap(cMap), _exprFactory(exprFactory) {
}


/* VALUE_EXP               */
/* |             \         */
/* TERM   (TERM_OP TERM)*  */
std::shared_ptr<query::ValueFactor>
ValueFactorFactory::newFactor(antlr::RefAST a) {
    if(!_columnRefNodeMap) {
        throw std::logic_error("ValueFactorFactory missing _columnRefNodeMap");
    }
    std::shared_ptr<query::ValueFactor> vt;
    if(a->getType() == SqlSQL2TokenTypes::FACTOR) {
        a = a->getFirstChild(); // FACTOR is a parent placeholder element
    }
    //LOGF_DEBUG("new ValueFactor: %1%" % tokenText(a));
    switch(a->getType()) {
    case SqlSQL2TokenTypes::COLUMN_REF:
        a = a->getFirstChild();
        // COLUMN_REF should have REGULAR_ID as only child.
        // Fall through to REGULAR_ID handler
    case SqlSQL2TokenTypes::REGULAR_ID:
        vt = _newColumnFactor(a); // Column ref or id.
        break;
    case SqlSQL2TokenTypes::FUNCTION_SPEC:
        vt = _newFunctionSpecFactor(a);
        break;
    case SqlSQL2TokenTypes::SET_FCT_SPEC:
        vt = _newSetFctSpec(a);
        break;
    case SqlSQL2TokenTypes::UNSIGNED_INTEGER:
    case SqlSQL2TokenTypes::EXACT_NUM_LIT:
        vt = newConstFactor(a);
        break;
    case SqlSQL2TokenTypes::LEFT_PAREN:
        vt = _newSubFactor(a);
        break;

    default:
        LOGF_DEBUG("Unhandled RefAST type in ValueFactor %1%" % a->getType());
        vt = newConstFactor(a);
        break;
    }
    if(!vt) {
        throw std::logic_error("Faled to construct ValueFactor");
    }
    return vt;
}

std::shared_ptr<query::ValueFactor>
ValueFactorFactory::_newColumnFactor(antlr::RefAST t) {
    assert(_columnRefNodeMap);
    ColumnRefNodeMap& cMap = *_columnRefNodeMap;
    RefAST child = t->getFirstChild();
    if(t->getType() == SqlSQL2TokenTypes::FACTOR) {
        t = child;
        child = t->getFirstChild();
    }
    std::shared_ptr<query::ValueFactor> vt = std::make_shared<query::ValueFactor>();
    std::shared_ptr<query::FuncExpr> fe;
    RefAST last;
    // LOGF_INFO("colterm: %1% %2%" % t->getType() % t->getText());
    int tType = t->getType();
    switch(tType) {
    case SqlSQL2TokenTypes::COLUMN_REF:
        t = child;
        child = t->getFirstChild();
        // Fall-through to REGULAR_ID handler
    case SqlSQL2TokenTypes::REGULAR_ID:
        // make column ref. (no further children)
        {
            ColumnRefNodeMap::Map::const_iterator it = cMap.map.find(t);
            if(it == cMap.map.end()) {
                throw std::logic_error("Expected to find REGULAR_ID in table map");
            }
            ColumnRefNodeMap::Ref r = it->second;

            std::shared_ptr<query::ColumnRef> newColumnRef;
            newColumnRef = std::make_shared<query::ColumnRef>(
                    tokenText(r.db),
                    tokenText(r.table),
                    tokenText(r.column));
            vt = query::ValueFactor::newColumnRefFactor(newColumnRef);
        }
        return vt;
    case SqlSQL2TokenTypes::FUNCTION_SPEC:
        // LOGF_INFO("col child (fct): %1% %2%"
        //           % child->getType() % child->getText());
        fe = std::make_shared<query::FuncExpr>();
        last = walkToSiblingBefore(child, SqlSQL2TokenTypes::LEFT_PAREN);
        fe->name = getSiblingStringBounded(child, last);
        last = last->getNextSibling(); // Advance to LEFT_PAREN
        if(!last.get()) {
            throw ParseException("Expected LEFT_PAREN", last);
        }
        // Now fill params.
        for(antlr::RefAST current = last->getNextSibling();
            current.get(); current = current->getNextSibling()) {
            // Should be a * or a value expr.
            std::shared_ptr<query::ValueFactor> pvt;
            // LOGF_INFO("fctspec param: %1% %2%"
            //           % current->getType() % current->getText());
            switch(current->getType()) {
            case SqlSQL2TokenTypes::VALUE_EXP:
                pvt = newFactor(current->getFirstChild());
                // pvt = newColumnFactor(current->getFirstChild(), cMap);
                break;
            case SqlSQL2TokenTypes::COMMA: continue;
            case SqlSQL2TokenTypes::RIGHT_PAREN: continue;
            default:
                throw ParseException(
                    "ValueFactorFactory::newColumnFactor fct spec with ",
                    current);
                break;
            }
            fe->params.push_back(query::ValueExpr::newSimple(pvt));
        }
        vt = query::ValueFactor::newFuncFactor(fe);
        return vt;

        break;
    default:
        throw ParseException("ValueFactorFactory::newColumnFactor with ", t);
        break;
    }
    return std::shared_ptr<query::ValueFactor>();
}

std::shared_ptr<query::ValueFactor>
ValueFactorFactory::_newSetFctSpec(antlr::RefAST expr) {
    assert(_columnRefNodeMap);
    // ColumnRefNodeMap& cMap = *_columnRefNodeMap; // for gdb
    std::shared_ptr<query::FuncExpr> fe = std::make_shared<query::FuncExpr>();
    // LOGF_INFO("set_fct_spec %1%" % walkTreeString(expr));
    RefAST nNode = expr->getFirstChild();
    if(!nNode.get()) {
        throw ParseException("Missing name node of function spec", expr);
    }
    fe->name = nNode->getText();
    // Now fill params.
    antlr::RefAST current = nNode->getFirstChild();
    // Aggregation functions can only have one param.
    if(current->getType() != SqlSQL2TokenTypes::LEFT_PAREN) {
        throw ParseException("Expected LEFT_PAREN", current);
    }
    current = current->getNextSibling();
    // Should be a * or a value expr.
    std::shared_ptr<query::ValueFactor> pvt;
    switch(current->getType()) {
    case SqlSQL2TokenTypes::VALUE_EXP:
        pvt = _newColumnFactor(current->getFirstChild());
        break;
    case SqlSQL2TokenTypes::ASTERISK:
        pvt = query::ValueFactor::newStarFactor("");
        break;
    default: break;
    }
    current = current->getNextSibling();
    if(current->getType() != SqlSQL2TokenTypes::RIGHT_PAREN) {
        throw ParseException("Expected RIGHT_PAREN", current);
    }
    fe->params.push_back(query::ValueExpr::newSimple(pvt));
    return query::ValueFactor::newAggFactor(fe);
}

std::shared_ptr<query::ValueFactor>
ValueFactorFactory::_newFunctionSpecFactor(antlr::RefAST fspec) {
    assert(_columnRefNodeMap);
    std::shared_ptr<query::FuncExpr> fe = std::make_shared<query::FuncExpr>();
    //LOGF_DEBUG("fspec: %1%" % walkIndentedString(fspec));
    // LOGF_INFO("set_fct_spec %1%" % walkTreeString(expr));
    RefAST nNode = fspec->getFirstChild();
    if(!nNode.get()) {
        throw ParseException("Missing name node of function spec", fspec);
    }
    fe->name = nNode->getText();
    // Now fill params.
    antlr::RefAST current = nNode->getNextSibling();
    // Aggregation functions can only have one param.
    if(current->getType() != SqlSQL2TokenTypes::LEFT_PAREN) {
        throw ParseException("Expected LEFT_PAREN", current);
    }
    current = current->getNextSibling();
    if(!current.get()) {
        throw ParseException("Expected parameter in function", fspec);
    }
    while(current->getType() != SqlSQL2TokenTypes::RIGHT_PAREN) {
        // Should be a value expression
        if(current->getType() != SqlSQL2TokenTypes::VALUE_EXP) {
            throw ParseException("Expected VALUE_EXP for parameter", current);
        }
        RefAST child = current->getFirstChild();
        std::shared_ptr<query::ValueExpr> ve = _exprFactory.newExpr(child);
        fe->params.push_back(ve);
        current = current->getNextSibling();
        if(!current.get()) {
            throw ParseException("Expected COMMA,VALUE_EXP,RIGHT_PAREN", fspec);
        }
        if(current->getType() == SqlSQL2TokenTypes::COMMA) {
            current = current->getNextSibling();
        }
        if(!current.get()) {
            throw ParseException("Expected VALUE_EXP,RIGHT_PAREN", fspec);
        }
    }
    return query::ValueFactor::newFuncFactor(fe);
}

std::shared_ptr<query::ValueFactor>
ValueFactorFactory::_newSubFactor(antlr::RefAST s) {
    // Subfactor is an expression of factor and factor-op.
    // ( expr )
    RefAST lparen = s;
    RefAST expr = s->getNextSibling();
    RefAST rparen = expr->getNextSibling();
    if(expr->getType() != SqlSQL2TokenTypes::VALUE_EXP) {
        throw ParseException("Expected VALUE_EXP", expr);
    }
    //LOGF_DEBUG("expr: %1%" % walkIndentedString(expr));
    RefAST exprChild = expr->getFirstChild();
    std::shared_ptr<query::ValueExpr> ve = _exprFactory.newExpr(exprChild);
    if(ve && ve->isFactor() && ve->getAlias().empty()) {
        return ve->getFactorOps().front().factor;
    }
    return query::ValueFactor::newExprFactor(ve);
}

}}} // namespace lsst::qserv::parser
