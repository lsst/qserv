// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
  * @brief PredicateFactory is a factory class for Predicate objects that get
  * placed (typically) in WhereClause objects.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "parser/PredicateFactory.h"

// Third-party headers
#include <boost/algorithm/string.hpp>
#include "SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "parser/parseTreeUtil.h"
#include "parser/ValueExprFactory.h"
#include "query/Predicate.h"


namespace lsst {
namespace qserv {
namespace parser {

boost::shared_ptr<query::CompPredicate>
PredicateFactory::newCompPredicate(antlr::RefAST a) {
    boost::shared_ptr<query::CompPredicate> p(new query::CompPredicate());
    if(a->getType() == SqlSQL2TokenTypes::COMP_PREDICATE) {
        a = a->getFirstChild();
    }
    RefAST left = a;
    RefAST op = left->getNextSibling();
    RefAST right = op->getNextSibling();
    p->left = _vf.newExpr(left->getFirstChild());
    p->op = op->getType();
    p->right = _vf.newExpr(right->getFirstChild());
    return p;
}

boost::shared_ptr<query::BetweenPredicate> PredicateFactory::newBetweenPredicate(antlr::RefAST a) {
    boost::shared_ptr<query::BetweenPredicate> p(new query::BetweenPredicate());
    if(a->getType() == SqlSQL2TokenTypes::BETWEEN_PREDICATE) {
        a = a->getFirstChild();
    }
    RefAST betweenToken = a->getNextSibling();
    RefAST minValue = betweenToken->getNextSibling();
    RefAST andToken = minValue->getNextSibling();
    RefAST maxValue = andToken->getNextSibling();
    p->value = _vf.newExpr(a->getFirstChild());
    p->minValue = _vf.newExpr(minValue->getFirstChild());
    p->maxValue = _vf.newExpr(maxValue->getFirstChild());
    return p;
}

boost::shared_ptr<query::InPredicate>
PredicateFactory::newInPredicate(antlr::RefAST a) {
    boost::shared_ptr<query::InPredicate> p(new query::InPredicate());
    if(a->getType() == SqlSQL2TokenTypes::IN_PREDICATE) {
        a = a->getFirstChild();
    }
    RefAST value = a;
    RefAST inToken = value->getNextSibling();
    RefAST leftParen = inToken->getNextSibling();
    RefAST firstElement = leftParen->getNextSibling();
    p->value = _vf.newExpr(value->getFirstChild());
    for(RefAST i=firstElement;
        i.get() && i->getType() != SqlSQL2TokenTypes::RIGHT_PAREN;
        i = i->getNextSibling()) {
        if(i->getType() == SqlSQL2TokenTypes::COMMA) {
            i = i->getNextSibling();
        }
        p->cands.push_back(_vf.newExpr(i->getFirstChild()));
    }
    // query::InPredicate& ip = *p; // for gdb
    return p;
}

boost::shared_ptr<query::LikePredicate>
PredicateFactory::newLikePredicate(antlr::RefAST a) {
    boost::shared_ptr<query::LikePredicate> p(new query::LikePredicate());
    if(a->getType() == SqlSQL2TokenTypes::LIKE_PREDICATE) {
        a = a->getFirstChild();
    }
    RefAST value = a;
    RefAST likeToken = value->getNextSibling();
    RefAST pattern = likeToken->getNextSibling();

    p->value = _vf.newExpr(value->getFirstChild());
    p->charValue = _vf.newExpr(pattern->getFirstChild());
    // query::LikePredicate& lp = *p; // for gdb
    return p;
}

boost::shared_ptr<query::NullPredicate>
PredicateFactory::newNullPredicate(antlr::RefAST a) {
    boost::shared_ptr<query::NullPredicate> p(new query::NullPredicate());

    if(a->getType() == SqlSQL2TokenTypes::NULL_PREDICATE) { a = a->getFirstChild(); }
    RefAST value = a;
    RefAST isToken = value->getNextSibling();
    RefAST nullToken = isToken->getNextSibling();
    std::string notCand = tokenText(nullToken);
    boost::to_upper(notCand);
    if(notCand == "NOT") {
        p->hasNot = true;
        nullToken = nullToken->getNextSibling();
    } else {
        p->hasNot = false;
    }

    p->value = _vf.newExpr(value->getFirstChild());
    // query::NullPredicate& np = *p; // for gdb
    return p;
}

}}} // namespace lsst::qserv::parser
