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
  * @file Predicate.cc
  *
  * @brief Predicate, CompPredicate, InPredicate, BetweenPredicate implementations.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "query/Predicate.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"
#include "parser/SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes
// FIXME: should not depend on parser/, move logic to factory.


namespace { // File-scope helpers
}

namespace lsst {
namespace qserv {
namespace query {

void CompPredicate::findColumnRefs(ColumnRef::List& list) {
    if(left) { left->findColumnRefs(list); }
    if(right) { right->findColumnRefs(list); }
}
void InPredicate::findColumnRefs(ColumnRef::List& list) {
    if(value) { value->findColumnRefs(list); }
    std::list<boost::shared_ptr<ValueExpr> >::iterator i;
    for(i=cands.begin(); i != cands.end(); ++i) {
        (**i).findColumnRefs(list);
    }
}
void BetweenPredicate::findColumnRefs(ColumnRef::List& list) {
    if(value) { value->findColumnRefs(list); }
    if(minValue) { minValue->findColumnRefs(list); }
    if(maxValue) { maxValue->findColumnRefs(list); }
}
void LikePredicate::findColumnRefs(ColumnRef::List& list) {
    if(value) { value->findColumnRefs(list); }
    if(charValue) { charValue->findColumnRefs(list); }
}

std::ostream& CompPredicate::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& InPredicate::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& BetweenPredicate::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& LikePredicate::putStream(std::ostream& os) const {
    // FIXME
    return os;
}

void CompPredicate::renderTo(QueryTemplate& qt) const {

    ValueExpr::render r(qt, false);
    r(left);
    switch(op) {
    case SqlSQL2TokenTypes::EQUALS_OP: qt.append("="); break;
    case SqlSQL2TokenTypes::NOT_EQUALS_OP: qt.append("<>"); break;
    case SqlSQL2TokenTypes::LESS_THAN_OP: qt.append("<"); break;
    case SqlSQL2TokenTypes::GREATER_THAN_OP: qt.append(">"); break;
    case SqlSQL2TokenTypes::LESS_THAN_OR_EQUALS_OP: qt.append("<="); break;
    case SqlSQL2TokenTypes::GREATER_THAN_OR_EQUALS_OP: qt.append(">="); break;
    case SqlSQL2TokenTypes::NOT_EQUALS_OP_ALT: qt.append("!="); break;
    }
    r(right);
}

void InPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r(value);
    qt.append("IN");
    ValueExpr::render rComma(qt, true);
    qt.append("(");
    std::for_each(cands.begin(), cands.end(), rComma);
    qt.append(")");
}

void BetweenPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r(value);
    qt.append("BETWEEN");
    r(minValue);
    qt.append("AND");
    r(maxValue);
}
void LikePredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r(value);
    qt.append("LIKE");
    r(charValue);
}

void CompPredicate::cacheValueExprList() {
    _cache.reset(new ValueExprList());
    _cache->push_back(left);
    _cache->push_back(right);
}
void InPredicate::cacheValueExprList() {
    _cache.reset(new ValueExprList());
    _cache->push_back(value);
    _cache->insert(_cache->end(), cands.begin(), cands.end());
}
void BetweenPredicate::cacheValueExprList() {
    _cache.reset(new ValueExprList());
    _cache->push_back(value);
    _cache->push_back(minValue);
    _cache->push_back(maxValue);
}

void LikePredicate::cacheValueExprList() {
    _cache.reset(new ValueExprList());
    _cache->push_back(value);
    _cache->push_back(charValue);
}


// CompPredicate special function
/// @return a parse token type that is the reversed operator of the
///         input token type.
int CompPredicate::reverseOp(int op) {
    switch(op) {
    case SqlSQL2TokenTypes::NOT_EQUALS_OP:
        return SqlSQL2TokenTypes::NOT_EQUALS_OP;
    case SqlSQL2TokenTypes::LESS_THAN_OR_EQUALS_OP:
        return SqlSQL2TokenTypes::GREATER_THAN_OR_EQUALS_OP;
    case  SqlSQL2TokenTypes::GREATER_THAN_OR_EQUALS_OP:
        return SqlSQL2TokenTypes::LESS_THAN_OR_EQUALS_OP;
    case SqlSQL2TokenTypes::LESS_THAN_OP:
        return SqlSQL2TokenTypes::GREATER_THAN_OP;
    case  SqlSQL2TokenTypes::GREATER_THAN_OP:
        return SqlSQL2TokenTypes::LESS_THAN_OP;
    case SqlSQL2TokenTypes::EQUALS_OP:
        return SqlSQL2TokenTypes::EQUALS_OP;
    default:
        throw std::logic_error("Invalid op type for reversing");
    }
}
char const* CompPredicate::lookupOp(int op) {
    switch(op) {
    case SqlSQL2TokenTypes::NOT_EQUALS_OP:
        return "<>";
    case SqlSQL2TokenTypes::LESS_THAN_OR_EQUALS_OP:
        return "<=";
    case  SqlSQL2TokenTypes::GREATER_THAN_OR_EQUALS_OP:
        return ">=";
    case SqlSQL2TokenTypes::LESS_THAN_OP:
        return "<";
    case  SqlSQL2TokenTypes::GREATER_THAN_OP:
        return ">";
    case SqlSQL2TokenTypes::EQUALS_OP:
        return "==";
    default:
        throw std::invalid_argument("Invalid op type");
    }
}

int CompPredicate::lookupOp(char const* op) {
    switch(op[0]) {
    case '<':
        if(op[1] == '\0') { return SqlSQL2TokenTypes::LESS_THAN_OP; }
        else if(op[1] == '>') { return SqlSQL2TokenTypes::NOT_EQUALS_OP; }
        else if(op[1] == '=') { return SqlSQL2TokenTypes::LESS_THAN_OR_EQUALS_OP; }
        else { throw std::invalid_argument("Invalid op string <?"); }
    case '>':
        if(op[1] == '\0') { return SqlSQL2TokenTypes::GREATER_THAN_OP; }
        else if(op[1] == '=') { return SqlSQL2TokenTypes::GREATER_THAN_OR_EQUALS_OP; }
        else { throw std::invalid_argument("Invalid op string >?"); }
    case '=':
        return SqlSQL2TokenTypes::EQUALS_OP;
    default:
        throw std::invalid_argument("Invalid op string ?");
    }
}

BfTerm::Ptr CompPredicate::copySyntax() const {
    CompPredicate* p = new CompPredicate;
    if(left) p->left = left->clone();
    p->op = op;
    if(right) p->right = right->clone();
    return BfTerm::Ptr(p);
}

struct valueExprCopy {
    inline ValueExprPtr operator()(ValueExprPtr const& p) {
        return p ? p->clone() : ValueExprPtr();
    }
};

BfTerm::Ptr InPredicate::copySyntax() const {
    InPredicate* p = new InPredicate;
    if(value) p->value = value->clone();
    std::transform(cands.begin(), cands.end(),
                   std::back_inserter(p->cands),
                   valueExprCopy());
    return BfTerm::Ptr(p);
}
BfTerm::Ptr BetweenPredicate::copySyntax() const {
    BetweenPredicate* p = new BetweenPredicate;
    if(value) p->value = value->clone();
    if(minValue) p->minValue = minValue->clone();
    if(maxValue) p->maxValue = maxValue->clone();
    return BfTerm::Ptr(p);
}

BfTerm::Ptr LikePredicate::copySyntax() const {
    LikePredicate* p = new LikePredicate;
    if(value) p->value = value->clone();
    if(charValue) p->charValue = charValue->clone();
    return BfTerm::Ptr(p);
}

}}} // namespace lsst::qserv::query
