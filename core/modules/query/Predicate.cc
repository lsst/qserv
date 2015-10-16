// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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
  * @brief Predicate, CompPredicate, InPredicate, BetweenPredicate, LikePredicate, and NullPredicate implementations.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/Predicate.h"

// System
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "query/QueryTemplate.h"
#include "query/SqlSQL2Tokens.h" // (generated) SqlSQL2Tokens
#include "query/ValueExpr.h"


namespace lsst {
namespace qserv {
namespace query {

void CompPredicate::findColumnRefs(ColumnRef::Vector& vector) {
    if(left) { left->findColumnRefs(vector); }
    if(right) { right->findColumnRefs(vector); }
}

void InPredicate::findColumnRefs(ColumnRef::Vector& vector) {
    if(value) { value->findColumnRefs(vector); }
    std::vector<std::shared_ptr<ValueExpr> >::iterator i;
    for(i=cands.begin(); i != cands.end(); ++i) {
        (**i).findColumnRefs(vector);
    }
}

void BetweenPredicate::findColumnRefs(ColumnRef::Vector& vector) {
    if(value) { value->findColumnRefs(vector); }
    if(minValue) { minValue->findColumnRefs(vector); }
    if(maxValue) { maxValue->findColumnRefs(vector); }
}

void LikePredicate::findColumnRefs(ColumnRef::Vector& vector) {
    if(value) { value->findColumnRefs(vector); }
    if(charValue) { charValue->findColumnRefs(vector); }
}

void NullPredicate::findColumnRefs(ColumnRef::Vector& vector) {
    if(value) { value->findColumnRefs(vector); }
}

std::ostream& CompPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}
std::ostream& InPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}
std::ostream& BetweenPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}
std::ostream& LikePredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}
std::ostream& NullPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}

void CompPredicate::renderTo(QueryTemplate& qt) const {

    ValueExpr::render r(qt, false);
    r(left);
    switch(op) {
    case SqlSQL2Tokens::EQUALS_OP: qt.append("="); break;
    case SqlSQL2Tokens::NOT_EQUALS_OP: qt.append("<>"); break;
    case SqlSQL2Tokens::LESS_THAN_OP: qt.append("<"); break;
    case SqlSQL2Tokens::GREATER_THAN_OP: qt.append(">"); break;
    case SqlSQL2Tokens::LESS_THAN_OR_EQUALS_OP: qt.append("<="); break;
    case SqlSQL2Tokens::GREATER_THAN_OR_EQUALS_OP: qt.append(">="); break;
    case SqlSQL2Tokens::NOT_EQUALS_OP_ALT: qt.append("!="); break;
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

void NullPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r(value);
    qt.append("IS");
    if(hasNot) { qt.append("NOT"); }
    qt.append("NULL");
}

void CompPredicate::findValueExprs(ValueExprPtrVector& vector) {
    vector.push_back(left);
    vector.push_back(right);
}

void InPredicate::findValueExprs(ValueExprPtrVector& vector) {
    vector.push_back(value);
    vector.insert(vector.end(), cands.begin(), cands.end());
}

void BetweenPredicate::findValueExprs(ValueExprPtrVector& vector) {
    vector.push_back(value);
    vector.push_back(minValue);
    vector.push_back(maxValue);
}

void LikePredicate::findValueExprs(ValueExprPtrVector& vector) {
    vector.push_back(value);
    vector.push_back(charValue);
}

void NullPredicate::findValueExprs(ValueExprPtrVector& vector) {
    vector.push_back(value);
}

int CompPredicate::lookupOp(char const* op) {
    switch(op[0]) {
    case '<':
        if(op[1] == '\0') { return SqlSQL2Tokens::LESS_THAN_OP; }
        else if(op[1] == '>') { return SqlSQL2Tokens::NOT_EQUALS_OP; }
        else if(op[1] == '=') { return SqlSQL2Tokens::LESS_THAN_OR_EQUALS_OP; }
        else { throw std::invalid_argument("Invalid op string <?"); }
    case '>':
        if(op[1] == '\0') { return SqlSQL2Tokens::GREATER_THAN_OP; }
        else if(op[1] == '=') { return SqlSQL2Tokens::GREATER_THAN_OR_EQUALS_OP; }
        else { throw std::invalid_argument("Invalid op string >?"); }
    case '=':
        return SqlSQL2Tokens::EQUALS_OP;
    default:
        throw std::invalid_argument("Invalid op string ?");
    }
}

BoolFactorTerm::Ptr CompPredicate::clone() const {
    CompPredicate* p = new CompPredicate;
    if(left) p->left = left->clone();
    p->op = op;
    if(right) p->right = right->clone();
    return BoolFactorTerm::Ptr(p);
}

BoolFactorTerm::Ptr GenericPredicate::clone() const {
    //return BfTerm::Ptr(new GenericPredicate());
    return BoolFactorTerm::Ptr();
}

namespace {
    struct valueExprCopy {
        inline ValueExprPtr operator()(ValueExprPtr const& p) {
            return p ? p->clone() : ValueExprPtr();
        }
    };
}

BoolFactorTerm::Ptr InPredicate::clone() const {
    InPredicate::Ptr p  = std::make_shared<InPredicate>();
    if(value) p->value = value->clone();
    std::transform(cands.begin(), cands.end(),
                   std::back_inserter(p->cands),
                   valueExprCopy());
    return BoolFactorTerm::Ptr(p);
}

BoolFactorTerm::Ptr BetweenPredicate::clone() const {
    BetweenPredicate::Ptr p = std::make_shared<BetweenPredicate>();
    if(value) p->value = value->clone();
    if(minValue) p->minValue = minValue->clone();
    if(maxValue) p->maxValue = maxValue->clone();
    return BoolFactorTerm::Ptr(p);
}

BoolFactorTerm::Ptr LikePredicate::clone() const {
    LikePredicate::Ptr p = std::make_shared<LikePredicate>();
    if(value) p->value = value->clone();
    if(charValue) p->charValue = charValue->clone();
    return BoolFactorTerm::Ptr(p);
}

BoolFactorTerm::Ptr NullPredicate::clone() const {
    NullPredicate::Ptr p = std::make_shared<NullPredicate>();
    if(value) p->value = value->clone();
    p->hasNot = hasNot;
    return BoolFactorTerm::Ptr(p);
}

}}} // namespace lsst::qserv::query
