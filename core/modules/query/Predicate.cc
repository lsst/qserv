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
  * @brief Predicate, CompPredicate, InPredicate, BetweenPredicate, LikePredicate, and NullPredicate implementations.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "query/Predicate.h"

// System
#include <stdexcept>

// Local headers
#include "query/QueryTemplate.h"
#include "query/SqlSQL2Tokens.h" // (generated) SqlSQL2Tokens
#include "query/ValueExpr.h"


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

void NullPredicate::findColumnRefs(ColumnRef::List& list) {
    if(value) { value->findColumnRefs(list); }
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

void CompPredicate::findValueExprs(ValueExprList& list) {
    list.push_back(left);
    list.push_back(right);
}

void InPredicate::findValueExprs(ValueExprList& list) {
    list.push_back(value);
    list.insert(list.end(), cands.begin(), cands.end());
}

void BetweenPredicate::findValueExprs(ValueExprList& list) {
    list.push_back(value);
    list.push_back(minValue);
    list.push_back(maxValue);
}

void LikePredicate::findValueExprs(ValueExprList& list) {
    list.push_back(value);
    list.push_back(charValue);
}

void NullPredicate::findValueExprs(ValueExprList& list) {
    list.push_back(value);
}

// CompPredicate special function
/// @return a parse token type that is the reversed operator of the
///         input token type.
int CompPredicate::reverseOp(int op) {
    switch(op) {
    case SqlSQL2Tokens::NOT_EQUALS_OP:
        return SqlSQL2Tokens::NOT_EQUALS_OP;
    case SqlSQL2Tokens::LESS_THAN_OR_EQUALS_OP:
        return SqlSQL2Tokens::GREATER_THAN_OR_EQUALS_OP;
    case  SqlSQL2Tokens::GREATER_THAN_OR_EQUALS_OP:
        return SqlSQL2Tokens::LESS_THAN_OR_EQUALS_OP;
    case SqlSQL2Tokens::LESS_THAN_OP:
        return SqlSQL2Tokens::GREATER_THAN_OP;
    case  SqlSQL2Tokens::GREATER_THAN_OP:
        return SqlSQL2Tokens::LESS_THAN_OP;
    case SqlSQL2Tokens::EQUALS_OP:
        return SqlSQL2Tokens::EQUALS_OP;
    default:
        throw std::logic_error("Invalid op type for reversing");
    }
}

char const* CompPredicate::lookupOp(int op) {
    switch(op) {
    case SqlSQL2Tokens::NOT_EQUALS_OP:
        return "<>";
    case SqlSQL2Tokens::LESS_THAN_OR_EQUALS_OP:
        return "<=";
    case  SqlSQL2Tokens::GREATER_THAN_OR_EQUALS_OP:
        return ">=";
    case SqlSQL2Tokens::LESS_THAN_OP:
        return "<";
    case  SqlSQL2Tokens::GREATER_THAN_OP:
        return ">";
    case SqlSQL2Tokens::EQUALS_OP:
        return "==";
    default:
        throw std::invalid_argument("Invalid op type");
    }
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

BfTerm::Ptr CompPredicate::clone() const {
    CompPredicate* p = new CompPredicate;
    if(left) p->left = left->clone();
    p->op = op;
    if(right) p->right = right->clone();
    return BfTerm::Ptr(p);
}

BfTerm::Ptr GenericPredicate::clone() const {
    //return BfTerm::Ptr(new GenericPredicate());
    return BfTerm::Ptr();
}

namespace {
    struct valueExprCopy {
        inline ValueExprPtr operator()(ValueExprPtr const& p) {
            return p ? p->clone() : ValueExprPtr();
        }
    };
}

BfTerm::Ptr InPredicate::clone() const {
    InPredicate::Ptr p(new InPredicate);
    if(value) p->value = value->clone();
    std::transform(cands.begin(), cands.end(),
                   std::back_inserter(p->cands),
                   valueExprCopy());
    return BfTerm::Ptr(p);
}

BfTerm::Ptr BetweenPredicate::clone() const {
    BetweenPredicate::Ptr p(new BetweenPredicate);
    if(value) p->value = value->clone();
    if(minValue) p->minValue = minValue->clone();
    if(maxValue) p->maxValue = maxValue->clone();
    return BfTerm::Ptr(p);
}

BfTerm::Ptr LikePredicate::clone() const {
    LikePredicate::Ptr p(new LikePredicate);
    if(value) p->value = value->clone();
    if(charValue) p->charValue = charValue->clone();
    return BfTerm::Ptr(p);
}

BfTerm::Ptr NullPredicate::clone() const {
    NullPredicate::Ptr p(new NullPredicate);
    if(value) p->value = value->clone();
    p->hasNot = hasNot;
    return BfTerm::Ptr(p);
}

}}} // namespace lsst::qserv::query
