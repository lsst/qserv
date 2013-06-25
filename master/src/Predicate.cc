/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#include "lsst/qserv/master/Predicate.h"
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/ValueExpr.h"
#include "SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
}

namespace lsst {
namespace qserv {
namespace master {

void CompPredicate::findColumnRefs(ColumnRefMap::List& list) {
    if(left) { left->findColumnRefs(list); }
    if(right) { right->findColumnRefs(list); }
}
void InPredicate::findColumnRefs(ColumnRefMap::List& list) {
    if(value) { value->findColumnRefs(list); }
    std::list<boost::shared_ptr<ValueExpr> >::iterator i;
    for(i=cands.begin(); i != cands.end(); ++i) {
        (**i).findColumnRefs(list);
    }
}
void BetweenPredicate::findColumnRefs(ColumnRefMap::List& list) {
    if(value) { value->findColumnRefs(list); }
    if(minValue) { minValue->findColumnRefs(list); }
    if(maxValue) { maxValue->findColumnRefs(list); }
}

std::ostream& qMaster::CompPredicate::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::InPredicate::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::BetweenPredicate::putStream(std::ostream& os) const {
    // FIXME
    return os;
}

void qMaster::CompPredicate::renderTo(QueryTemplate& qt) const {

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

void qMaster::InPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r(value);
    qt.append("IN");
    ValueExpr::render rComma(qt, true);
    qt.append("(");
    std::for_each(cands.begin(), cands.end(), rComma);
    qt.append(")");
}

void qMaster::BetweenPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r(value);
    qt.append("BETWEEN");
    r(minValue);
    qt.append("AND");
    r(maxValue);
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

}}} // lsst::qserv::master
