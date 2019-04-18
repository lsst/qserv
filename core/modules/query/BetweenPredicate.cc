// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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


// Class header
#include "query/BetweenPredicate.h"

// Qserv headers
#include "query/ColumnRef.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"


namespace lsst {
namespace qserv {
namespace query {


void BetweenPredicate::findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {
    if (value) value->findColumnRefs(vector);
    if (minValue) minValue->findColumnRefs(vector);
    if (maxValue) maxValue->findColumnRefs(vector);
}


std::ostream& BetweenPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}


void BetweenPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r.applyToQT(value);
    if (hasNot) qt.append("NOT");
    qt.append("BETWEEN");
    r.applyToQT(minValue);
    qt.append("AND");
    r.applyToQT(maxValue);
}


void BetweenPredicate::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    vector.push_back(value);
    vector.push_back(minValue);
    vector.push_back(maxValue);
}


void BetweenPredicate::findValueExprRefs(ValueExprPtrRefVector& vector) {
    vector.push_back(value);
    vector.push_back(minValue);
    vector.push_back(maxValue);
}


BoolFactorTerm::Ptr BetweenPredicate::clone() const {
    BetweenPredicate::Ptr p = std::make_shared<BetweenPredicate>();
    if (value) p->value = value->clone();
    p->hasNot = hasNot;
    if (minValue) p->minValue = minValue->clone();
    if (maxValue) p->maxValue = maxValue->clone();
    return p;
}


void BetweenPredicate::dbgPrint(std::ostream& os) const {
    os << "BetweenPredicate(";
    os << value;
    os << (hasNot ? ", NOT_BETWEEN" : ", BETWEEN");
    os << ", " << minValue;
    os << ", " << maxValue;
    os << ")";
}


bool BetweenPredicate::operator==(const BoolFactorTerm& rhs) const {
    auto rhsBetweenPredicate = dynamic_cast<BetweenPredicate const *>(&rhs);
    if (nullptr == rhsBetweenPredicate) {
        return false;
    }
    return util::ptrCompare<ValueExpr>(value, rhsBetweenPredicate->value) &&
           hasNot == rhsBetweenPredicate->hasNot &&
           util::ptrCompare<ValueExpr>(minValue, rhsBetweenPredicate->minValue) &&
           util::ptrCompare<ValueExpr>(maxValue, rhsBetweenPredicate->maxValue);
}


}}} // namespace lsst::qserv::query
