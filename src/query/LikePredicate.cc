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
#include "query/LikePredicate.h"

// Qserv headers
#include "query/ColumnRef.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"

namespace lsst { namespace qserv { namespace query {

void LikePredicate::findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {
    if (value != nullptr) {
        value->findColumnRefs(vector);
    }
    if (charValue != nullptr) {
        charValue->findColumnRefs(vector);
    }
}

std::ostream& LikePredicate::putStream(std::ostream& os) const { return QueryTemplate::renderDbg(os, *this); }

void LikePredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r.applyToQT(value);
    if (hasNot) {
        qt.append("NOT");
    }
    qt.append("LIKE");
    r.applyToQT(charValue);
}

void LikePredicate::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    vector.push_back(value);
    vector.push_back(charValue);
}

void LikePredicate::findValueExprRefs(ValueExprPtrRefVector& vector) {
    vector.push_back(value);
    vector.push_back(charValue);
}

BoolFactorTerm::Ptr LikePredicate::clone() const {
    return std::make_shared<LikePredicate>(((nullptr != value) ? value->clone() : nullptr),
                                           ((nullptr != charValue) ? charValue->clone() : nullptr), hasNot);
}

void LikePredicate::dbgPrint(std::ostream& os) const {
    os << "LikePredicate(";
    os << value;
    os << (hasNot ? ", NOT_LIKE" : ", LIKE");
    os << ", " << charValue;
    os << ")";
}

bool LikePredicate::operator==(BoolFactorTerm const& rhs) const {
    auto rhsLikePredicate = dynamic_cast<LikePredicate const*>(&rhs);
    if (nullptr == rhsLikePredicate) {
        return false;
    }
    return util::ptrCompare<ValueExpr>(value, rhsLikePredicate->value) &&
           util::ptrCompare<ValueExpr>(charValue, rhsLikePredicate->charValue) &&
           hasNot == rhsLikePredicate->hasNot;
}

}}}  // namespace lsst::qserv::query
