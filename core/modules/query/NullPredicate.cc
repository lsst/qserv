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
#include "query/NullPredicate.h"

// System headers
#include <algorithm>

// Qserv headers
#include "query/ColumnRef.h"
#include "query/ValueExpr.h"
#include "query/QueryTemplate.h"


namespace lsst {
namespace qserv {
namespace query {


void NullPredicate::findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {
    if (value) { value->findColumnRefs(vector); }
}


std::ostream& NullPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}


void NullPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r.applyToQT(value);
    qt.append("IS");
    if (hasNot) { qt.append("NOT"); }
    qt.append("NULL");
}


void NullPredicate::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    vector.push_back(value);
}


BoolFactorTerm::Ptr NullPredicate::clone() const {
    NullPredicate::Ptr p = std::make_shared<NullPredicate>();
    if (value) p->value = value->clone();
    p->hasNot = hasNot;
    return BoolFactorTerm::Ptr(p);
}


void NullPredicate::dbgPrint(std::ostream& os) const {
    os << "NullPredicate(value:" << value;
    if (hasNot) {
        os << ", NOT";
    }
    os << ")";
}


bool NullPredicate::operator==(const BoolFactorTerm& rhs) const {
    auto rhsNullPredicate = dynamic_cast<NullPredicate const *>(&rhs);
    if (nullptr == rhsNullPredicate) {
        return false;
    }
    return hasNot == rhsNullPredicate->hasNot &&
           util::ptrCompare<ValueExpr>(value, rhsNullPredicate->value);
}


}}} // namespace lsst::qserv::query
