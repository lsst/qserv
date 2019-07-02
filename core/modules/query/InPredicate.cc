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
#include "query/InPredicate.h"

// System headers
#include <algorithm>

// Qserv headers
#include "query/ColumnRef.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"
#include "util/IterableFormatter.h"


namespace lsst {
namespace qserv {
namespace query {


namespace {

struct valueExprCopy {
    inline std::shared_ptr<ValueExpr> operator()(std::shared_ptr<ValueExpr> const& p) {
        return p ? p->clone() : std::shared_ptr<ValueExpr>();
    }
};

}


void InPredicate::findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {
    if (value) {
        value->findColumnRefs(vector);
    }
    for(auto&& valueExpr : cands) {
        valueExpr->findColumnRefs(vector);
    }
}


std::ostream& InPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}


void InPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r.applyToQT(value);
    if (hasNot) qt.append("NOT");
    qt.append("IN");
    ValueExpr::render rComma(qt, true);
    qt.append("(");
    for (auto& cand : cands) {
        rComma.applyToQT(cand);
    }
    qt.append(")");
}


void InPredicate::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    vector.push_back(value);
    vector.insert(vector.end(), cands.begin(), cands.end());
}


void InPredicate::findValueExprRefs(ValueExprPtrRefVector& vector) {
    vector.push_back(value);
    vector.insert(vector.end(), cands.begin(), cands.end());
}


BoolFactorTerm::Ptr InPredicate::clone() const {
    InPredicate::Ptr p  = std::make_shared<InPredicate>();
    if (value) p->value = value->clone();
    std::transform(cands.begin(), cands.end(),
                   std::back_inserter(p->cands),
                   valueExprCopy());
    p->hasNot = hasNot;
    return BoolFactorTerm::Ptr(p);
}


void InPredicate::dbgPrint(std::ostream& os) const {
    os << "InPredicate(";
    os << value;
    os << (hasNot ? ", NOT_IN" : ", IN");
    os << ", " << util::printable(cands, "", "");
    os << ")";
}


bool InPredicate::operator==(BoolFactorTerm const& rhs) const {
    auto rhsInPredicate = dynamic_cast<InPredicate const*>(&rhs);
    if (nullptr == rhsInPredicate) {
        return false;
    }
    return util::ptrCompare<ValueExpr>(value, rhsInPredicate->value) &&
           util::vectorPtrCompare<ValueExpr>(cands, rhsInPredicate->cands) &&
           hasNot == rhsInPredicate->hasNot;
}


}}} // namespace lsst::qserv::query
