/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// Class header
#include "ValueExprPredicate.h"

// Qserv headers
#include "query/ValueExpr.h"
#include "util/PointerCompare.h"

namespace lsst::qserv::query {

BoolFactorTerm::Ptr ValueExprPredicate::copySyntax() const { return clone(); }

void ValueExprPredicate::dbgPrint(std::ostream& os) const {
    os << "ValueExprPredicate(" << _valueExpr << ")";
}

BoolFactorTerm::Ptr ValueExprPredicate::clone() const {
    return std::make_shared<ValueExprPredicate>(_valueExpr->clone());
}

std::ostream& ValueExprPredicate::putStream(std::ostream& os) const {
    return QueryTemplate::renderDbg(os, *this);
}

void ValueExprPredicate::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt, false);
    r.applyToQT(_valueExpr);
}

void ValueExprPredicate::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    vector.push_back(_valueExpr);
}

void ValueExprPredicate::findValueExprRefs(ValueExprPtrRefVector& vector) { vector.push_back(_valueExpr); }

void ValueExprPredicate::findColumnRefs(std::vector<std::shared_ptr<ColumnRef>>& vector) const {
    _valueExpr->findColumnRefs(vector);
}

bool ValueExprPredicate::operator==(BoolFactorTerm const& rhs) const {
    auto rhsValueExprPredicate = dynamic_cast<ValueExprPredicate const*>(&rhs);
    if (nullptr == rhsValueExprPredicate) {
        return false;
    }
    return util::ptrCompare(_valueExpr, rhsValueExprPredicate->_valueExpr);
}

}  // namespace lsst::qserv::query
