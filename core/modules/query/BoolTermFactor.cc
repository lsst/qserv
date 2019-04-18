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
#include "BoolTermFactor.h"

// Qserv headers
#include "query/BoolTerm.h"


namespace lsst {
namespace qserv {
namespace query {


std::ostream& BoolTermFactor::putStream(std::ostream& os) const {
    if (_term) { return _term->putStream(os); }
    return os;
}


void BoolTermFactor::renderTo(QueryTemplate& qt) const {
    if (_term) { _term->renderTo(qt); }
}


BoolFactorTerm::Ptr BoolTermFactor::clone() const {
    BoolTermFactor* p = new BoolTermFactor;
    if (_term) { p->_term = _term->clone(); }
    return BoolFactorTerm::Ptr(p);
}


BoolFactorTerm::Ptr BoolTermFactor::copySyntax() const {
    BoolTermFactor* p = new BoolTermFactor;
    if (_term) { p->_term = _term->copySyntax(); }
    return BoolFactorTerm::Ptr(p);
}


void BoolTermFactor::dbgPrint(std::ostream& os) const {
    os << "BoolTermFactor(" << _term << ")";
}


bool BoolTermFactor::operator==(BoolFactorTerm const& rhs) const {
    auto rhsTerm = dynamic_cast<BoolTermFactor const *>(&rhs);
    if (nullptr == rhsTerm) {
        return false;
    }
    return util::ptrCompare<BoolTerm>(_term, rhsTerm->_term);
}


void BoolTermFactor::findValueExprs(std::vector<std::shared_ptr<ValueExpr>>& vector) const {
    if (_term) { _term->findValueExprs(vector); }
}


void BoolTermFactor::findValueExprRefs(ValueExprPtrRefVector& vector) {
    if (_term) { _term->findValueExprRefs(vector); }
}


void BoolTermFactor::findColumnRefs(ColumnRef::Vector& vector) const {
    if (_term) { _term->findColumnRefs(vector); }
}


}}} // namespace lsst::qserv::query
