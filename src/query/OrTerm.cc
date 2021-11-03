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


#include "OrTerm.h"


#include "query/BoolFactorTerm.h"
#include "query/CopyTerms.h"
#include "util/IterableFormatter.h"


namespace lsst {
namespace qserv {
namespace query {


void OrTerm::renderTo(QueryTemplate& qt) const {
    renderList(qt, _terms, "OR");
}


std::shared_ptr<BoolTerm> OrTerm::clone() const {
    std::shared_ptr<OrTerm> ot = std::make_shared<OrTerm>();
    copyTerms<BoolTerm::PtrVector, deepCopy>(ot->_terms, _terms);
    return ot;
}


std::shared_ptr<OrTerm> OrTerm::copy() const {
    auto orTerm = std::make_shared<OrTerm>();
    copyTerms<BoolTerm::PtrVector, syntaxCopy>(orTerm->_terms, _terms);
    return orTerm;
}


std::shared_ptr<BoolTerm> OrTerm::copySyntax() const {
    return copy();
}


bool OrTerm::merge(const BoolTerm& other) {
    auto otherOr = dynamic_cast<const OrTerm*>(&other);
    if (nullptr == otherOr) {
        return false;
    }
    _terms.insert(_terms.end(), otherOr->_terms.begin(), otherOr->_terms.end());
    return true;
}


void OrTerm::dbgPrint(std::ostream& os) const {
    os << "OrTerm(" << util::printable(_terms, "", "") << ")";
}


bool OrTerm::operator==(const BoolTerm& rhs) const {
    auto rhsOrTerm = dynamic_cast<OrTerm const *>(&rhs);
    if (nullptr == rhsOrTerm) {
        return false;
    }
    return util::vectorPtrCompare<BoolTerm>(_terms, rhsOrTerm->_terms);
}


}}} // namespace lsst::qserv::query
