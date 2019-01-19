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
#include "PassListTerm.h"

// Qserv headers
#include "query/QueryTemplate.h"
#include "util/IterableFormatter.h"


namespace lsst {
namespace qserv {
namespace query {


std::ostream& PassListTerm::putStream(std::ostream& os) const {
    std::copy(_terms.begin(), _terms.end(),
              std::ostream_iterator<std::string>(os, " "));
    return os;
}


void PassListTerm::renderTo(QueryTemplate& qt) const {
    qt.append("(");
    bool isFirst=true;
    for (auto&& str : _terms) {
        if (!isFirst) {
            qt.append(",");
        }
        qt.append(str);
        isFirst = false;
    }
    qt.append(")");
}


BoolFactorTerm::Ptr PassListTerm::clone() const {
    auto p = std::make_shared<PassListTerm>();
    std::copy(_terms.begin(), _terms.end(), std::back_inserter(p->_terms));
    return p;
}


BoolFactorTerm::Ptr PassListTerm::copySyntax() const {
    auto p = std::make_shared<PassListTerm>();
    p->_terms = _terms;
    return p;
}


void PassListTerm::dbgPrint(std::ostream& os) const {
    os << "PassListTerm(" << util::printable(_terms) << ")";
}


bool PassListTerm::operator==(const BoolFactorTerm& rhs) const {
    auto rhsTerm = dynamic_cast<PassListTerm const *>(&rhs);
    if (nullptr == rhsTerm) {
        return false;
    }
    return _terms == rhsTerm->_terms;
}


}}} // namespace lsst::qserv::query
