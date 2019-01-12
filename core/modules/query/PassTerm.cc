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

#include "query/PassTerm.h"

#include "query/BoolFactorTerm.h"
#include "query/QueryTemplate.h"


namespace lsst {
namespace qserv {
namespace query {


BoolFactorTerm::Ptr PassTerm::copySyntax() const {
    PassTerm* p = new PassTerm;
    p->_text = _text;
    return BoolFactorTerm::Ptr(p);
}


void PassTerm::dbgPrint(std::ostream& os) const {
    os << "PassTerm('";
    if ("(" == _text) os << "LHP";
    else if (")" == _text) os << "RHP";
    else os << _text;
    os << "')";
}


bool PassTerm::operator==(const BoolFactorTerm& rhs) const {
    auto rhsPassTerm = dynamic_cast<PassTerm const *>(&rhs);
    if (nullptr == rhsPassTerm) {
        return false;
    }
    return _text == rhsPassTerm->_text;
}


std::ostream& PassTerm::putStream(std::ostream& os) const {
    return os << _text;
}


void PassTerm::renderTo(QueryTemplate& qt) const {
    qt.append(_text);
}


}}} // namespace lsst::qserv::query
