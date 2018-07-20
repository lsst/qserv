// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
#include "query/JoinRef.h"

#include "util/PointerCompare.h"

namespace lsst {
namespace qserv {
namespace query {

std::ostream& JoinRef::putStream(std::ostream& os) const {
    QueryTemplate t;
    _putJoinTemplate(t);
    os << "Join( " << t << " ";
    if (_right) {_right->putStream(os); }
    else { os << "<BROKEN_JOIN>";}
    if (_spec) { _spec->putStream(os << " "); }
    return os;
}

void JoinRef::putTemplate(QueryTemplate& qt) const {
    _putJoinTemplate(qt);
    _right->putTemplate(qt);
    if (_spec) { _spec->putTemplate(qt); }
}

JoinRef::Ptr JoinRef::clone() const {
    TableRef::Ptr r;
    if (_right) { r = _right->clone(); }
    JoinSpec::Ptr s;
    if (_spec) { s = _spec->clone(); }
    return std::make_shared<JoinRef>(r, _joinType, _isNatural, s);
}

void JoinRef::_putJoinTemplate(QueryTemplate& qt) const {
    if (_isNatural) { qt.append("NATURAL"); }

    switch(_joinType) {
    case DEFAULT: break;
    case INNER: qt.append("INNER"); break;
    case LEFT: qt.append("LEFT"); qt.append("OUTER"); break;
    case RIGHT: qt.append("RIGHT"); qt.append("OUTER"); break;
    case FULL: qt.append("FULL"); qt.append("OUTER"); break;
    case CROSS: qt.append("CROSS"); break;
    case UNION: qt.append("UNION"); break;
    }

    qt.append("JOIN");
}
std::ostream& operator<<(std::ostream& os, JoinRef const& js) {
    os << "JoinRef(";
    os << "right:" << js._right;
    os << ", joinType:";
    switch (js._joinType) {
    default: os << "!!unhandled!!"; break;
    case JoinRef::DEFAULT: os << "DEFAULT"; break;
    case JoinRef::INNER: os << "INNER"; break;
    case JoinRef::LEFT: os << "LEFT"; break;
    case JoinRef::RIGHT: os << "RIGHT"; break;
    case JoinRef::FULL: os << "FULL"; break;
    case JoinRef::CROSS: os << "CROSS"; break;
    case JoinRef::UNION: os << "UNION"; break;
    }
    os << ", isNatural:" << js._isNatural;
    os << ", joinSpec:" << js._spec;
    os << ")";
    return os;
}

std::ostream& operator<<(std::ostream& os, JoinRef const* js) {
    return js->putStream(os);
}

bool JoinRef::operator==(const JoinRef& rhs) const {
    return  util::ptrCompare<TableRef>(_right, rhs._right) &&
            _joinType == rhs._joinType &&
            _isNatural == rhs._isNatural &&
            util::ptrCompare<JoinSpec>(_spec, rhs._spec);
}

}}} // lsst::qserv::query
