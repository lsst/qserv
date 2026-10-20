// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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
/**
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "query/JoinSpec.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "query/BoolTerm.h"
#include "query/ColumnRef.h"
#include "query/QueryTemplate.h"

namespace lsst::qserv::query {

void JoinSpec::_validate() const {
    if ((_onTerm != nullptr) == (_usingColumn != nullptr)) {
        throw std::logic_error("JoinSpec must contain exactly *one* of: ON / USING");
    }
}

std::ostream& operator<<(std::ostream& os, JoinSpec const& js) {
    os << "JoinSpec(";
    os << js._usingColumn;
    os << ", " << js._onTerm;
    os << ")";
    return os;
}

std::ostream& operator<<(std::ostream& os, JoinSpec const* js) {
    (nullptr == js) ? os << "nullptr" : os << *js;
    return os;
}

std::ostream& JoinSpec::putStream(std::ostream& os) const {
    // boilerplate impl until we can think of something better
    QueryTemplate qt;
    putTemplate(qt);
    return os << qt;
}

void JoinSpec::putTemplate(QueryTemplate& qt) const {
    _validate();
    auto const prevAliasMode = qt.getAliasMode();
    qt.setAliasMode(QueryTemplate::NO_VALUE_ALIAS_USE_TABLE_ALIAS);
    if (_onTerm) {
        qt.append("ON");
        _onTerm->renderTo(qt);
    } else {
        qt.append("USING");
        qt.append("(");
        qt.append(*_usingColumn);  // FIXME: update to support column lists
        qt.append(")");
    }
    qt.setAliasMode(prevAliasMode);
}

JoinSpec::Ptr JoinSpec::clone() const {
    _validate();
    if (_usingColumn) {
        std::shared_ptr<ColumnRef> col = std::make_shared<ColumnRef>(*_usingColumn);
        return std::make_shared<JoinSpec>(col);
    } else {
        return std::make_shared<JoinSpec>(_onTerm->copySyntax());
    }
}

bool JoinSpec::operator==(const JoinSpec& rhs) const {
    return util::ptrCompare<ColumnRef>(_usingColumn, rhs._usingColumn) &&
           util::ptrCompare<BoolTerm>(_onTerm, rhs._onTerm);
}

}  // namespace lsst::qserv::query
