// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2016 AURA/LSST.
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
 * @file
 *
 * @brief Implementation of HavingClause
 *
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "query/HavingClause.h"

// System headers
#include <iostream>

// Qserv headers
#include "query/BoolTerm.h"
#include "query/QueryTemplate.h"
#include "util/PointerCompare.h"

namespace lsst::qserv::query {

////////////////////////////////////////////////////////////////////////
// HavingClause
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, HavingClause const& c) {
    os << "HavingClause(" << c._tree << ")";
    return os;
}

std::ostream& operator<<(std::ostream& os, HavingClause const* c) {
    (nullptr == c) ? os << "nullptr" : os << *c;
    return os;
}

std::string HavingClause::getGenerated() const {
    QueryTemplate qt;
    renderTo(qt);
    return qt.sqlFragment();
}

void HavingClause::renderTo(QueryTemplate& qt) const {
    if (_tree.get()) {
        _tree->renderTo(qt);
    }
}

std::shared_ptr<HavingClause> HavingClause::clone() const {
    std::shared_ptr<HavingClause> hc = std::make_shared<HavingClause>();
    if (_tree) {
        hc->_tree = _tree->clone();
    }
    return hc;
}

std::shared_ptr<HavingClause> HavingClause::copySyntax() { return std::make_shared<HavingClause>(*this); }

void HavingClause::findValueExprs(ValueExprPtrVector& list) const {
    if (_tree) {
        _tree->findValueExprs(list);
    }
}

void HavingClause::findValueExprRefs(ValueExprPtrRefVector& list) {
    if (_tree) {
        _tree->findValueExprRefs(list);
    }
}

bool HavingClause::operator==(const HavingClause& rhs) const {
    return util::ptrCompare<BoolTerm>(_tree, rhs._tree);
}

}  // namespace lsst::qserv::query
