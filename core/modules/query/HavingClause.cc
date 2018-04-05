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

// Third-party headers

// Qserv headers
#include "query/BoolTerm.h"
#include "query/QueryTemplate.h"
#include "util/PointerCompare.h"
#include "util/DbgPrintHelper.h"

namespace lsst {
namespace qserv {
namespace query {

////////////////////////////////////////////////////////////////////////
// HavingClause
////////////////////////////////////////////////////////////////////////
std::ostream&
operator<<(std::ostream& os, HavingClause const& c) {
    if (c._tree.get()) {
        std::string generated = c.getGenerated();
        if (!generated.empty()) { os << "HAVING " << generated; }
    }
    return os;
}
std::string
HavingClause::getGenerated() const {
    QueryTemplate qt;
    renderTo(qt);
    return qt.sqlFragment();
}
void
HavingClause::renderTo(QueryTemplate& qt) const {
    if (_tree.get()) {
        _tree->renderTo(qt);
    }
}

std::shared_ptr<HavingClause>
HavingClause::clone() const {
    std::shared_ptr<HavingClause> hc = std::make_shared<HavingClause>();
    if (_tree) {
        hc->_tree = _tree->clone();
    }
    return hc;
}

std::shared_ptr<HavingClause>
HavingClause::copySyntax() {
    return std::make_shared<HavingClause>(*this);
}

void
HavingClause::findValueExprs(ValueExprPtrVector& list) {
    if (_tree) { _tree->findValueExprs(list); }
}

bool HavingClause::operator==(const HavingClause& rhs) const {
    return util::ptrCompare<BoolTerm>(_tree, rhs._tree);
}

void HavingClause::dbgPrint(std::ostream& os) const {
    os << "HavingClause(tree:" << util::DbgPrintPtrH<BoolTerm>(_tree) << ")";
}

}}} // namespace lsst::qserv::query
