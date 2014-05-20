/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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

#include "query/HavingClause.h"

// System headers
#include <iostream>

// Third-party headers
#include <boost/make_shared.hpp>

// Local headers
#include "query/BoolTerm.h"
#include "query/QueryTemplate.h"

namespace lsst {
namespace qserv {
namespace query {

////////////////////////////////////////////////////////////////////////
// HavingClause
////////////////////////////////////////////////////////////////////////
std::ostream&
operator<<(std::ostream& os, HavingClause const& c) {
    if(c._tree.get()) {
        std::string generated = c.getGenerated();
        if(!generated.empty()) { os << "HAVING " << generated; }
    }
    return os;
}
std::string
HavingClause::getGenerated() const {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}
void
HavingClause::renderTo(QueryTemplate& qt) const {
    if(_tree.get()) {
        _tree->renderTo(qt);
    }
}

boost::shared_ptr<HavingClause>
HavingClause::clone() const {
    boost::shared_ptr<HavingClause> hc(new HavingClause);
    if(_tree) {
        hc->_tree = _tree->clone();
    }
    return hc;
}

boost::shared_ptr<HavingClause>
HavingClause::copySyntax() {
    return boost::make_shared<HavingClause>(*this);
}

}}} // namespace lsst::qserv::query
