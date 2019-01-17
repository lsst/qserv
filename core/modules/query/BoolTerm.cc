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
  * @file
  *
  * @brief BoolTerm implementations.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "query/BoolTerm.h"

// System headers
#include <algorithm>
#include <iterator>
#include <stdexcept>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "query/Predicate.h"
#include "query/QueryTemplate.h"
#include "query/ValueExpr.h"
#include "util/IterableFormatter.h"


namespace lsst {
namespace qserv {
namespace query {


std::ostream& operator<<(std::ostream& os, BoolTerm const& bt) {
    bt.dbgPrint(os);
    return os;
}


std::ostream& operator<<(std::ostream& os, BoolTerm const* bt) {
    (nullptr == bt) ? os << "nullptr" : os << *bt;
    return os;
}


void BoolTerm::renderList(QueryTemplate& qt,
                          BoolTerm::PtrVector const& terms,
                          std::string const& sep) const {
    int count=0;
    for(auto&& ptr : terms) {
        if (!sep.empty() && ++count > 1) {
            qt.append(sep);
        }
        if (nullptr == ptr) {
            throw std::logic_error("Bad list term");
        }
        bool parensNeeded = getOpPrecedence() > ptr->getOpPrecedence();
        if (parensNeeded) {
            qt.append("(");
        }
        ptr->renderTo(qt);
        if (parensNeeded) {
            qt.append(")");
        }
    }
}


void BoolTerm::renderList(QueryTemplate& qt,
                          std::vector<std::shared_ptr<BoolFactorTerm>> const& terms,
                          std::string const& sep) const {
    int count=0;
    for(auto&& ptr : terms) {
        if (!sep.empty() && ++count > 1) {
            qt.append(sep);
        }
        if (nullptr == ptr) {
            throw std::logic_error("Bad list term");
        }
        bool parensNeeded = getOpPrecedence() > OTHER_PRECEDENCE;
        if (parensNeeded) {
            qt.append("(");
        }
        ptr->renderTo(qt);
        if (parensNeeded) {
            qt.append(")");
        }
    }
}


}}} // namespace lsst::qserv::query
