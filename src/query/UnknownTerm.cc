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
#include "UnknownTerm.h"

// Qserv headers
#include "query/QueryTemplate.h"

namespace lsst::qserv::query {

void UnknownTerm::dbgPrint(std::ostream& os) const { os << "UnknownTerm()"; }

bool UnknownTerm::operator==(const BoolTerm& rhs) const { return true; }

std::ostream& UnknownTerm::putStream(std::ostream& os) const { return os << "--UNKNOWNTERM--"; }

void UnknownTerm::renderTo(QueryTemplate& qt) const { qt.append("unknown"); }

std::shared_ptr<BoolTerm> UnknownTerm::clone() const {
    return std::make_shared<UnknownTerm>();  // TODO what is unknown now?
}

}  // namespace lsst::qserv::query
