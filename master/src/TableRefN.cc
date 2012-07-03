/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
// TableRefN, SimpleTableN, JoinRefN implementations
#include "lsst/qserv/master/TableRefN.h"
#include <sstream>

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
}

namespace lsst { namespace qserv { namespace master {
////////////////////////////////////////////////////////////////////////
// TableRefN 
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, TableRefN const& refN) {
    return refN.putStream(os);
}
std::ostream& operator<<(std::ostream& os, TableRefN const* refN) {
    return refN->putStream(os);
}

void TableRefN::render::operator()(TableRefN const& refN) {
    if(_count++ > 0) _qt.append(",");
    refN.putTemplate(_qt);
}

void JoinRefN::apply(TableRefN::Func& f) {
    // Apply over myself and my join tables.
    // FIXME
}

}}} // lsst::qserv::master
