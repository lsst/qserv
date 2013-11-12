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
/**
  * @file FromList.cc
  *
  * @brief Implementation of FromList
  *
  * @author Daniel L. Wang, SLAC
  */
#include "query/FromList.h"

namespace qMaster=lsst::qserv::master;

std::ostream&
qMaster::operator<<(std::ostream& os, qMaster::FromList const& fl) {
    os << "FROM ";
    if(fl._tableRefns.get() && fl._tableRefns->size() > 0) {
        TableRefnList const& refList = *(fl._tableRefns);
        std::copy(refList.begin(), refList.end(),
                  std::ostream_iterator<TableRefN::Ptr>(os,", "));
    } else {
        os << "(empty)";
    }
    return os;
}

std::string
qMaster::FromList::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
qMaster::FromList::renderTo(qMaster::QueryTemplate& qt) const {
    if(_tableRefns.get() && _tableRefns->size() > 0) {
        TableRefnList const& refList = *_tableRefns;
        std::for_each(refList.begin(), refList.end(), TableRefN::render(qt));
    }
}

boost::shared_ptr<qMaster::FromList> qMaster::FromList::copySyntax() {
    boost::shared_ptr<FromList> newL(new FromList(*this));
    // Shallow copy of expr list is okay.
    newL->_tableRefns.reset(new TableRefnList(*_tableRefns));
    // For the other fields, default-copied versions are okay.
    return newL;
}

boost::shared_ptr<qMaster::FromList> qMaster::FromList::copyDeep() const {
    // FIXME
    boost::shared_ptr<FromList> newL(new FromList(*this));
    // Shallow copy of expr list is okay.
    newL->_tableRefns.reset(new TableRefnList(*_tableRefns));
    // For the other fields, default-copied versions are okay.
    return newL;
}
