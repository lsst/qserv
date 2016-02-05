// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 AURA/LSST.
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
#include "proto/ScanTableInfo.h"

// System headers
#include <ostream>

namespace lsst {
namespace qserv {
namespace proto {


std::ostream& operator<<(std::ostream& os, ScanTableInfo const& tbl) {
    os << "(db=" << tbl.db << " table=" << tbl.table;
    os << " lockInMemory=" << tbl.lockInMemory << " scanSpeed=" << tbl.scanSpeed << ")";
    return os;
}


std::ostream& operator<<(std::ostream& os, ScanInfo const& info) {
    os << "ScanInfo{speed=" << info.scanSpeed << " tables: ";
    bool first = true;
    for (auto const& table: info.infoTables) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << table;
    }
    os << "}";
    return os;
}


}}} // namespace lsst::qserv::proto


