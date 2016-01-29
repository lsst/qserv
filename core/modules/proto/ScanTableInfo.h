// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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

#ifndef LSST_QSERV_PROTO_SCANTABLEINFO_H
#define LSST_QSERV_PROTO_SCANTABLEINFO_H

// System headers
#include <string>
#include <vector>

namespace lsst {
namespace qserv {
namespace proto {



/// Structure to store shared scan information for a single table.
///
struct ScanTableInfo {
    using ListOf = std::vector<ScanTableInfo>;

    ScanTableInfo(std::string const& db_, std::string const& table_) : db(db_), table(table_) {}
    ScanTableInfo(std::string const& db_, std::string const& table_,
                  bool lockInMemory_, int scanSpeed_) :
                  db(db_), table(table_), lockInMemory(lockInMemory_), scanSpeed(scanSpeed_) {}
    std::string db    {""};
    std::string table {""};
    bool lockInMemory {false};
    int  scanSpeed    {0};
};

struct ScanInfo {
    /// Threshold priority values. Scan priorities are not limited to these values.
    enum Priority { FASTEST = 0, FAST = 1, MEDIUM = 2, SLOW = 3 };

    ScanTableInfo::ListOf infoTables;
    int priority{Priority::FASTEST};
};

}}} // namespace lsst::qserv::proto

#endif // LSST_QSERV_PROTO_SCANTABLEINFO_H
