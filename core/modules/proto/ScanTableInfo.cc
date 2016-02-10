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
#include <algorithm>
#include <ostream>

namespace lsst {
namespace qserv {
namespace proto {

/// @return 0 if equal, -1 if this < rhs, 1 if this > rhs
int ScanTableInfo::compare(ScanTableInfo const& rhs) const {
    // Having no tables to lock is less than having tables to lock.
    if (!lockInMemory) {
        if (rhs.lockInMemory) return -1;
        return 0;
    } else if (!rhs.lockInMemory) return 1;
    // Both have tables to lock, compare scanSpeed
    if (scanRating < rhs.scanRating) return -1;
    if (scanRating > rhs.scanRating) return 1;
    // Scan speeds equal, compare database names alphabetically
    int dbComp = db.compare(rhs.db);
    if (dbComp < 0) return -1;
    if (dbComp > 0) return 1;
    // compare table names
    int tblComp = table.compare(rhs.table);
    if (tblComp < 0) return -1;
    if (tblComp > 0) return 1;
    return 0;
}


/// Compare the tables in the scanInfo objects, the purpose being to
/// get similar scanInfo objects to group together when sorting.
/// @return 0 if approximately equal, -1 if this < rhs, 1 if this > rhs
/// Faster (easier) scans are less than slower (larger) scans.
/// Precondition, tables must be sorted before calling this function.
int ScanInfo::compareTables(ScanInfo const& rhs) {
    if (infoTables.size() == 0) {
        if (rhs.infoTables.size() == 0) return 0;
        return -1; // this is faster
    } else if (rhs.infoTables.size() == 0) return 1; // rhs is faster
    // Need to compare tables. The point is to get the slowest tables
    // grouped up together, so number of tables is not very important.
    auto thsIter = infoTables.begin();
    auto thsEnd  = infoTables.end();
    auto rhsIter = rhs.infoTables.begin();
    auto rhsEnd  = rhs.infoTables.end();
    for (; thsIter!=thsEnd && rhsIter!=rhsEnd; ++thsIter, ++rhsIter) {
        int tblComp = thsIter->compare(*rhsIter);
        if (tblComp < 0) return -1; // this is faster
        if (tblComp > 0) return 1; // rhs is faster
    }
    // Enough similarity (at least one table in common) to consider the tables equal.
    return 0;
}


/// Sort the tables using compareTables to have the slowest tables first.
void ScanInfo::sortTablesSlowestFirst() {
    auto func = [](ScanTableInfo const& x, ScanTableInfo const& y) -> bool {
        return x.compare(y) > 0;
    };
    std::sort(infoTables.begin(), infoTables.end(), func);
}


std::ostream& operator<<(std::ostream& os, ScanTableInfo const& tbl) {
    os << "(db=" << tbl.db << " table=" << tbl.table;
    os << " lockInMemory=" << tbl.lockInMemory << " scanRating=" << tbl.scanRating << ")";
    return os;
}


std::ostream& operator<<(std::ostream& os, ScanInfo const& info) {
    os << "ScanInfo{speed=" << info.scanRating << " tables: ";
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


