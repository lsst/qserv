// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

/// \file
/// \brief Table metadata object pooling implementation.

// Class header
#include "qana/TableInfoPool.h"

// System headers
#include <algorithm>
#include <memory>
#include <utility>

// Third-party headers

// Qserv headers
#include "css/CssAccess.h"
#include "qana/InvalidTableError.h"
#include "qana/TableInfo.h"
#include "query/QueryContext.h"

namespace {
/// `TableInfoLt` is a less-than comparison functor for non-null `TableInfo`
/// pointers.
struct TableInfoLt {
    bool operator()(std::unique_ptr<lsst::qserv::qana::TableInfo const> const& t1,
            std::unique_ptr<lsst::qserv::qana::TableInfo const> const& t2) const {
        return *t1 < *t2;
    }
};
}


namespace lsst {
namespace qserv {
namespace qana {

TableInfo const*
TableInfoPool::get(std::string const& db, std::string const& table) {

    std::string const& db_ = db.empty() ? _defaultDb : db;

    // Note that t.kind is irrelevant to the search,
    // and is set to an arbitrary value.
    std::unique_ptr<TableInfo const> t(new TableInfo(db, table, TableInfo::DIRECTOR));
    auto range = std::equal_range(_pool.begin(), _pool.end(), t, TableInfoLt());
    if (range.first != range.second) {
        return range.first->get();
    }

    css::TableParams const tParam = _css.getTableParams(db_, table);
    css::PartTableParams const& partParam = tParam.partitioning;
    int const chunkLevel = partParam.chunkLevel();
    // unpartitioned table
    if (chunkLevel == 0) {
        return nullptr;
    }
    // match table
    if (tParam.match.isMatchTable()) {
        css::MatchTableParams const& m = tParam.match;
        double angSep = m.angSep;
        std::unique_ptr<MatchTableInfo> infoPtr(new MatchTableInfo(db_, table, angSep));
        infoPtr->director.first = dynamic_cast<DirTableInfo const*>(
            get(db_, m.dirTable1));
        infoPtr->director.second = dynamic_cast<DirTableInfo const*>(
            get(db_, m.dirTable2));
        if (!infoPtr->director.first || !infoPtr->director.second) {
            throw InvalidTableError(db_ + "." + table + " is a match table, but"
                                    " does not reference two director tables!");
        }
        if (m.dirColName1 == m.dirColName2 ||
            m.dirColName1.empty() || m.dirColName2.empty()) {
            throw InvalidTableError("Match table " + db_ + "." + table +
                                    " metadata does not contain 2 non-empty"
                                    " and distinct director column names!");
        }
        infoPtr->fk.first = m.dirColName1;
        infoPtr->fk.second = m.dirColName2;
        if (infoPtr->director.first->partitioningId !=
            infoPtr->director.second->partitioningId) {
            throw InvalidTableError("Match table " + db_ + "." + table +
                                    " relates two director tables with"
                                    " different partitionings!");
        }
        return _insert(std::move(infoPtr));
    }
    std::string const& dirTable = partParam.dirTable;
    // director table
    if (dirTable.empty() || dirTable == table) {
        if (chunkLevel != 2) {
            throw InvalidTableError(db_ + "." + table + " is a director "
                                    "table, but cannot be sub-chunked!");
        }
        css::StripingParams dbStriping = _css.getDbStriping(db_);
        // use per-table or per-database overlap value
        double overlap = partParam.overlap != 0.0 ? partParam.overlap : dbStriping.overlap;
        std::unique_ptr<DirTableInfo> infoPtr(new DirTableInfo(db_, table, overlap));
        std::vector<std::string> v = _css.getPartTableParams(db, table).partitionCols();
        if (v.size() != 3 ||
            v[0].empty() || v[1].empty() || v[2].empty() ||
            v[0] == v[1] || v[1] == v[2] || v[0] == v[2]) {
            throw InvalidTableError("Director table " + db_ + "." + table +
                                    " metadata does not contain non-empty and"
                                    " distinct director, longitude and"
                                    " latitude column names.");
        }
        infoPtr->pk = v[2];
        infoPtr->lon = v[0];
        infoPtr->lat = v[1];
        infoPtr->partitioningId = dbStriping.partitioningId;
        return _insert(std::move(infoPtr));
    }
    // child table
    if (chunkLevel != 1) {
        throw InvalidTableError(db_ + "." + table + " is a child"
                                " table, but can be sub-chunked!");
    }
    std::unique_ptr<ChildTableInfo> infoPtr(new ChildTableInfo(db_, table));
    infoPtr->director = dynamic_cast<DirTableInfo const*>(
        get(db_, partParam.dirTable));
    if (!infoPtr->director) {
        throw InvalidTableError(db_ + "." + table + " is a child table, but"
                                " does not reference a director table!");
    }
    infoPtr->fk = partParam.dirColName;
    if (infoPtr->fk.empty()) {
        throw InvalidTableError("Child table " + db_ + "." + table + " metadata"
                                " does not contain a director column name!");
    }

    return _insert(std::move(infoPtr));
}

TableInfo const*
TableInfoPool::_insert(std::unique_ptr<TableInfo const> t) {
    if (t != nullptr) {
        Pool::iterator iter =
            std::upper_bound(_pool.begin(), _pool.end(), t, TableInfoLt());
        iter = _pool.insert(iter, std::move(t));
        return iter->get();
    }
    return nullptr;
}

}}} // namespace lsst::qserv::qana
