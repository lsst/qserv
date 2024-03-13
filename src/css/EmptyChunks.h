// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2019 AURA/LSST.
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
 * @brief Empty-chunks tracker. Reads an on-disk file from cwd, but
 * should ideally query (and cache) table state.
 *
 * @Author Daniel L. Wang, SLAC
 */

#ifndef LSST_QSERV_CSS_EMPTYCHUNKS_H
#define LSST_QSERV_CSS_EMPTYCHUNKS_H

// System headers
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

namespace lsst::qserv::css {

class DbInterfaceMySql;

/// High-level empty-chunk-tracking class. Tracks empty chunks
/// per-database. In the future, we will likely migrate to a
/// per-partitioning-group scheme, at which point, we will re-think
/// the db-based dispatch as well (user tables in the partitioning
/// group may be extremely sparse).
class EmptyChunks {
public:
    /// Doing anything with _css inside the constructor would be dangerous.
    EmptyChunks(std::shared_ptr<DbInterfaceMySql> const& databaseInterface)
            : _databaseInterface(databaseInterface) {}

    // This form of construction is used for unit testing of the class API.
    EmptyChunks(std::map<std::string, std::set<int>> const& database2chunks);

    EmptyChunks() = delete;
    EmptyChunks(EmptyChunks const&) = delete;
    EmptyChunks(EmptyChunks&&) = delete;
    EmptyChunks& operator=(EmptyChunks const&) = delete;

    ~EmptyChunks() = default;

    /// @return set of empty chunks for this db
    std::shared_ptr<std::set<int> const> getEmpty(std::string const& db);

    /// @return true if db/chunk is empty
    bool isEmpty(std::string const& db, int chunk);

    /// Clear cache for empty chunk list so that on next call to above methods
    /// empty chunk list is re-populated. If database name is empty then cache
    /// for all databases is cleared.
    void clearCache(std::string const& db = std::string());

private:
    /// @return all the empty chunks for database 'db'.
    std::set<int> _populate(std::string const& db);

    /// Allow access to the persistent store of the empty chunk sets
    std::shared_ptr<DbInterfaceMySql> const _databaseInterface;

    /// Container for on-per database empty chunks sets (cache)
    std::map<std::string, std::shared_ptr<std::set<int>>> _sets;

    /// The mutex for the thread-safe implementation of the synchronized methods
    std::mutex _mtx;
};

}  // namespace lsst::qserv::css

#endif  // LSST_QSERV_CSS_EMPTYCHUNKS_H
