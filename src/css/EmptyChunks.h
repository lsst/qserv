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
#include <string>

// Qserv headers
#include "global/intTypes.h"

namespace lsst { namespace qserv { namespace css {

class DbInterfaceMySql;

/// High-level empty-chunk-tracking class. Tracks empty chunks
/// per-database. In the future, we will likely migrate to a
/// per-partitioning-group scheme, at which point, we will re-think
/// the db-based dispatch as well (user tables in the partitioning
/// group may be extremely sparse).
class EmptyChunks {
public:
    /// Doing anything with _css inside the constructor would be dangerous.
    EmptyChunks(std::shared_ptr<DbInterfaceMySql> const& dbI, std::string const& path = ".",
                std::string const& fallbackFile = "emptyChunks.txt")
            : _dbI(dbI), _path(path), _fallbackFile(fallbackFile) {}

    EmptyChunks() = delete;
    EmptyChunks(EmptyChunks const&) = delete;
    EmptyChunks(EmptyChunks&&) = delete;
    EmptyChunks& operator=(EmptyChunks const&) = delete;

    ~EmptyChunks() = default;

    // accessors

    /// @return set of empty chunks for this db
    std::shared_ptr<IntSet const> getEmpty(std::string const& db);

    /// @return true if db/chunk is empty
    bool isEmpty(std::string const& db, int chunk);

    /// Clear cache for empty chunk list so that on next call to above methods
    /// empty chunk list is re-populated. If database name is empty then cache
    // for all databases is cleared.
    void clearCache(std::string const& db = std::string()) const;

private:
    // Convenience types
    typedef std::shared_ptr<IntSet> IntSetPtr;
    typedef std::shared_ptr<IntSet const> IntSetConstPtr;

    /// @return all the empty chunks for database 'db'.
    IntSet _populate(std::string const& db);

    typedef std::map<std::string, IntSetPtr> IntSetMap;
    std::shared_ptr<DbInterfaceMySql> const _dbI;  ///< allow access to empty chunks table.
    std::string _path;                             ///< Search path for empty chunks files
    std::string _fallbackFile;                     ///< Fallback path for empty chunks
    mutable IntSetMap _sets;                       ///< Container for empty chunks sets (cache)
    mutable std::mutex _setsMutex;
};

}}}  // namespace lsst::qserv::css

#endif  // LSST_QSERV_CSS_EMPTYCHUNKS_H
