// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
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

#ifndef LSST_QSERV_WPUBLISH_CHUNKINVENTORY_H
#define LSST_QSERV_WPUBLISH_CHUNKINVENTORY_H

// System headers
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <stdexcept>
#include <string>

// Qserv headers
#include "global/ResourceUnit.h"
#include "mysql/MySqlConfig.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace sql {
    class SqlConnection;
}}}

// This header declarations
namespace lsst {
namespace qserv {
namespace wpublish {

/// Exception class for reporting problems with parameter values
class InvalidParamError : public std::runtime_error {
public:
    InvalidParamError (std::string const& msg)
        :   std::runtime_error(msg) {
    }
};

/// Exception class for reporting problems during query processing
class QueryError : public std::runtime_error {
public:
    QueryError (std::string const& msg)
        :   std::runtime_error(msg) {
    }
};

/**
 * ChunkInventory is a class that implements the capability to retrieve
 * table publishing information from a qserv worker's associated mysqld.
 * It includes helper functions for checking the resulting data structure for
 * the existence of certain xrootd paths.
 */
class ChunkInventory {

public:

    // These should be converted to unordered_* with C++11
    typedef std::set<int> ChunkMap;
    typedef std::map<std::string, ChunkMap> ExistMap;

    typedef std::shared_ptr<ChunkInventory>       Ptr;
    typedef std::shared_ptr<ChunkInventory const> CPtr;

    ChunkInventory() = default;
    ChunkInventory(std::string const& name, std::shared_ptr<sql::SqlConnection> sc);
    ChunkInventory(ExistMap const& existMap, std::string const& name, std::string const& id);

    void init(std::string const& name, mysql::MySqlConfig const& mysqlConfig);

    /// Rebuilding the Chunks table
    void rebuild(std::string const& name, mysql::MySqlConfig const& mysqlConfig);

    /// Add the chunk to the inventory if it's not registered yet
    void add(std::string const& db, int chunk);

    /// Add the chunk to the inventory if it's not registered yet,
    /// also add a new entry to the database table
    void add(std::string const& db, int chunk, mysql::MySqlConfig const& mySqlConfig);

    /// Remove chunk from the inventory if it's still registered
    void remove(std::string const& db, int chunk);

    /// Remove chunk from the inventory if it's still registered
    /// also remove an entry from the database table
    void remove(std::string const& db, int chunk, mysql::MySqlConfig const& mySqlConfig);

    /// @return true if the specified db and chunk are in the inventory
    bool has(std::string const& db, int chunk) const;

    /// @return a unique identifier of a worker instance
    std::string const& id() const { return _id; }

    /// Rest the identifier of the worker service
    void resetId(std::string const& id) { _id = id; }

    /// Construct a ResourceUnit::Checker backed by this instance
    std::shared_ptr<ResourceUnit::Checker> newValidator();

    /// @return the name of the inventory
    std::string const& name() const { return _name; }

    /// @return a copy of the map in a thread-safe way
    ExistMap existMap() const;

    void dbgPrint(std::ostream& os) const;

    friend ChunkInventory::ExistMap operator-(ChunkInventory const& lhs, ChunkInventory const& rhs);

private:

    void _init(sql::SqlConnection& sc);
    void _rebuild(sql::SqlConnection& sc);

    ExistMap _existMap;
    std::string _name;

    /// a unique identifier of a worker
    std::string _id;

    /// The mutex is used to safeguard the methods in the multi-threaded
    /// environment
    mutable std::mutex _mtx;
};

/// @return databases and chunks known to 'lhs' and which are not in 'rhs
ChunkInventory::ExistMap operator-(ChunkInventory const& lhs, ChunkInventory const& rhs);


}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_CHUNKINVENTORY_H
