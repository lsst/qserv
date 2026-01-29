/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_CONFIGDATABASE_H
#define LSST_QSERV_REPLICA_CONFIGDATABASE_H

// System headers
#include <cstdint>
#include <list>
#include <iosfwd>
#include <map>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/config/ConfigTable.h"
#include "replica/util/Common.h"

// Forward declarations
namespace lsst::qserv::replica {
class DatabaseFamilyInfo;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class DatabaseInfo encapsulates various parameters describing databases.
 * @note The interface is not thread-safe.
 */
class DatabaseInfo {
public:
    std::string name;    ///< The name of a database.
    std::string family;  ///< The name of the database family.

    bool isPublished = false;
    uint64_t createTime = 0;
    uint64_t publishTime = 0;

    /**
     * Construct an empty unpublished database object for the given name and the family.
     * @note The create time of the database will be set to the current time.
     * @param name The name of the database.
     * @param family The name of the database family.
     * @return The initialized database descriptor.
     */
    static DatabaseInfo create(std::string const& name, std::string const family);

    /**
     * Construct from JSON.
     * @note Passing an empty JSON object or json::null object as a value of the optional
     *   parameter 'families' will disable the optional step of the family validation.
     *   This is safe to do once if the object is pulled from the transient state
     *   of the configuration which is guaranteed to be complete. In other cases, where
     *   the input provided by a client the input needs to be sanitized.
     * @param obj The JSON object to be used of a source of the worker's state.
     * @param families The collection of the database families to be used for validating
     *   the database definition.
     * @param databases The collection of databases is needed for validating
     *   the referential integrity of the "RefMatch" tables that may refer
     *   to the director tables of other databases.
     * @return The initialized database descriptor.
     * @throw std::invalid_argument If the input object can't be parsed, or if it has
     *   incorrect schema.
     */
    static DatabaseInfo parse(nlohmann::json const& obj,
                              std::map<std::string, DatabaseFamilyInfo> const& families,
                              std::map<std::string, DatabaseInfo> const& databases);

    /// @return The JSON representation of the object.
    nlohmann::json toJson() const;

    /// @return The names of all tables.
    std::vector<std::string> tables() const;

    /// @return The names of the "regular" (fully-replicated) tables.
    std::vector<std::string> regularTables() const;

    /// @return The names of the "partitioned" tables.
    std::vector<std::string> partitionedTables() const;

    /// @return The names of the "director" tables.
    std::vector<std::string> directorTables() const;

    /// @return The names of the "RefMatch" tables.
    std::vector<std::string> refMatchTables() const;

    /// @param tableName The name of a table.
    /// @return 'true' if the table (of either kind) exists.
    bool tableExists(std::string const& tableName) const;

    /**
     * @brief Locate the immutable table descriptor.
     * @param tableName The name of the table.
     * @return The table descriptor
     * @throws std::invalid_argument If the name is empty.
     * @throws ConfigUnknownTable If no table for the specified name was found.
     */
    TableInfo const& findTable(std::string const& tableName) const;

    /**
     * @brief Locate the mutable table descriptor.
     * @param tableName The name of the table.
     * @return The table descriptor
     * @throws std::invalid_argument If the name is empty.
     * @throws ConfigUnknownTable If no table for the specified name was found.
     */
    TableInfo& findTable(std::string const& tableName) {
        return const_cast<TableInfo&>(const_cast<DatabaseInfo const*>(this)->findTable(tableName));
    }

    /**
     * @brief Validate parameters of a new table, then register the table in the database.
     * @param databases The collection of databases is needed for validating
     *   the referential integrity of the "RefMatch" tables that may refer
     *   to the director tables of other databases.
     * @param table The table descriptor.
     * @return The table descriptor. It would be the original or validated and/or sanitized
     *   one depending on a value of the optional flags \param validate_ and \param sanitize_.
     * @throw std::invalid_argument if the input parameters are incorrect,
     *   or if they're inconsistent, or if the table already present.
     */
    TableInfo addTable(std::map<std::string, DatabaseInfo> const& databases, TableInfo const& table_,
                       bool validate_ = true, bool sanitize_ = false);

    /**
     * @brief Validate parameters of a new table.
     * @param databases The collection of databases is needed for validating
     *   the referential integrity of the "RefMatch" tables that may refer
     *   to the director tables of other databases.
     * @param table_ The table descriptor.
     * @param sanitize_ If the flag is set to "true" then the method will try to fix
     *   incomplete or incorrect values of the attributes where it's possible rather
     *   then throwing the exception. the exception will be still throws should
     *   such sanitation attempt failed.
     * @return The table descriptor. It would be the original or sanitized one
     *   depending on a value of the optional flag \param sanitize_.
     * @throw std::invalid_argument if the input parameters are incorrect,
     *   or if they're inconsistent, or if the table already present.
     */
    TableInfo validate(std::map<std::string, DatabaseInfo> const& databases, TableInfo const& table_,
                       bool sanitize_ = false) const;

    /**
     * @brief Correct attributes of the table descriptor where this is possible.
     * @param table_ The input table descriptor.
     * @return The sanitized descriptor.
     */
    TableInfo sanitize(TableInfo const& table_) const;

    /// Remove the specified table from the database
    /// @param tableName The name of the table to be removed.
    /// @throw std::invalid_argument If the empty string is passed as a value of
    ///   the parameter 'table', or the table doesn't exist.
    void removeTable(std::string const& tableName);

private:
    std::map<std::string, TableInfo> _tables;  ///< The collection of all tables.
};

std::ostream& operator<<(std::ostream& os, DatabaseInfo const& info);

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGDATABASE_H
