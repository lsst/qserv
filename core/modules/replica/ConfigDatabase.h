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
#include <memory>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class DatabaseFamilyInfo;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class DatabaseInfo encapsulates various parameters describing databases.
 */
class DatabaseInfo {
public:
    std::string name;    // The name of a database.
    std::string family;  // The name of the database family.

    bool isPublished = false;   // The status of the database.

    std::vector<std::string> partitionedTables; // The names of the partitioned tables.
    std::vector<std::string> regularTables;     // The list of fully replicated tables.

    /// Table schema (optional).
    std::map<std::string,                       // table name
             std::list<SqlColDef>> columns;

    /// @return The names of all tables.
    std::vector<std::string> tables() const;

    std::string directorTable;          // The name of the Qserv "director" table if any.

    std::map<std::string,               // The table name (partitioned tables only!).
        std::string> directorTableKey;  // The name of the table's key representing object identifiers.
                                        // NOTES: (1) In the "dependent" tables the key represents the FK
                                        // associated with the corresponding PK of the "director" table.
                                        // (2) The key is allowed to be empty for the partitioned tables
                                        // that don't have any objectId-based association with
                                        // any "director" table.

    // Names of special columns of the partitioned tables.

    std::map<std::string,                   // table name
             std::string> latitudeColName;  // latitude (declination) column name

    std::map<std::string,                   // table name
             std::string> longitudeColName; // longitude (right ascension) column name

    /**
     * Construct from JSON.
     * @note Passing an empty JSON object or json::null object as a value of the optional
     *   parameter 'families' will disable the optional step of the family validation.
     *   This is safe to do once if the object is pulled from the transient state
     *   of the configuration which is guaranteed to be complete. In other cases, where
     *   the input provided by a client the input needs to be sanitized.
     * @param obj The optional object to be used of a source of the worker's state.
     * @param families The optional collection of the database families to be used
     *   for validating database definition when parsing from JSON=.
     * @throw std::invalid_argument If the input object can't be parsed, or if it has
     *   incorrect schema.
     */
    explicit DatabaseInfo(nlohmann::json const& obj=nlohmann::json::object(),
                          std::map<std::string, DatabaseFamilyInfo> const& families =
                                std::map<std::string, DatabaseFamilyInfo>());

    /// @return The JSON representation of the object.
    nlohmann::json toJson() const;

    /// @param table The name of a table.
    /// @return 'true' if the table (of either kind) exists.
    bool hasTable(std::string const& table) const;

    /// Validate parameters of a new table, then add it to the database.
    /// @throw std::invalid_argument If the input parameters are incorrect or inconsistent.
    void addTable(std::string const& table,
                  std::list<SqlColDef> const& columns_=std::list<SqlColDef>(),
                  bool isPartitioned=false,
                  bool isDirectorTable=false,
                  std::string const& directorTableKey_=std::string(),
                  std::string const& latitudeColName_=std::string(),
                  std::string const& longitudeColName_=std::string());

    /// Remove the specified table from the database
    /// @throw std::invalid_argument If the empty string is passed as a value of
    ///   the parameter 'table', or the table doesn't exist.
    void removeTable(std::string const& table);

    /// @param The name of a table to be located and inspected
    /// @return 'true' if the table was found and it's 'partitioned'
    /// @throw std::invalid_argument if no such table is known
    bool isPartitioned(std::string const& table) const;

    /// @param The name of a table to be located and inspected
    /// @return 'true' if the table was found and it's the 'partitioned' and the 'director' table
    /// @throw std::invalid_argument if no such table is known
    bool isDirector(std::string const& table) const;

    /// @return The table schema in format which is suitable for CSS.
    /// @throws std::out_of_range If the table is unknown.
    std::string schema4css(std::string const& table) const;
};

std::ostream& operator <<(std::ostream& os, DatabaseInfo const& info);

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGDATABASE_H
