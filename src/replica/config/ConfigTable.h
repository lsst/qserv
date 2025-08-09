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
#ifndef LSST_QSERV_REPLICA_CONFIGTABLE_H
#define LSST_QSERV_REPLICA_CONFIGTABLE_H

// System headers
#include <cstdint>
#include <list>
#include <iosfwd>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/util/Common.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * @brief Class DirectorTableRef encapculate references to the director
 * tables from the child or the RefMatch tables. Objects of this class could be also
 * used for extending the director table definition in TableInfo.
 */
class DirectorTableRef {
public:
    /**
     * @brief Construct a new Director Table Ref object
     *
     * @param databaseTableName The optional name of the that may also include the name of
     *   a database. Of both are present then the names should be separated by '.'.
     * @param primaryKeyColumn_ The mandatory name of the column.
     * @throw std::invalid_argument If the name of a column is empty, or if the combined
     *   database and table name doesn't adhere to the above explained syntax.
     */
    explicit DirectorTableRef(std::string const& databaseTableName_, std::string const& primaryKeyColumn_);

    DirectorTableRef() = default;
    DirectorTableRef(DirectorTableRef const&) = default;
    DirectorTableRef& operator=(DirectorTableRef const&) = default;

    /// @return The name of a database if it was set.
    std::string const& databaseName() const { return _databaseName; }

    /// @return The name of a table if it was set.
    std::string const& tableName() const { return _tableName; }

    /// @return The name of the director table's column uniquely identifying
    ///   rows of the tables.
    std::string const& primaryKeyColumn() const { return _primaryKeyColumn; }

    /**
     * @return The combined name that includes the name of a database and the name
     *   of a table using. The database name (if present) will be separated from
     *   the table using the '.' symbol (the usual SQL notation for referencing
     *   such identifiers).
     * @note If each name is required to be quoted then it's up to the user code
     *   to do so by fetching the corresponding attributes and constructing such
     *   identifier.
     */
    std::string databaseTableName() const;

    /// @return 'true' if the object was initialized using the default constructor
    bool empty() const;

    /// @return The JSON representation of the object.
    nlohmann::json toJson() const;

private:
    /// The name can only be set if this this table is the dependency of
    /// the RefMatch table. For ordinary child tables no database name should be
    /// set. And if it will be set (for the child table's dependencies) then the name
    /// will be ignored.
    std::string _databaseName;

    /// The name of a director table. The name must not be empty if this is the depedency
    /// of the child or the RefMatch table. Otherwise (if this is an extension of the director
    /// table) then th ename must me empty. And if it's set in the later case then name will
    /// be ignored.
    std::string _tableName;

    /// The name of the director table's column uniquely identifying rows of the tables.
    std::string _primaryKeyColumn;
};

bool operator==(DirectorTableRef const& lhs, DirectorTableRef const& rhs);

std::ostream& operator<<(std::ostream& os, DirectorTableRef const& table);

/**
 * Class TableInfo encapsulates various parameters describing tables.
 * @note The interface is not thread-safe.
 */
class TableInfo {
public:
    std::string name;      ///< The name of the table.
    std::string database;  ///< The name of the parent database.

    // The publishing status and major events in the table's history.

    bool isPublished = false;
    uint64_t createTime = 0;
    uint64_t publishTime = 0;

    // The type of the table is determined by these attributes.

    bool isPartitioned = false;
    bool isDirector() const { return isPartitioned && directorTable.tableName().empty(); }
    bool isRefMatch() const {
        return isPartitioned && !directorTable.empty() && !directorTable2.tableName().empty();
    }

    /**
     * @brief The "director" table (if any).
     *
     * The dependency is required for all partitioned tables including the "directors".
     *
     * For the "director" table the field stores the name of the corresponding "director"
     * key (the primary key) column. Other attributes of the object would be ignored.
     *
     * For the "RefMatch" table the value points to the first matched "director".
     * Unlike other tables, the matched director table is also allowed to include
     * the name of a database where the referenced director is residing (in case if
     * this is not the same database as the one where the "RefMatch" itself is residing.
     */
    DirectorTableRef directorTable;

    /**
     * @brief The second matched director table for the "RefMatch" tables.
     * For other tables this parameter should be ignored.
     */
    DirectorTableRef directorTable2;

    /// The name of a column that's used to store flags  (RefMatch tables only)
    std::string flagColName;

    /// The angular separation parameter (RefMatch tables only)
    double angSep = 0.0;

    /**
     * The optional flag that is related to the "director" tables only. The flag
     * affects the construction of the "director" indexes of such tables.
     * Setting a value of this parameter to "false" would drop the "UNIQUE"
     * constraint from a definition of the corresponiding key in the "director"
     * table schema when the index is constructed at the transaction commit time
     * during catalog ingest.
     * @note This behavior can be explicitly overriden in any direction (if needed) when
     *   the index is build as a post-ingest data management operation.
     * @see TableInfo::directorTable
     * @see DirectorTableRef::primaryKeyColumn()
     */
    bool uniquePrimaryKey = true;

    // The names of the character set and the collation for the table (optional).
    // Server defaults will be used for the empty names.

    std::string charsetName;
    std::string collationName;

    // Names of special columns of the partitioned tables.
    // The non-empty values are required for the "director" tables only.
    // The "dependent" tables may have non empty values here.
    // Empty values are guaranteed for the "regular" (fully-replicated) and
    // the "RefMatch" tables.

    std::string latitudeColName;
    std::string longitudeColName;

    std::list<SqlColDef> columns;  ///< Table schema (optional).

    /**
     * @brief Extract the optional name of a database from the table
     *   specification string.
     * @param str The table specification that may be optionally
     *   prepended by the name of a database.
     * @return The name of a database or the empty string.
     */
    static std::string databaseName(std::string const& str);

    /**
     * @brief Extract the name of the table from the specification string.
     * @param str The table specification that may be optionally
     *   prepended by the name of a database.
     * @return The name of a table or the empty string.
     */
    static std::string tableName(std::string const& str);

    /// @return A collection of the column names in the same order they're defined
    ///   in the collection of columns.
    std::vector<std::string> columnNames() const;

    /// @param table The name of the director table to be evaluated.
    /// @return 'true' if the specified table is one of this table's directors.
    /// @throw std::invalid_argument If the name of the director table is empty
    bool isDependantOf(std::string const& table) const;

    /// @return The table schema in format that is suitable for CSS.
    std::string schema4css() const;

    /// @return The JSON representation of the object.
    nlohmann::json toJson() const;
};

/// Check if the tables are equal.
/// @note The operation excludes the publishing state of the table, as well as
///   the table creation/publishing timestamps.
/// @return 'true' if the tables are equal in the above stated sense.
bool operator==(TableInfo const& lhs, TableInfo const& rhs);

/// Check if the tables are not equal.
inline bool operator!=(TableInfo const& lhs, TableInfo const& rhs) { return !operator==(lhs, rhs); }

/// Serialize the table descriptor into the output stream.
std::ostream& operator<<(std::ostream& os, TableInfo const& info);

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_CONFIGTABLE_H
