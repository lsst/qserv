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
#ifndef LSST_QSERV_REPLICA_SQLRESULTSET_H
#define LSST_QSERV_REPLICA_SQLRESULTSET_H

// System headers
#include <list>
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/protocol.pb.h"
#include "util/TablePrinter.h"

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Structure SqlResultSet represents a result set received from a remote worker
 * service upon a successful completion of a query against the worker database.
 */
struct SqlResultSet {

    /// Optional error code received from a server
    std::string error;

    /// Of the connection
    std::string charSetName;

    /// The request produced a result set
    bool hasResult;

    /// Structure Field stores a content captured from MYSQL_FIELD
    struct Field {
        std::string name;       /// The name of the column
        std::string orgName;    /// The original name of the column
        std::string table;      /// The name of the table
        std::string orgTable;   /// The original name of the table
        std::string db;         /// The name of the database (schema)
        std::string catalog;    /// The catalog name (always 'def')
        std::string def;        /// default value
        uint32_t    length;     /// The length (width) of the column definition
        uint32_t    maxLength;  /// The maximum length of the column value
        uint32_t    flags;      /// Flags
        uint32_t    decimals;   /// Number of decimals
        int32_t     type;       /// Field type (see MySQL headers for enum enum_field_types)

        /// The default c-tor is required at a presence of the explicit one
        Field() = default;

        /// @return string representatio of the type
        std::string type2string() const;

        /**
         * Construct the object by carrying over the content of the input protocol
         * message into the corresponding data members of the structure.
         * 
         * @param field
         *   input message to be parsed
         */
        explicit Field(ProtocolResponseSqlField const& field);
    };

    /// A vector with field definitions from a result set,
    /// where the number of objects in the array represents
    /// the "width" of the result set
    std::vector<Field> fields;

    /// The row type of a result set. The number of elements in each row
    /// must match the number of fields.
    struct Row {

        /// Values at the cells
        std::vector<std::string> cells;

        /// Flags indicating if the corresponding values of the cells
        /// represent SQL NULL. Zero value represents 'false'.
        std::vector<uint8_t> nulls;

        /// The default c-tor is required at a presence of the explicit one
        Row() = default;

        /**
         * Construct the object by carrying over the content of the input protocol
         * message into the corresponding data members of the structure.
         * 
         * @param row
         *   input message to be parsed
         */
        explicit Row(ProtocolResponseSqlRow const& row);
    };

    /// An array of rows from a result set,
    /// where the number of elements in the array represents
    /// the number of rows.
    std::list<Row> rows;

    /// The duration of a request (in seconds) since it was created
    /// by the Controller and before its completion was recorded by
    /// the Controller.
    /// @see SqlRequst::performance()
    double performanceSec;

    /**
     * Carry over the content of the input protocol message into
     * the corresponding data members of the structure.
     * 
     * @param message
     *   input message to be parsed
     */
    void set(ProtocolResponseSql const& message);

    /**
     * Translate the structure into JSON
     *
     * @return
     *   JSON array
     */
    nlohmann::json toJson() const;

    /**
     * Package results into a table. For  description of the input parameters:
     * @see class util::ColumnTablePrinter
     * 
     * @throws std::logic_error
     *    if attempting to use the method when member SqlResultSet::hasResult is
     *    set to 'false'.
     */
    util::ColumnTablePrinter toColumnTable(std::string const& caption=std::string(),
                                           std::string const& indent=std::string(),
                                           bool verticalSeparator=true) const;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLRESULTSET_H
