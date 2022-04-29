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
#include <cstdint>
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
namespace lsst { namespace qserv { namespace replica {

/**
 * Class SqlResultSet represents a result set received from a remote worker
 * service upon a successful completion of a query against the worker database.
 */
class SqlResultSet {
public:
    class ResultSet {
    public:
        /// A status code for a particular query/operation.
        /// The default value is always set to construct the stats object
        /// to indicate a "failed" attempt to process a query. If will be
        /// explicitly set by worker services to other values for queries with
        /// a different outcome (wether they succeeded or failed due to a specific problem).
        /// The successful completion of a query is indicated by status set
        /// to extendedCompletionStatus::EXT_STATUS_NONE.
        ProtocolStatusExt extendedStatus = ProtocolStatusExt::MYSQL_ERROR;

        std::string error;        /// is set if a error code received from a server
        std::string charSetName;  /// of the connection
        bool hasResult = false;   /// 'true' if the request produced a result set

        /// Structure Field stores a content captured from MYSQL_FIELD
        struct Field {
            std::string name;        /// The name of the column
            std::string orgName;     /// The original name of the column
            std::string table;       /// The name of the table
            std::string orgTable;    /// The original name of the table
            std::string db;          /// The name of the database (schema)
            std::string catalog;     /// The catalog name (always 'def')
            std::string def;         /// default value
            uint32_t length = 0;     /// The length (width) of the column definition
            uint32_t maxLength = 0;  /// The maximum length of the column value
            uint32_t flags = 0;      /// Flags
            uint32_t decimals = 0;   /// Number of decimals
            int32_t type = 0;        /// Field type (see MySQL headers for enum enum_field_types)

            /// The default c-tor is required at a presence of the explicit one
            Field() = default;

            /// @return string representation of the type
            std::string type2string() const;

            /**
             * Construct the object by carrying over the content of the input protocol
             * message into the corresponding data members of the structure.
             *
             * @param field  a filed from an input result set to be parsed
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
             * @param row a row from an input result set to be parsed
             */
            explicit Row(ProtocolResponseSqlRow const& row);
        };

        /// An array of rows from a result set,
        /// where the number of elements in the array represents
        /// the number of rows.
        std::list<Row> rows;

        /**
         * Construct the object by carrying over the content of the input protocol
         * message into the corresponding data members of the structure.
         *
         * @param result  a result set from an input message to be parsed
         */
        explicit ResultSet(ProtocolResponseSqlResultSet const& result);

        /// @return  JSON representation of the object
        nlohmann::json toJson() const;

        /**
         * Package results into a table. For description of other input parameters
         * @see class util::ColumnTablePrinter.
         *
         * @throws std::logic_error
         *    if attempting to use the method when member SqlResultSet::hasResult is
         *    set to 'false'.
         */
        util::ColumnTablePrinter toColumnTable(std::string const& caption = std::string(),
                                               std::string const& indent = std::string(),
                                               bool verticalSeparator = true) const;
    };

    /// A collection of result sets for queries or other operations over
    /// the content of the worker databases. The key to the dictionary defines
    /// a scope of an operation over a worker database, and its meaning varies
    /// depending on a request. It could be the name of a table, the name of a database,
    /// or a query.
    std::map<std::string, ResultSet> queryResultSet;

    /// The duration of a request (in seconds) since it was created
    /// by the Controller and before its completion was recorded by
    /// the Controller.
    /// @see SqlRequst::performance()
    double performanceSec = 0.;

    /**
     * Carry over the content of the input protocol message into
     * the corresponding data members of the structure.
     *
     * @param message
     *   input message to be parsed
     */
    void set(ProtocolResponseSql const& message);

    /// @return  JSON representation of the object
    nlohmann::json toJson() const;

    /// @return 'true' if any errors are found in the result sets
    /// based on a value of the extended status of each result set.
    bool hasErrors() const;

    /// @return 'true' if all errors are of the specified 'status'
    /// @note this method should be used together with method SqlResultSet::hasErrors().
    /// @note ProtocolStatusExt::NONE is not treated as an error.
    bool allErrorsOf(ProtocolStatusExt status) const;

    /**
     * Look for errors in the result sets based on a value of the extended
     * status and MySQL error strings reported by the servers. And return
     * the first error found in there (if any).
     *
     * @return a string formatted as <scope>:<extendedStatus>:<MySQL error>
     * if an error was found, or the empty string otherwise
     */
    std::string firstError() const;

    /// @return a collection of all error strings formatted in the same way as
    /// results returned by method SqlResultSet::firstError()
    /// @see SqlResultSet::firstError()
    std::vector<std::string> allErrors() const;
};

std::ostream& operator<<(std::ostream& os, SqlResultSet const& info);

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SQLRESULTSET_H
