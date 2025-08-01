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
#ifndef LSST_QSERV_QMETA_QPROGRESSHISTORY_H
#define LSST_QSERV_QMETA_QPROGRESSHISTORY_H

// System headers
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// Qserv headers
#include "global/intTypes.h"
#include "mysql/MySqlConfig.h"

// Third party headers
#include <nlohmann/json.hpp>

// Forward declarations

namespace lsst::qserv::sql {
class SqlConnection;
}  // namespace lsst::qserv::sql

// This header declarations
namespace lsst::qserv::qmeta {

/**
 * Class QProgressHistory manages the query progress history in memory and in
 * the metadata database.
 *
 * The history of the ongoing queries is stored in memory and updated
 * periodically. The history of the completed queries is archived in
 * the table QProgressHistory.
 *
 * @note The class has a singleton-like behavior. The instance of the class
 * is created on the first call to the static method create() and then
 * the same instance is returned on subsequent calls to the same method or to
 * the static method get(). It's safe to call the method create() multiple times
 * from different threads, but the method get() should be called only after
 * the instance is created.
 *
 * @note The class has the thread-safe implementation.
 *
 * @note The query history data both in the corresponding data member and the public API
 * of the class have been deliberately designed around the JSON format.
 * This is done to simplify using this class by the HTTP API of the Czar
 * and for serializing/deserializing the data in the database.
 * In the JSON schemas used by the class, <queryId> is the unique identifier of a query,
 * <timeMs> is the time in milliseconds (64-bit unsigned integer type) when the point was
 * recorded, and <numChunks> is the number (32-bit signed integer type) of unfinished chunks
 * at that time.
 * Note that the query identifier is always turned into the string representation in
 * the JSON schemas since the JSON format doesn't support integers types for keys in
 * the associative collections.
 */
class QProgressHistory {
public:
    /**
     * Factory method to create a QProgressHistory instance.
     * @param connConfig The MySQL configuration for the metadata database.
     * @return The shared pointer to the QProgressHistory instance. The method will return
     *  a shared pointer to the instance regardless if it was created during the call or
     *  it was done before.
     */
    static std::shared_ptr<QProgressHistory> create(mysql::MySqlConfig const& connConfig);

    /**
     * Get the singleton instance of the QProgressHistory class.
     * @return The shared pointer to the QProgressHistory instance or nullptr if not created.
     */
    static std::shared_ptr<QProgressHistory> get();

    QProgressHistory() = delete;
    QProgressHistory(QProgressHistory const&) = delete;
    QProgressHistory& operator=(QProgressHistory const&) = delete;

    ~QProgressHistory() = default;

    /**
     * Begin tracking the specified query.
     * @param queryId The unique identifier of a query affected by the operation.
     */
    void track(QueryId queryId);

    /**
     * Update the query counter(s).
     * @note The method will only record changes in the counter of chunks if
     *  the provided number differs from the previously recorded value.
     * @param queryId The unique identifier of a query affected by the operation.
     * @param numUnfinishedChunks The number of unfinished chunks.
     * @throw qmeta::ConsistencyError if the query is not being tracked.
     */
    void update(QueryId queryId, int numUnfinishedChunks);

    /**
     * Finish tracking the specified query and archive the history in the database.
     * @param queryId The unique identifier of a query affected by the operation.
     * @throw qmeta::ConsistencyError if the query is not being tracked.
     * @throw qmeta::SqlError if the query for archiving the query history fails to execute.
     */
    void untrack(QueryId queryId);

    /**
     * Get info on a progress of a query.
     * @param queryId The unique identifier of a query.
     * @return The JSON object encapsulating the progress and the status of the query.
     *  Return the empty object if the query progress history is not available.
     * The schema of the result object is as follows:
     * @code
     *   { "queryId": <number>,
     *     "status":  <string>,
     *     "history": [
     *       [ <timeMs>, <numChunks> ],
     *       ...
     *     ]
     *   }
     * @endcode
     * @throw qmeta::SqlError if the query for fetching the query history fails to execute.
     * @throw qmeta::ConsistencyError if the history info read from the database is not valid.
     */
    nlohmann::json findOne(QueryId queryId) const;

    /**
     * Get info on a progress of the select queries.
     * @param lastSeconds The cut-off time for the age of entries to be reported.
     *  A value of the parameter is used to determined the time interval [ now - lastSeconds, now ],
     *  where "now" is the current time.
     * @param queryStatus The optional status ("EXECUTING", "COMPLETED", "FAILED", etc. or
     *  the empty string for all) of the queries to be selected.
     * @return The JSON array encapsulating the progress and the status of the queries.
     * The schema of the result object is as follows:
     * @code
     *   [ { "queryId": <number>,
     *       "status":  <string>,
     *       "history": [
     *         [ <timeMs>, <numChunks> ],
     *         ...
     *       ]
     *     },
     *     ...
     *   ]
     * @endcode
     * @throw util::Issue if the cut-off time is 0 and includeFinished is true.
     * @throw qmeta::SqlError if the query for fetching the query history fails to execute.
     * @throw qmeta::ConsistencyError if the history info read from the database is not valid.
     */
    nlohmann::json findMany(unsigned int lastSeconds, std::string const& queryStatus = std::string()) const;

private:
    QProgressHistory(mysql::MySqlConfig const& connConfig);

    /// Remove the history of a query from the in-memory collection.
    /// @param queryId The unique identifier of a query.
    /// @return The JSON array with the history of the query that was removed.
    /// @throw qmeta::ConsistencyError if the query is not being tracked.
    nlohmann::json _removeFromMemory(QueryId queryId);

    /// Read the history of a query from the in-memory collection.
    /// @param queryId The unique identifier of a query.
    /// @return The JSON object with the history of the query. The object will be empty
    ///  if the query is not being tracked.
    nlohmann::json _readFromMemory(QueryId queryId) const;

    /// Get the history of all queries in the in-memory collection.
    /// @param resultJson The JSON array to store the result.
    /// @param minTimeMs The minimum time in milliseconds for the history points to be included
    ///  in the result.
    void _readFromMemory(nlohmann::json& resultJson, uint64_t minTimeMs) const;

    /// Read the history of a query from the database.
    /// @param queryId The unique identifier of a query.
    /// @return The JSON object with the history of the query. The object will be empty
    ///  if the query is not found in the database.
    /// @throw qmeta::SqlError if the query fails to execute.
    nlohmann::json _readFromDatabase(QueryId queryId) const;

    /// Read the history of the select queries from the database.
    /// @param resultJson The JSON array to store the result.
    /// @param minTimeMs The minimum time in milliseconds for the history points to be included
    ///  in the result.
    /// @param statusRestrictor The SQL condition to restrict the status of the queries.
    ///  The condition should be in the form of a SQL expression, e.g. "`qi`.`status` NOT IN ('EXECUTING')".
    ///  The condition will be appended to the SQL query.
    ///  The condition should not be empty.
    /// @throw qmeta::SqlError if the query fails to execute.
    void _readFromDatabase(nlohmann::json& resultJson, uint64_t minTimeMs,
                           std::string const& statusRestrictor) const;

    /// Archive the history of a query in the database
    /// @param queryId The unique identifier of a query.
    /// @param history The JSON array with the history of the query to be archived.
    /// @throw qmeta::SqlError if the query fails to execute.
    void _writeToDatabase(QueryId queryId, nlohmann::json const& history);

    static std::shared_ptr<QProgressHistory> _instance;  ///< The singleton instance of the class.
    static std::mutex _instanceMutex;                    ///< Mutex for the singleton instance.

    std::uint64_t const _createdTimeMs;               ///< The time when the instance was created.
    std::shared_ptr<sql::SqlConnection> const _conn;  ///< Database connection.

    mutable std::mutex _mtx;      ///< For synchronized access to operations with ongoing queries.
    mutable std::mutex _connMtx;  ///< For synchronized access to the database connection.

    /**
     * The progress history of the ongoing queries (queries that are in the EXECUTING state
     * in the Qserv's metadata table QInfo). The schema of the collection is as follows:
     * @code
     *   { "<queryId>": [
     *     [ <timeMs>, <numChunks> ],
     *       ...
     *     ]
     *   }
     * @endcode
     */
    nlohmann::json _executing = nlohmann::json::object();
};

}  // namespace lsst::qserv::qmeta

#endif  // LSST_QSERV_QMETA_QPROGRESSHISTORY_H
