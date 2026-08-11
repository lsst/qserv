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
#ifndef LSST_QSERV_QMETA_USERTABLES_H
#define LSST_QSERV_QMETA_USERTABLES_H

// System headers
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>

// Qserv headers
#include "qmeta/UserTableIngestRequest.h"
#include "sql/SqlException.h"
#include "util/Issue.h"

// Forward declarations
namespace lsst::qserv::mysql {
class MySqlConfig;
}  // namespace lsst::qserv::mysql

namespace lsst::qserv::sql {
class SqlConnection;
}  // namespace lsst::qserv::sql

// This header declarations
namespace lsst::qserv::qmeta {

/**
 * Class IngestRequestNotFound is thrown when a user table ingest request is not found.
 */
class IngestRequestNotFound : public sql::SqlException {
public:
    IngestRequestNotFound(util::Issue::Context const& ctx, std::uint32_t id)
            : sql::SqlException(ctx, "No such user table ingest request with id: " + std::to_string(id)) {}
    IngestRequestNotFound(util::Issue::Context const& ctx, std::string const& cond)
            : sql::SqlException(ctx, "No such user table ingest request with condition: '" + cond + "'") {}
};

/**
 * Class UserTables is a registry of the user table ingest requests. The information on the requests
 * is stored in the 'UserTables' and 'UserTablesParams' tables of the QMeta database.
 */
class UserTables {
public:
    UserTables(mysql::MySqlConfig const& mysqlConf);
    UserTables() = delete;
    UserTables(UserTables const&) = delete;
    UserTables& operator=(UserTables const&) = delete;

    ~UserTables() = default;

    /**
     * Registers a new user table ingest request into the database.
     * @param request The request to be registered.
     * @return The updated request with the initial state set to IN_PROGRESS, an automatically
     *   allocated identifier, initialized timestamps, etc.
     * @throws qmeta::SqlError if a database error occurs.
     */
    UserTableIngestRequest registerRequest(UserTableIngestRequest const& request);

    /**
     * Finds a user table ingest request by its ID.
     * @param id The unique identifier of the request.
     * @param extended If true, fetch extended information.
     * @return The request if found; otherwise, an exception is thrown.
     * @throws qmeta::IngestRequestNotFound if the request is not found.
     */
    UserTableIngestRequest findRequest(std::uint32_t id, bool extended = true) const;

    /**
     * Finds the most recent user table ingest request for the given database and table name.
     * @param database The name of the database.
     * @param table The name of the table.
     * @param extended If true, fetch extended information.
     * @return The request if found; otherwise, an exception is thrown.
     * @throws qmeta::IngestRequestNotFound if the request is not found.
     */
    UserTableIngestRequest findRequest(std::string const& database, std::string const& table,
                                       bool extended = true) const;

    /**
     * Finds user table ingest requests by the given criteria.
     * @param filterByStatus (optional) If 'true' then the search will be filtered by the status of the
     *   request.
     * @param status (optional) The status of the request to be matched if 'filterByStatus' is 'true'.
     * @param filterByTableType (optional) If 'true' then the search will be filtered by the type of the
     *   table.
     * @param tableType (optional) The type of the table to be matched if 'filterByTableType' is 'true'.
     * @param deleteStatus (optional) The status of the table to be matched. A value of '-1' will match only
     *   non-deleted tables, '0' will match any tables, and '1' will match only deleted tables.
     * @param databaseSearchPattern (optional) A pattern to match the database name against.
     * @param databaseSearchRegexpMode (optional) If 'true' then the 'databaseSearchPattern' will be treated
     *   as a regular expression; otherwise, it will be treated as a simple wildcard pattern.
     * @param minNumChunks (optional) The minimum number of chunks ingested. A value of '0' disables
     *   filtering by the minimum number of chunks.
     * @param minNumBytes (optional) The minimum number of bytes ingested. A value of '0' disables
     *   filtering by the minimum number of bytes.
     * @param minNumRows (optional) The minimum number of rows ingested. A value of '0' disables filtering
     *   by the minimum number of rows.
     * @param limit (optional) The maximum number of requests to be returned. A value of '0' disables
     *   limiting the number of requests.
     * @param extended If true, fetch extended information.
     * @return A collection of requests which met the criteria. The collection may be empty
     *   if no requests were found. The requests are sorted by the 'begin_time' field
     *   in descending order (the most recent requests are listed first).
     * @throws qmeta::SqlError if a database error occurs.
     */
    std::list<UserTableIngestRequest> findRequests(
            bool filterByStatus = false,
            UserTableIngestRequest::Status status = UserTableIngestRequest::Status::IN_PROGRESS,
            bool filterByTableType = false,
            UserTableIngestRequest::TableType tableType = UserTableIngestRequest::TableType::FULLY_REPLICATED,
            int deleteStatus = 0, std::string const& databaseSearchPattern = std::string(),
            bool databaseSearchRegexpMode = false, unsigned int minNumChunks = 0,
            unsigned int minNumBytes = 0, unsigned int minNumRows = 0, std::uint64_t limit = 0,
            bool extended = true) const;

    /**
     * Marks a user table ingest request as finished.
     * @note The persistent state (status, completion timestamp, counters and error code)
     *   of the request will be updated.
     * @param id The unique identifier of the request. The request must exist and it be in the IN_PROGRESS
     *   state.
     * @param status The new status of the ingest request. It must be either COMPLETED, FAILED or FAILED_LR.
     * @param error (optional) An error message, if any.
     * @param transactionId (optional) The transaction ID (Replication/Ingest system).
     * @param numChunks (optional) The number of chunks ingested.
     * @param numRows (optional) The number of rows ingested.
     * @param numBytes (optional) The number of bytes ingested.
     * @throws qmeta::IngestRequestNotFound if the request is not found.
     * @throws qmeta::SqlError if a database error occurs.
     */
    UserTableIngestRequest ingestFinished(std::uint32_t id, UserTableIngestRequest::Status status,
                                          std::string const& error = "", std::uint32_t transactionId = 0,
                                          std::uint32_t numChunks = 0, std::uint64_t numRows = 0,
                                          std::uint64_t numBytes = 0);

    /**
     * Marks a user database and all tables in it as deleted.
     * @param database (optional) The name of the database.
     * @throws qmeta::SqlError if a database error occurs.
     */
    void databaseDeleted(std::string const& database);

    /**
     * Marks a user table as deleted.
     * @param id The unique identifier of the table ingest request.
     * @throws qmeta::IngestRequestNotFound if the request is not found.
     * @throws qmeta::SqlError if a database error occurs.
     */
    UserTableIngestRequest tableDeleted(std::uint32_t id);

private:
    /**
     * Helper method to find a single request by an arbitrary condition.
     * @note A lock on the database mutex must be held to ensure thread safety.
     * @note A transaction must be active to ensure consistency of the read operation.
     * @param cond The SQL condition (the WHERE clause without the 'WHERE' keyword).
     * @param extended If true, fetch extended information.
     * @return The request if found; otherwise, an exception is thrown.
     * @throws qmeta::IngestRequestNotFound if the request is not found.
     * @throws qmeta::SqlError if a database error occurs.
     */
    UserTableIngestRequest _findOneRequestBy(std::string const& cond, bool extended) const;

    std::shared_ptr<sql::SqlConnection> const _conn;
    mutable std::mutex _dbMutex;  ///< Synchronizes access to certain DB operations
};

}  // namespace lsst::qserv::qmeta

#endif  // LSST_QSERV_QMETA_USERTABLES_H