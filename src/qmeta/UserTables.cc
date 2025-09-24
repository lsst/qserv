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
// Class header
#include "qmeta/UserTables.h"

// System headers
#include <stdexcept>

// Third-party headers
#include "boost/lexical_cast.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "qmeta/Exceptions.h"
#include "qmeta/QMetaTransaction.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlErrorObject.h"
#include "sql/SqlResults.h"
#include "util/TimeUtils.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qmeta.UserTables");
}

namespace lsst::qserv::qmeta {

UserTables::UserTables(mysql::MySqlConfig const& mysqlConf)
        : _conn(sql::SqlConnectionFactory::make(mysqlConf)) {}

UserTableIngestRequest UserTables::registerRequest(UserTableIngestRequest const& request) {
    std::lock_guard<std::mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);

    std::list<std::string> queries;
    queries.push_back(
            "INSERT INTO `UserTables` "
            "(`begin_time`,`database`,`table`,`table_type`,`is_temporary`,`data_format`) "
            "VALUES (" +
            std::to_string(util::TimeUtils::now()) + ",'" + _conn->escapeString(request.database) + "','" +
            _conn->escapeString(request.table) + "','" +
            UserTableIngestRequest::tableType2str(request.tableType) + "'," +
            std::to_string(request.isTemporary) + ",'" +
            UserTableIngestRequest::dataFormat2str(request.dataFormat) + "')");
    auto makeQuery = [this](std::string const& key, std::string const& val) {
        return "INSERT INTO `UserTablesParams` (`id`,`key`,`val`) "
               "VALUES (LAST_INSERT_ID(), '" +
               _conn->escapeString(key) + "', '" + _conn->escapeString(val) + "')";
    };
    queries.push_back(makeQuery("schema", request.schema.dump()));
    queries.push_back(makeQuery("indexes", request.indexes.dump()));
    queries.push_back(makeQuery("extended", request.extended.dump()));
    for (auto const& query : queries) {
        sql::SqlErrorObject errObj;
        if (!_conn->runQuery(query, errObj)) {
            LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
            throw qmeta::SqlError(ERR_LOC, errObj);
        }
    }
    std::string const cond = "`id`=LAST_INSERT_ID()";
    bool const extended = true;
    UserTableIngestRequest const updatedRequest = _findOneRequestBy(cond, extended);
    trans->commit();
    return updatedRequest;
}

UserTableIngestRequest UserTables::findRequest(std::uint32_t id, bool extended) const {
    std::lock_guard<std::mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    std::string const cond = "`id`=" + std::to_string(id);
    UserTableIngestRequest const request = _findOneRequestBy(cond, extended);
    trans->commit();
    return request;
}

UserTableIngestRequest UserTables::findRequest(std::string const& database, std::string const& table,
                                               bool extended) const {
    std::lock_guard<std::mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    std::string const cond =
            "`database`='" + _conn->escapeString(database) + "' AND `table`='" + _conn->escapeString(table) +
            "' AND `begin_time`=(SELECT MAX(`begin_time`) FROM `UserTables` WHERE `database`='" +
            _conn->escapeString(database) + "' AND `table`='" + _conn->escapeString(table) + "')";
    UserTableIngestRequest const request = _findOneRequestBy(cond, extended);
    trans->commit();
    return request;
}

std::list<UserTableIngestRequest> UserTables::findRequests(std::string const& database,
                                                           std::string const& table, bool filterByStatus,
                                                           UserTableIngestRequest::Status status,
                                                           std::uint64_t beginTimeMs, std::uint64_t endTimeMs,
                                                           std::uint64_t limit, bool extended) const {
    std::lock_guard<std::mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    std::string cond;
    if (!database.empty()) {
        cond += "`database`='" + _conn->escapeString(database) + "'";
    }
    if (!table.empty()) {
        if (!cond.empty()) cond += " AND ";
        cond += "`table`='" + _conn->escapeString(table) + "'";
    }
    if (filterByStatus) {
        if (!cond.empty()) cond += " AND ";
        cond += "`status`='" + UserTableIngestRequest::status2str(status) + "'";
    }
    if (beginTimeMs > 0) {
        if (!cond.empty()) cond += " AND ";
        cond += "`begin_time`>=" + std::to_string(beginTimeMs);
    }
    if (endTimeMs > 0) {
        if (!cond.empty()) cond += " AND ";
        cond += "`begin_time`<=" + std::to_string(endTimeMs);
    }
    std::list<UserTableIngestRequest> requests;
    std::string query = "SELECT `id` FROM `UserTables`" + (cond.empty() ? "" : " WHERE " + cond) +
                        " ORDER BY `begin_time` DESC" + (limit > 0 ? " LIMIT " + std::to_string(limit) : "");
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    if (!_conn->runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw qmeta::SqlError(ERR_LOC, errObj);
    }
    for (sql::SqlResults::iterator rowIter = results.begin(); rowIter != results.end(); ++rowIter) {
        sql::SqlResults::value_type const& row = *rowIter;
        std::string const cond = "`id`=" + std::string(row[0].first);
        requests.push_back(_findOneRequestBy(cond, extended));
    }
    trans->commit();
    return requests;
}

UserTableIngestRequest UserTables::ingestFinished(std::uint32_t id, UserTableIngestRequest::Status status,
                                                  std::string const& error, std::uint32_t transactionId,
                                                  std::uint32_t numChunks, std::uint64_t numRows,
                                                  std::uint64_t numBytes) {
    if (status != UserTableIngestRequest::Status::COMPLETED &&
        status != UserTableIngestRequest::Status::FAILED &&
        status != UserTableIngestRequest::Status::FAILED_LR) {
        throw std::invalid_argument("Invalid status for ingestFinished: " +
                                    UserTableIngestRequest::status2str(status));
    }
    std::lock_guard<std::mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    std::string const cond = "`id`=" + std::to_string(id);
    bool const extended = true;
    UserTableIngestRequest request = _findOneRequestBy(cond, extended);
    if (request.status != UserTableIngestRequest::Status::IN_PROGRESS) {
        throw std::invalid_argument("Request with id=" + std::to_string(id) + " is not in IN_PROGRESS state");
    }
    request.status = status;
    request.endTime = util::TimeUtils::now();
    request.error = error;
    request.numChunks = numChunks;
    request.numRows = numRows;
    request.numBytes = numBytes;
    request.transactionId = transactionId;

    std::string const query =
            "UPDATE `UserTables` SET `status` = '" + UserTableIngestRequest::status2str(request.status) +
            "',`end_time` = " + std::to_string(request.endTime) + ", `error` = '" +
            _conn->escapeString(request.error) + "', `num_chunks` = " + std::to_string(request.numChunks) +
            ", `num_rows` = " + std::to_string(request.numRows) +
            ", `num_bytes` = " + std::to_string(request.numBytes) +
            ", `transaction_id` = " + std::to_string(request.transactionId) +
            " WHERE `id` = " + std::to_string(id);
    sql::SqlErrorObject errObj;
    if (!_conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw qmeta::SqlError(ERR_LOC, errObj);
    }
    trans->commit();
    return request;
}

void UserTables::databaseDeleted(std::string const& database) {
    std::lock_guard<std::mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);
    sql::SqlErrorObject errObj;
    std::string const query =
            "UPDATE `UserTables` SET `delete_time`=" + std::to_string(util::TimeUtils::now()) +
            " WHERE `database`='" + _conn->escapeString(database) + "'";
    if (!_conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw qmeta::SqlError(ERR_LOC, errObj);
    }
    trans->commit();
}

UserTableIngestRequest UserTables::tableDeleted(std::uint32_t id) {
    std::lock_guard<std::mutex> sync(_dbMutex);
    auto trans = QMetaTransaction::create(*_conn);

    std::string const cond = "`id`=" + std::to_string(id);
    bool const extended = true;
    UserTableIngestRequest request = _findOneRequestBy(cond, extended);
    if (request.status == UserTableIngestRequest::Status::IN_PROGRESS) {
        throw std::invalid_argument("Request with id=" + std::to_string(id) +
                                    " is still in IN_PROGRESS state");
    }
    request.deleteTime = util::TimeUtils::now();

    std::string const query =
            "UPDATE `UserTables` SET `status`='" + UserTableIngestRequest::status2str(request.status) +
            "',`delete_time`=" + std::to_string(request.deleteTime) + " WHERE `id`=" + std::to_string(id);
    sql::SqlErrorObject errObj;
    if (!_conn->runQuery(query, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw qmeta::SqlError(ERR_LOC, errObj);
    }
    trans->commit();
    return request;
}

UserTableIngestRequest UserTables::_findOneRequestBy(std::string const& cond, bool extended) const {
    sql::SqlErrorObject errObj;
    sql::SqlResults results;

    // Query the main table to get the basic fields.
    std::string query =
            "SELECT `id`,`status`,`begin_time`,`end_time`,`delete_time`,`error`,`database`,`table`,"
            "`table_type`,`is_temporary`,`data_format`,`num_chunks`,`num_rows`,`num_bytes`,`transaction_id` "
            "FROM `UserTables` "
            "WHERE " +
            cond;
    if (!_conn->runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw qmeta::SqlError(ERR_LOC, errObj);
    }
    sql::SqlResults::iterator rowIter = results.begin();
    if (rowIter == results.end()) {
        LOGS(_log, LOG_LVL_ERROR, "Unknown request: " << cond);
        throw qmeta::IngestRequestNotFound(ERR_LOC, cond);
    }
    sql::SqlResults::value_type const& row = *rowIter;
    if (row.size() != 15) {
        throw std::runtime_error("Unexpected number of columns in UserTables: " + std::to_string(row.size()));
    }
    if (++rowIter != results.end()) {
        throw std::runtime_error("More than one row found in UserTables for condition: " + cond);
    }
    std::size_t col = 0;
    UserTableIngestRequest request;
    request.id = boost::lexical_cast<unsigned int>(row[col++].first);
    request.status = UserTableIngestRequest::str2status(row[col++].first);
    request.beginTime = boost::lexical_cast<std::uint64_t>(row[col++].first);
    request.endTime = boost::lexical_cast<std::uint64_t>(row[col++].first);
    request.deleteTime = boost::lexical_cast<std::uint64_t>(row[col++].first);
    request.error = row[col++].first;
    request.database = row[col++].first;
    request.table = row[col++].first;
    request.tableType = UserTableIngestRequest::str2tableType(row[col++].first);
    request.isTemporary = (std::string(row[col++].first) == "1");
    request.dataFormat = UserTableIngestRequest::str2dataFormat(row[col++].first);
    request.numChunks = boost::lexical_cast<std::uint32_t>(row[col++].first);
    request.numRows = boost::lexical_cast<std::uint64_t>(row[col++].first);
    request.numBytes = boost::lexical_cast<std::uint64_t>(row[col++].first);
    request.transactionId = boost::lexical_cast<std::uint32_t>(row[col++].first);

    // In case if the caller does not need extended information.
    if (!extended) return request;

    // Query the parameters table to get the key-value pairs.
    query = "SELECT `key`,`val` FROM `UserTablesParams` WHERE `id`=" + std::to_string(request.id);
    if (!_conn->runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "SQL query failed: " << query);
        throw qmeta::SqlError(ERR_LOC, errObj);
    }
    for (sql::SqlResults::iterator rowIter = results.begin(); rowIter != results.end(); ++rowIter) {
        sql::SqlResults::value_type const& row = *rowIter;
        std::string const key = row[0].first;

        // Common parameters for all data formats
        if (key == "schema") {
            request.schema = nlohmann::json::parse(row[1].first);
            continue;
        } else if (key == "indexes") {
            request.indexes = nlohmann::json::parse(row[1].first);
            continue;
        } else if (key == "extended") {
            request.extended = nlohmann::json::parse(row[1].first);
            continue;
        }
        LOGS(_log, LOG_LVL_WARN, "Unknown parameter key: " << key);
    }
    return request;
}

}  // namespace lsst::qserv::qmeta
