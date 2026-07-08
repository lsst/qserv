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
#include "replica/worker/WorkerDirectorIndexHttpRequest.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <system_error>

// Qserv headers
#include "global/constants.h"
#include "replica/config/Configuration.h"
#include "replica/config/ConfigDatabase.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/proto/Protocol.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Performance.h"
#include "util/String.h"

// LSST headers
#include "lsst/log/Log.h"

#define CONTEXT context("WorkerDirectorIndexHttpRequest", __func__)

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerDirectorIndexHttpRequest");

}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

shared_ptr<WorkerDirectorIndexHttpRequest> WorkerDirectorIndexHttpRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, protocol::RequestParams const& params,
        ExpirationCallbackType const& onExpired,
        shared_ptr<database::mysql::ConnectionPool> const& connectionPool) {
    auto ptr = shared_ptr<WorkerDirectorIndexHttpRequest>(new WorkerDirectorIndexHttpRequest(
            serviceProvider, worker, hdr, params, onExpired, connectionPool));
    ptr->init();
    return ptr;
}

WorkerDirectorIndexHttpRequest::WorkerDirectorIndexHttpRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, protocol::RequestParams const& params,
        ExpirationCallbackType const& onExpired,
        shared_ptr<database::mysql::ConnectionPool> const& connectionPool)
        : WorkerHttpRequest(serviceProvider, worker, "INDEX", hdr, params, onExpired),
          _databaseName(params.requiredString("database")),
          _tableName(params.requiredString("director_table")),
          _hasTransactions(params.requiredBool("has_transaction")),
          _transactionId(params.requiredUInt32("transaction_id")),
          _chunkNumber(params.requiredUInt32("chunk")),
          _connectionPool(connectionPool),
          _tmpDirName("worker/director-index/" + database::mysql::obj2fs(_databaseName) + "-" +
                      database::mysql::obj2fs(_tableName)),
          _tmpDirFullPath(serviceProvider->config()->get<string>("worker", "loader-tmp-dir") + "/" +
                          _tmpDirName),
          _fileName(to_string(_chunkNumber) + (_hasTransactions ? "-p" + to_string(_transactionId) : "") +
                    "-" + hdr.id + ".csv"),
          _fullFilePath(_tmpDirFullPath + "/" + _fileName) {}

json WorkerDirectorIndexHttpRequest::getResult() const {
    auto const host = WorkerHttpRequest::getWorkerFqdn(expirationTimeoutSec());
    auto const port = serviceProvider()->config()->get<uint16_t>("worker", "http-svc-port");
    return {{"url", "http://" + host + ":" + to_string(port) + "/" + _tmpDirName + "/" + _fileName},
            {"size_bytes", _fileSizeBytes}};
}

bool WorkerDirectorIndexHttpRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, CONTEXT);

    replica::Lock lock(mtx, CONTEXT);
    checkIfCancelling(lock, CONTEXT);

    // This method will throw ConfigUnknownDatabase if the database is invalid.
    DatabaseInfo const database = serviceProvider()->config()->databaseInfo(_databaseName);

    // This method will throw ConfigUnknownTable if the table is invalid.
    TableInfo const table = database.findTable(_tableName);

    // Validate that the table is indeed a "director" table.
    if (!table.isDirector()) {
        throw invalid_argument("table '" + _tableName + "' is not been configured as director in database '" +
                               _databaseName + "'");
    }
    if (table.directorTable.primaryKeyColumn().empty()) {
        throw invalid_argument("director table '" + _tableName +
                               "' has not been properly configured in database '" + _databaseName + "'");
    }
    if (table.columns.empty()) {
        throw invalid_argument("no schema found for director table '" + _tableName + "' of database '" +
                               _databaseName + "'");
    }
    try {
        // Create a folder (if it still doesn't exist) where the temporary files will be placed
        // NOTE: this folder is supposed to be seen by the worker's MySQL/MariaDB server, and it
        // must be write-enabled for an account under which the service is run.
        std::error_code ec;
        fs::create_directory(fs::path(_tmpDirFullPath), ec);
        if (ec.value() != 0) {
            string const errorMsg =
                    "failed to create folder '" + _tmpDirFullPath + "', error: " + ec.message();
            LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
            setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::FOLDER_CREATE, errorMsg);
            return true;
        }

        // Make sure no file exists from any previous attempt to harvest the index data
        // in a scope of the request. Otherwise MySQL query will fail.
        _removeFile();

        // Connect to the worker database
        // Manage the new connection via the RAII-style handler to ensure the transaction
        // is automatically rolled-back in case of exceptions.
        ConnectionHandler const h(_connectionPool);

        // A scope of the query depends on parameters of the request
        h.conn->executeInOwnTransaction(
                [self = shared_from_base<WorkerDirectorIndexHttpRequest>(),
                 primaryKeyColumn = table.directorTable.primaryKeyColumn()](auto conn) {
                    conn->execute(self->_query(conn, primaryKeyColumn));
                });

        // Get the file size.
        _fileSizeBytes = fs::file_size(_fullFilePath, ec);
        if (ec.value() != 0) {
            string const errorMsg = "failed to get file size '" + _fullFilePath + "', error: " + ec.message();
            LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
            setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::FILE_SIZE, errorMsg);
        } else {
            setStatus(lock, protocol::Status::SUCCESS);
        }

    } catch (ER_NO_SUCH_TABLE_ const& ex) {
        string const errorMsg = "MySQL error: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
        setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::NO_SUCH_TABLE, errorMsg);
    } catch (database::mysql::ER_PARTITION_MGMT_ON_NONPARTITIONED_ const& ex) {
        string const errorMsg = "MySQL error: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
        setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::NOT_PARTITIONED_TABLE, errorMsg);
    } catch (database::mysql::ER_UNKNOWN_PARTITION_ const& ex) {
        string const errorMsg = "MySQL error: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
        setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::NO_SUCH_PARTITION, errorMsg);
    } catch (database::mysql::Error const& ex) {
        string const errorMsg = "MySQL error: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
        setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::MYSQL_ERROR, errorMsg);
    } catch (invalid_argument const& ex) {
        string const errorMsg = "invalid_argument: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
        setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::INVALID_PARAM, errorMsg);
    } catch (out_of_range const& ex) {
        string const errorMsg = "out_of_range: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
        setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::LARGE_RESULT, errorMsg);
    } catch (exception const& ex) {
        string const errorMsg = "exception: " + string(ex.what());
        LOGS(_log, LOG_LVL_ERROR, CONTEXT << " " << errorMsg);
        setStatus(lock, protocol::Status::FAILED, protocol::StatusExt::OTHER_EXCEPTION, errorMsg);
    }
    return true;
}

string WorkerDirectorIndexHttpRequest::_query(Connection::Ptr const& conn,
                                              string const& primaryKeyColumn) const {
    // IMPORTANT: injecting the chunk number into each row of the result set because
    // the chunk-id column is optional.
    QueryGenerator const g(conn);
    DoNotProcess const chunk = g.val(_chunkNumber);
    SqlId const sqlTableId = g.id(_databaseName, _tableName + "_" + to_string(_chunkNumber));
    string query;
    if (_hasTransactions) {
        string const qservTransId = "qserv_trans_id";
        query = g.select(qservTransId, primaryKeyColumn, chunk, lsst::qserv::SUB_CHUNK_COLUMN) +
                g.from(sqlTableId) + g.inPartition(g.partId(_transactionId)) +
                g.orderBy(make_pair(qservTransId, ""), make_pair(primaryKeyColumn, ""));
    } else {
        query = g.select(primaryKeyColumn, chunk, lsst::qserv::SUB_CHUNK_COLUMN) + g.from(sqlTableId) +
                g.orderBy(make_pair(primaryKeyColumn, ""));
    }
    return query + g.intoOutfile(_fullFilePath);
}

void WorkerDirectorIndexHttpRequest::_removeFile() const {
    // Make the best attempt to get rid of the temporary file. Ignore any errors
    // for now. Just report them. Note that 'remove_all' won't complain if the file
    // didn't exist.
    std::error_code ec;
    fs::remove_all(fs::path(_fullFilePath), ec);
    if (ec.value() != 0) {
        LOGS(_log, LOG_LVL_WARN,
             CONTEXT << " failed to remove the temporary file: '" << _fullFilePath
                     << "', error: " << ec.message());
    }
}

}  // namespace lsst::qserv::replica
