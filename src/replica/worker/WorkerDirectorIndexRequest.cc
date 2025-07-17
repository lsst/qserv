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
#include "replica/worker/WorkerDirectorIndexRequest.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <iostream>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "global/constants.h"
#include "replica/config/Configuration.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/util/Performance.h"
#include "replica/util/ProtocolBuffer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerDirectorIndexRequest");

}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

WorkerDirectorIndexRequest::Ptr WorkerDirectorIndexRequest::create(
        ServiceProvider::Ptr const& serviceProvider, ConnectionPoolPtr const& connectionPool,
        string const& worker, string const& id, int priority, ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec, ProtocolRequestDirectorIndex const& request) {
    auto ptr = WorkerDirectorIndexRequest::Ptr(
            new WorkerDirectorIndexRequest(serviceProvider, connectionPool, worker, id, priority, onExpired,
                                           requestExpirationIvalSec, request));
    ptr->init();
    return ptr;
}

WorkerDirectorIndexRequest::WorkerDirectorIndexRequest(ServiceProvider::Ptr const& serviceProvider,
                                                       ConnectionPoolPtr const& connectionPool,
                                                       string const& worker, string const& id, int priority,
                                                       ExpirationCallbackType const& onExpired,
                                                       unsigned int requestExpirationIvalSec,
                                                       ProtocolRequestDirectorIndex const& request)
        : WorkerRequest(serviceProvider, worker, "INDEX", id, priority, onExpired, requestExpirationIvalSec),
          _connectionPool(connectionPool),
          _request(request),
          _tmpDirName(serviceProvider->config()->get<string>("worker", "loader-tmp-dir") + "/" +
                      database::mysql::obj2fs(request.database())),
          _fileName(_tmpDirName + "/" + database::mysql::obj2fs(request.director_table()) + "-" +
                    to_string(request.chunk()) +
                    (request.has_transactions() ? "-p" + to_string(request.transaction_id()) : "") + "-" +
                    id) {}

void WorkerDirectorIndexRequest::setInfo(ProtocolResponseDirectorIndex& response) const {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    replica::Lock lock(_mtx, context(__func__));
    response.set_allocated_target_performance(performance().info().release());
    response.set_error(_error);
    response.set_data(_data);
    response.set_total_bytes(_fileSizeBytes);
    *(response.mutable_request()) = _request;
}

bool WorkerDirectorIndexRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    replica::Lock lock(_mtx, context(__func__));
    checkIfCancelling(lock, __func__);

    try {
        // The table will be scanned only when the offset is set to 0.
        if (_request.offset() == 0) {
            auto const config = serviceProvider()->config();
            auto const database = config->databaseInfo(_request.database());

            // Create a folder (if it still doesn't exist) where the temporary files will be placed
            // NOTE: this folder is supposed to be seen by the worker's MySQL/MariaDB server, and it
            // must be write-enabled for an account under which the service is run.
            boost::system::error_code ec;
            fs::create_directory(fs::path(_tmpDirName), ec);
            if (ec.value() != 0) {
                _error = "failed to create folder '" + _tmpDirName;
                LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
                setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::FOLDER_CREATE);
            }

            // Make sure no file exists from any previous attempt to harvest the index data
            // in a scope of the request. Otherwise MySQL query will fail.
            _removeFile();

            // Connect to the worker database
            // Manage the new connection via the RAII-style handler to ensure the transaction
            // is automatically rolled-back in case of exceptions.
            ConnectionHandler const h(_connectionPool);

            // A scope of the query depends on parameters of the request
            h.conn->executeInOwnTransaction([self = shared_from_base<WorkerDirectorIndexRequest>()](
                                                    auto conn) { conn->execute(self->_query(conn)); });
        }
        if (auto const status = _readFile(_request.offset()); status != ProtocolStatusExt::NONE) {
            setStatus(lock, ProtocolStatus::FAILED, status);
        } else {
            setStatus(lock, ProtocolStatus::SUCCESS);
        }
    } catch (ER_NO_SUCH_TABLE_ const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::NO_SUCH_TABLE);
    } catch (database::mysql::ER_PARTITION_MGMT_ON_NONPARTITIONED_ const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::NOT_PARTITIONED_TABLE);
    } catch (database::mysql::ER_UNKNOWN_PARTITION_ const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::NO_SUCH_PARTITION);
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::MYSQL_ERROR);
    } catch (invalid_argument const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  exception: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::INVALID_PARAM);
    } catch (out_of_range const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  exception: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::LARGE_RESULT);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  exception: " << ex.what());
        _error = "Exception: " + string(ex.what());
        setStatus(lock, ProtocolStatus::FAILED);
    }
    return true;
}

string WorkerDirectorIndexRequest::_query(Connection::Ptr const& conn) const {
    auto const config = serviceProvider()->config();
    auto const database = config->databaseInfo(_request.database());
    auto const table = database.findTable(_request.director_table());

    if (!table.isDirector()) {
        throw invalid_argument("table '" + table.name + "' is not been configured as director in database '" +
                               database.name + "'");
    }
    if (table.directorTable.primaryKeyColumn().empty()) {
        throw invalid_argument("director table '" + table.name +
                               "' has not been properly configured in database '" + database.name + "'");
    }
    if (table.columns.empty()) {
        throw invalid_argument("no schema found for director table '" + table.name + "' of database '" +
                               database.name + "'");
    }

    // Find types required by the "director" index table's columns

    string const qservTransId = _request.has_transactions() ? "qserv_trans_id" : string();
    string qservTransIdType;
    string primaryKeyColumnType;
    string subChunkIdColNameType;

    for (auto&& column : table.columns) {
        if (!qservTransId.empty() && column.name == qservTransId)
            qservTransIdType = column.type;
        else if (column.name == table.directorTable.primaryKeyColumn())
            primaryKeyColumnType = column.type;
        else if (column.name == lsst::qserv::SUB_CHUNK_COLUMN)
            subChunkIdColNameType = column.type;
    }
    if ((!qservTransId.empty() && qservTransIdType.empty()) || primaryKeyColumnType.empty() or
        subChunkIdColNameType.empty()) {
        throw invalid_argument(
                "column definitions for the Object identifier or sub-chunk identifier"
                " columns are missing in the director table schema for table '" +
                table.name + "' of database '" + database.name + "'");
    }

    // NOTE: injecting the chunk number into each row of the result set because
    // the chunk-id column is optional.
    QueryGenerator const g(conn);
    DoNotProcess const chunk = g.val(_request.chunk());
    SqlId const sqlTableId = g.id(database.name, table.name + "_" + to_string(_request.chunk()));
    string query;
    if (qservTransId.empty()) {
        query = g.select(table.directorTable.primaryKeyColumn(), chunk, lsst::qserv::SUB_CHUNK_COLUMN) +
                g.from(sqlTableId) + g.orderBy(make_pair(table.directorTable.primaryKeyColumn(), ""));
    } else {
        query = g.select(qservTransId, table.directorTable.primaryKeyColumn(), chunk,
                         lsst::qserv::SUB_CHUNK_COLUMN) +
                g.from(sqlTableId) + g.inPartition(g.partId(_request.transaction_id())) +
                g.orderBy(make_pair(qservTransId, ""), make_pair(table.directorTable.primaryKeyColumn(), ""));
    }
    return query + g.intoOutfile(_fileName);
}

ProtocolStatusExt WorkerDirectorIndexRequest::_readFile(size_t offset) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    // Open the the file.
    ifstream f(_fileName, ios::binary);
    if (!f.good()) {
        _error = "failed to open file '" + _fileName + "'";
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
        return ProtocolStatusExt::FILE_ROPEN;
    }

    // Get the file size.
    boost::system::error_code ec;
    _fileSizeBytes = fs::file_size(_fileName, ec);
    if (ec.value() != 0) {
        _error = "failed to get file size '" + _fileName + "'";
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
        return ProtocolStatusExt::FILE_SIZE;
    }

    // Validate a value of the offset and position indicator as requested.
    if (offset == _fileSizeBytes) {
        _removeFile();
        return ProtocolStatusExt::NONE;
    } else if (offset > _fileSizeBytes) {
        _error = "attempted to read the file '" + _fileName + "' at the offset " + to_string(offset) +
                 " that is beyond the file size of " + to_string(_fileSizeBytes) + " bytes.";
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
        return ProtocolStatusExt::INVALID_PARAM;
    } else if (offset != 0) {
        f.seekg(offset, ios::beg);
    }

    // Resize the memory buffer for the efficiency of the following read.
    size_t const recordSize = std::min(
            _fileSizeBytes - offset,
            std::min(ProtocolBuffer::HARD_LIMIT,
                     serviceProvider()->config()->get<size_t>("worker", "director-index-record-size")));
    _data.resize(recordSize, ' ');

    // Read the specified number of bytes into the buffer.
    ProtocolStatusExt result = ProtocolStatusExt::NONE;
    f.read(&_data[0], recordSize);
    if (f.bad()) {
        _error = "failed to read " + to_string(recordSize) + " bytes from the file '" + _fileName +
                 "' at the offset " + to_string(offset) + ".";
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
        result = ProtocolStatusExt::FILE_READ;
    }
    f.close();

    // If this was the last record read from the file then delete the file.
    if (offset + recordSize >= _fileSizeBytes) {
        _removeFile();
    }
    return result;
}

void WorkerDirectorIndexRequest::_removeFile() const {
    // Make the best attempt to get rid of the temporary file. Ignore any errors
    // for now. Just report them. Note that 'remove_all' won't complain if the file
    // didn't exist.
    boost::system::error_code ec;
    fs::remove_all(fs::path(_fileName), ec);
    if (ec.value() != 0) {
        LOGS(_log, LOG_LVL_WARN, context(__func__) << " failed to remove the temporary file '" << _fileName);
    }
}

}  // namespace lsst::qserv::replica
