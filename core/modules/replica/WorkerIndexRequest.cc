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
#include "replica/WorkerIndexRequest.h"

// System headers
// System headers
#include <cerrno>
#include <iostream>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "global/constants.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/Performance.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerIndexRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

WorkerIndexRequest::Ptr WorkerIndexRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        ConnectionPoolPtr const& connectionPool,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestIndex const& request) {
    return WorkerIndexRequest::Ptr(new WorkerIndexRequest(serviceProvider,
        connectionPool,
        worker,
        id,
        priority,
        onExpired,
        requestExpirationIvalSec,
        request
    ));
}


WorkerIndexRequest::WorkerIndexRequest(
        ServiceProvider::Ptr const& serviceProvider,
        ConnectionPoolPtr const& connectionPool,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestIndex const& request)
    :   WorkerRequest(
            serviceProvider,
            worker,
            "INDEX",
            id,
            priority,
            onExpired,
            requestExpirationIvalSec),
        _connectionPool(connectionPool),
        _request(request) {
}


void WorkerIndexRequest::setInfo(ProtocolResponseIndex& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    response.set_allocated_target_performance(performance().info().release());
    response.set_error(_error);
    response.set_data(_data);

    *(response.mutable_request()) = _request;
}


bool WorkerIndexRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    switch (status()) {
        case ProtocolStatus::IN_PROGRESS: break;
        case ProtocolStatus::IS_CANCELLING:
            setStatus(lock, ProtocolStatus::CANCELLED);
            throw WorkerRequestCancelled();
        default:
            throw logic_error(
                    "WorkerIndexRequest::" + context(__func__) + "  not allowed while in state: " +
                    WorkerRequest::status2string(status()));
    }
    try {
        auto const config = serviceProvider()->config();
        auto const databaseInfo = config->databaseInfo(_request.database());    
        auto const workerInfo = config->workerInfo(worker());    

        // Create a folder (if it still doesn't exist) where the temporary files will be placed
        // NOTE: this folder is supposed to be seen by the worker's MySQL/MariaDB server, and it
        // must be write-enabled for an account under which the service is run.

        boost::system::error_code ec;
        fs::path const tmpDirPath = fs::path(workerInfo.loaderTmpDir)  / databaseInfo.name;
        fs::create_directory(tmpDirPath, ec);
        if (ec.value() != 0) {
            _error = "failed to create folder '" + tmpDirPath.string();
            LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
            setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::FOLDER_CREATE);
        }
 
        // The name of a temporary file where the index data will be dumped into
        auto const tmpFileName = fs::unique_path("%%%%-%%%%-%%%%-%%%%.tsv", ec);
        if (ec.value() != 0) {
            _error = "failed to create temporary file at '" + tmpDirPath.string();
            LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
            setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::FILE_CREATE);
        }
        _fileName = (tmpDirPath / tmpFileName).string(); 

        // Connect to the worker database
        // Manage the new connection via the RAII-style handler to ensure the transaction
        // is automatically rolled-back in case of exceptions.
        database::mysql::ConnectionHandler const h(_connectionPool);

        // A scope of the query depends on parameters of the request

        auto self = shared_from_base<WorkerIndexRequest>();
        bool fileReadSuccess = false;
        h.conn->execute([self,&fileReadSuccess](decltype(h.conn) const& conn) {
            conn->begin();
            conn->execute(self->_query(conn));
            fileReadSuccess = self->_readFile();
            conn->commit();
        });
        if (fileReadSuccess) setStatus(lock, ProtocolStatus::SUCCESS);
        else                 setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::FILE_READ);

    } catch(database::mysql::ER_NO_SUCH_TABLE_ const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::NO_SUCH_TABLE);
    } catch(database::mysql::ER_PARTITION_MGMT_ON_NONPARTITIONED_ const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::NOT_PARTITIONED_TABLE);
    } catch(database::mysql::ER_UNKNOWN_PARTITION_ const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _error = ex.what();
        setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::NO_SUCH_PARTITION);
    } catch(database::mysql::Error const& ex) {
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



string WorkerIndexRequest::_query(database::mysql::Connection::Ptr const& conn) const {

    auto const config = serviceProvider()->config();
    auto const databaseInfo = config->databaseInfo(_request.database());
    string const& directorTable = databaseInfo.directorTable;

    if (directorTable.empty() or
        (databaseInfo.directorTableKey.count(directorTable) == 0) or
        databaseInfo.directorTableKey.at(directorTable).empty()) {
        throw invalid_argument(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }
    string const& directorTableKey = databaseInfo.directorTableKey.at(directorTable);

    if (0 == databaseInfo.columns.count(directorTable)) {
        throw invalid_argument(
                "no schema found for director table '" + directorTable +
                "' of database '" + databaseInfo.name + "'");
    }

    // Find types required by the secondary index table's columns

    string const qservTransId = _request.has_transactions() ? "qserv_trans_id" : string();
    string qservTransIdType;
    string directorTableKeyType;
    string subChunkIdColNameType;

    for (auto&& coldef: databaseInfo.columns.at(directorTable)) {
        if      (not qservTransId.empty() and coldef.name == qservTransId) qservTransIdType = coldef.type;
        else if (coldef.name == directorTableKey) directorTableKeyType = coldef.type;
        else if (coldef.name == lsst::qserv::SUB_CHUNK_COLUMN) subChunkIdColNameType = coldef.type;
    }
    if ((not qservTransId.empty() and qservTransIdType.empty()) or
        directorTableKeyType.empty() or
        subChunkIdColNameType.empty()) {

        throw invalid_argument(
                "column definitions for the Object identifier or sub-chunk identifier"
                " columns are missing in the director table schema for table '" +
                databaseInfo.directorTable + "' of database '" + databaseInfo.name + "'");
    }

    // NOTE: injecting the chunk number into each row of the result set because
    // the chunk-id column is optional.
    string const columnsEscaped =
        (qservTransId.empty() ? string() : conn->sqlId(qservTransId) + ",") +
        conn->sqlId(directorTableKey) + "," +
        conn->sqlValue(_request.chunk()) + "," +
        conn->sqlId(lsst::qserv::SUB_CHUNK_COLUMN);

    string const databaseTableEscaped =
        conn->sqlId(databaseInfo.name) + "." +
        conn->sqlId(databaseInfo.directorTable + "_" + to_string(_request.chunk()));

    string const partitionRestrictorEscaped =
        qservTransId.empty() ? string() : "PARTITION (" + conn->sqlPartitionId(_request.transaction_id()) + ")";

    string const orderByEscaped =
        (qservTransId.empty() ? string() : conn->sqlId(qservTransId) + ",") +
        conn->sqlId(directorTableKey);

    return
        "SELECT " + columnsEscaped +
        "  FROM " + databaseTableEscaped + " " + partitionRestrictorEscaped +
        "  ORDER BY " + orderByEscaped +
        "  INTO OUTFILE " + conn->sqlValue(_fileName);
}


bool WorkerIndexRequest::_readFile() {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    // Open the stream to 'lock' the file.
    ifstream f(_fileName);
    if (not f.good()) {
        _error = "failed to open file '" + _fileName + "'";
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
        return false;
    }

    // Resize the memory buffer for the efficiency of the following read
    boost::system::error_code ec;
    const auto size = fs::file_size(_fileName, ec);
    if (ec.value() != 0) {
        _error = "failed to get file size '" + _fileName + "'";
        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  " << _error);
        return false;
    }
    _data.resize(size, ' ');

    // Read the whole file into the buffer
    f.read(&_data[0], size);
    f.close();
    return true;
}

}}} // namespace lsst::qserv::replica
