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
#include "replica/IngestServerConnection.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestServerConnection");

/// The limit of 16 MB for the maximum record size for file I/O and
/// network operations.
size_t const maxFileBufSizeBytes = 16 * 1024 * 1024;

/// The context for diagnostic & debug printouts
string const context = "INGEST-SERVER-CONNECTION  ";

bool isErrorCode(boost::system::error_code const& ec,
                 string const& scope) {
    if (ec.value() != 0) {
        if (ec == boost::asio::error::eof) {
            LOGS(_log, LOG_LVL_DEBUG, context << scope << "  ** closed **");
        } else {
            LOGS(_log, LOG_LVL_ERROR, context << scope << "  ** failed: " << ec << " **");
        }
        return true;
    }
    return false;
}


bool readIntoBuffer(boost::asio::ip::tcp::socket& socket,
                    shared_ptr<ProtocolBuffer> const& ptr,
                    size_t bytes) {

    ptr->resize(bytes);     // make sure the buffer has enough space to accommodate
                            // the data of the message.
    boost::system::error_code ec;
    boost::asio::read(
        socket,
        boost::asio::buffer(
            ptr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        ec
    );
    return not ::isErrorCode(ec, __func__);
}


template <class T>
bool readMessage(boost::asio::ip::tcp::socket& socket,
                 shared_ptr<ProtocolBuffer> const& ptr,
                 size_t bytes,
                 T& message) {

    if (not readIntoBuffer(socket,
                           ptr,
                           bytes)) return false;

    // Parse the response to see what should be done next.

    ptr->parse(message, bytes);
    return true;
}
}   // namespace

namespace lsst {
namespace qserv {
namespace replica {

size_t IngestServerConnection::networkBufSizeBytes = 1024 * 1024;


IngestServerConnection::Ptr IngestServerConnection::create(
        ServiceProvider::Ptr const& serviceProvider,
        string const& workerName,
        boost::asio::io_service& io_service) {
    return IngestServerConnection::Ptr(
        new IngestServerConnection(
            serviceProvider,
            workerName,
            io_service));
}


IngestServerConnection::IngestServerConnection(ServiceProvider::Ptr const& serviceProvider,
                                               string const& workerName,
                                               boost::asio::io_service& io_service)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
        _workerInfo(serviceProvider->config()->workerInfo(workerName)),
        _socket(io_service),
        _bufferPtr(make_shared<ProtocolBuffer>(
            serviceProvider->config()->requestBufferSizeBytes()
        )) {
}


IngestServerConnection::~IngestServerConnection() {
    _closeFile();
}


void IngestServerConnection::beginProtocol() {
    _receiveHandshake();
}


void IngestServerConnection::_receiveHandshake() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), bytes),
        boost::asio::transfer_at_least(bytes),
        boost::bind(
            &IngestServerConnection::_handshakeReceived,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}


void IngestServerConnection::_handshakeReceived(boost::system::error_code const& ec,
                                                size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) return;

    // Now read the body of the request

    ProtocolIngestHandshakeRequest request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) return;

    _transactionId   = request.transaction_id();
    _table           = request.table();
    _chunk           = request.chunk();
    _isOverlap       = request.is_overlap();
    _columnSeparator = request.column_separator() == ProtocolIngestHandshakeRequest::COMMA ? ',' : '\t';

    // Check if a context of the request is valid
    
    try {
        auto transactionInfo = _serviceProvider->databaseServices()->transaction(_transactionId);
        if (transactionInfo.state != "STARTED") {
            _illegalParameters("transaction is not active");
            return;
        }
        _database = transactionInfo.database;

        // The next test is to see if the chunk number is valid and it's allocated to this
        // worker. This test will also check (indirectly though) that the database is
        // still in the UNPUBLISHED state.
        //
        // TODO: in the future the last test should be made by pulling the most
        // recent state of the database directly from the Configuration. A reason
        // why this can't be done now is because the current implementation of
        // the Configuration service loads and caches its state when the workers
        // are starting. Meanwhile the DatabaseServices never caches its products.

        DatabaseInfo const databaseInfo = _serviceProvider->config()->databaseInfo(_database);
        if (databaseInfo.isPublished) {
            throw invalid_argument("database '" + _database + "' is not in the UNPUBLISHED state");
        }

        vector<ReplicaInfo> replicas;       // Chunk replicas at the current worker found
                                            // among the unpublished databases only
        bool const allDatabases = false;
        bool const isPublished = false;

        _serviceProvider->databaseServices()->findWorkerReplicas(
            replicas,
            _chunk,
            _workerName,
            databaseInfo.family,
            allDatabases,
            isPublished
        );
        bool databaseIsFound = false;
        for (auto&& replica: replicas) {
            if (replica.database() == _database) {
                databaseIsFound = true;
                break;
            }
        }
        if (not databaseIsFound) {
            throw invalid_argument(
                    "chunk " + to_string(_chunk) + " of the UNPUBLISHED database '" +
                    databaseInfo.name + "' is not allocated to worker '" + _workerName + "'");
        }
                
    } catch (DatabaseServicesNotFound const& ex) {
        _illegalParameters("invalid transaction identifier");
        return;
    } catch (invalid_argument const& ex) {
        _illegalParameters(ex.what());
        return;
    }
    
    // Create a temporary file

    boost::system::error_code errCode;
    
    string const fileExt = ".csv";
    string const pattern = _database + "-" + _table + "-" + to_string(_chunk) + "-" +
                           to_string(_transactionId) + "-%%%%" + fileExt;
    fs::path const baseFileName = fs::unique_path(pattern, errCode);
    if (errCode.value() != 0) {
        _failed("failed to allocate the name for a temporary file, error: " + errCode.message());
        return;
    }

    fs::path const filePath = fs::path(_workerInfo.loaderTmpDir) / baseFileName;
    _fileName = filePath.string();
    _file.open(_fileName, ofstream::out);
    if (not _file.is_open()) {
        _failed("failed to create a temporary file: " + _fileName);
        return;
    }

    // Ask a client to send 1 row to begin with. An optimal number of rows will be
    // calculated based later upon a completion of that row and measuting its size.
    _sendReadyToReadData(1);
}


void IngestServerConnection::_sendResponse() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
        boost::bind(
            &IngestServerConnection::_responseSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}


void IngestServerConnection::_responseSent(boost::system::error_code const& ec,
                                           size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) _closeFile();
    if (not _file.is_open()) return;

    _receiveData();
}

void IngestServerConnection::_receiveData() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), bytes),
        boost::asio::transfer_at_least(bytes),
        boost::bind(
            &IngestServerConnection::_dataReceived,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}


void IngestServerConnection::_dataReceived(boost::system::error_code const& ec,
                                           size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) {
        _closeFile();
        return;
    }

    ProtocolIngestData request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) {
        _closeFile();
        return;
    }

    // Prepend each row with the transaction identifier and wite it into the output file.
    // Compute the maximum length of the rows. It's value will be used on the next step
    // to advise a client on the most optimal number of rows to be sent with the next
    // batch (of rows).

    size_t rowSize = 0;
    for (int i = 0, num = request.rows_size(); i < num; ++i) {
        auto&& row = request.rows(i);
        rowSize = max(rowSize, row.size());
        _file << _transactionId << _columnSeparator << row << "\n";
        ++_totalNumRows;
    }

    ProtocolIngestResponse response;
    if (request.last()) {
        LOGS(_log, LOG_LVL_DEBUG, context << __func__ << "  _totalNumRows: " << _totalNumRows);
        
        // Make sure no unsaved rows were staying in memory before proceeding
        // to the loading phase.
        _file.flush();

        try {
            _loadDataIntoTable();
            _finished();
        } catch(exception const& ex) {
            string const error = string("data load failed: ") + ex.what();
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  " << error);
            _failed(error);
        }
    } else {

        size_t maxRows = 1;
        if (rowSize != 0) maxRows = max(maxRows, networkBufSizeBytes / rowSize);
        _sendReadyToReadData(maxRows);
    }
}


void IngestServerConnection::_reply(ProtocolIngestResponse::Status status,
                                    string const& msg,
                                    size_t maxRows) {
    ProtocolIngestResponse response;
    response.set_status(status);
    response.set_error(msg);
    response.set_max_rows(maxRows);

    _bufferPtr->resize();
    _bufferPtr->serialize(response);

    _sendResponse();
}


void IngestServerConnection::_loadDataIntoTable() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    // ATTENTION: this will require that the MySQL server could see the data directory
    // when the CSV file will be residing. So, make proper adjustments to a configuration
    // of the Replication system, so that it would shared that temporary data directory.
    //
    // All workers at NCSA PDAC have the following folder mounted RW into the container:
    //
    //   /qserv/data/
    //
    // So, perhaps, the following folder would work here?
    //
    //   /qserv/data/ingest

    database::mysql::Connection::Ptr conn;
    try {
        
        conn = database::mysql::Connection::open(database::mysql::ConnectionParams(
            _workerInfo.dbHost,
            _workerInfo.dbPort,
            _workerInfo.dbUser,
            _serviceProvider->config()->qservWorkerDatabasePassword(),
            ""
        ));

        string const sqlDatabase     = conn->sqlId(_database);
        string const sqlProtoTable   = sqlDatabase + "." + conn->sqlId(_table);
        string const sqlTable        = sqlDatabase + "." + conn->sqlId(_table + "_" + to_string(_chunk));
        string const sqlOverlapTable = sqlDatabase + "." + conn->sqlId(_table + "FullOverlap_" + to_string(_chunk));
        string const sqlPartition    = conn->sqlId("p" + to_string(_transactionId));

        vector<string> const statements = {
            "CREATE TABLE IF NOT EXISTS " + sqlTable + " LIKE " + sqlProtoTable,
            "ALTER TABLE " + sqlTable + " ADD PARTITION IF NOT EXISTS (PARTITION " + sqlPartition +
                " VALUES IN (" + to_string(_transactionId) + "))",
            "CREATE TABLE IF NOT EXISTS " + sqlOverlapTable + " LIKE " + sqlProtoTable,
            "ALTER TABLE " + sqlOverlapTable + " ADD PARTITION IF NOT EXISTS (PARTITION " + sqlPartition +
                " VALUES IN (" + to_string(_transactionId) + "))",
            "LOAD DATA INFILE " + conn->sqlValue(_fileName) +
                " INTO TABLE " + (_isOverlap ? sqlOverlapTable : sqlTable) +
                " PARTITION (" + sqlPartition + ")" +
                " FIELDS TERMINATED BY " + conn->sqlValue(string() + _columnSeparator)
        };
        for (auto&& statement: statements) {
            LOGS(_log, LOG_LVL_DEBUG, context << __func__ << "  statement: " << statement);
            conn->execute([&statement](decltype(conn) const& conn_) {
                conn_->begin();
                conn_->execute(statement);
                conn_->commit();
            });
       }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  exception: " << ex.what());
        if ((nullptr != conn) and conn->inTransaction()) conn->rollback();
        throw;
    }
}


void IngestServerConnection::_closeFile() {
    if (_file.is_open()) {
        _file.close();
        boost::system::error_code ec;
        fs::remove(_fileName, ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  file removal failed: " << ec.message());
        }
    }
}

}}} // namespace lsst::qserv::replica
