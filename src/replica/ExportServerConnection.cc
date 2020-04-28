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
#include "replica/ExportServerConnection.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <functional>
#include <thread>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "global/constants.h"
#include "replica/ChunkedTable.h"
#include "replica/Configuration.h"
#include "replica/ConfigurationIFace.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/FileUtils.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
namespace fs = boost::filesystem;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ExportServerConnection");

/// The context for diagnostic & debug printouts
string const context = "EXPORT-SERVER-CONNECTION  ";

bool isErrorCode(boost::system::error_code const& ec, string const& scope) {
    if (ec.value() == 0) return false;
    if (ec == boost::asio::error::eof) {
        LOGS(_log, LOG_LVL_DEBUG, context << scope << "  ** closed **");
    } else {
        LOGS(_log, LOG_LVL_ERROR, context << scope << "  ** failed: " << ec << " **");
    }
    return true;
}


bool readIntoBuffer(boost::asio::ip::tcp::socket& socket,
                    shared_ptr<ProtocolBuffer> const& ptr,
                    size_t bytes) {
    // Make sure the buffer has enough space to accommodate the data
    // of the message. Note, this call may throw an exception which is
    // supposed to be caught by the method's caller.
    ptr->resize(bytes);

    boost::system::error_code ec;
    boost::asio::read(
        socket,
        boost::asio::buffer(ptr->data(), bytes),
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
    try {
        if (readIntoBuffer(socket, ptr, bytes)) {
            ptr->parse(message, bytes);
            return true;
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << __func__ << ex.what());
    }
    return false;
}
}   // namespace

namespace lsst {
namespace qserv {
namespace replica {

size_t ExportServerConnection::networkBufSizeBytes = 1024 * 1024;


ExportServerConnection::Ptr ExportServerConnection::create(
        ServiceProvider::Ptr const& serviceProvider,
        string const& workerName,
        string const& authKey,
        boost::asio::io_service& io_service) {
    return ExportServerConnection::Ptr(
        new ExportServerConnection(
            serviceProvider,
            workerName,
            authKey,
            io_service));
}


ExportServerConnection::ExportServerConnection(ServiceProvider::Ptr const& serviceProvider,
                                               string const& workerName,
                                               string const& authKey,
                                               boost::asio::io_service& io_service)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
        _authKey(authKey),
        _workerInfo(serviceProvider->config()->workerInfo(workerName)),
        _socket(io_service),
        _bufferPtr(make_shared<ProtocolBuffer>(
            serviceProvider->config()->requestBufferSizeBytes()
        )) {
}


ExportServerConnection::~ExportServerConnection() {
    _closeFile();
}


void ExportServerConnection::beginProtocol() {
    _receiveHandshake();
}


void ExportServerConnection::_receiveHandshake() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);
    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), bytes),
        boost::asio::transfer_at_least(bytes),
        bind(&ExportServerConnection::_handshakeReceived, shared_from_this(), _1, _2)
    );
}


void ExportServerConnection::_handshakeReceived(boost::system::error_code const& ec,
                                                size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) return;

    // This flag will be passed as an optional parameter into method _failed() to
    // ensure a proper error notification is sent to the client in the handhshake
    // response message.
    bool const isHandshakeError = true;

    // Now read the body of the request
    ProtocolExportHandshakeRequest request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) return;

    _database        = request.database();
    _table           = request.table();
    _chunk           = request.chunk();
    _isOverlap       = request.is_overlap();
    _columnSeparator = request.column_separator() == ProtocolExportHandshakeRequest::COMMA ? ',' : '\t';

    // Check if the client is authorized for the operation

    if (request.auth_key() != _authKey) {
        _failed("not authorized");
        return;
    }

    // Check if a context of the request is valid
    try {

        // Get and validate a status of the database and the table
        _databaseInfo = _serviceProvider->config()->databaseInfo(_database);
        if (not _databaseInfo.isPublished) {
            throw invalid_argument("database '" + _databaseInfo.name + "' is not PUBLISHED");
        }
        _isPartitioned = _databaseInfo.partitionedTables.end() != find(
                _databaseInfo.partitionedTables.begin(),
                _databaseInfo.partitionedTables.end(),
                _table);
        if (not _isPartitioned) {
            if (_databaseInfo.regularTables.end() == find(
                    _databaseInfo.regularTables.begin(),
                    _databaseInfo.regularTables.end(),
                    _table)) {
                throw invalid_argument(
                        "no such table '" + _table + "' in a scope of database '" +
                        _databaseInfo.name + "'");
            }
        }

        // The next test is for the partitioned tables, and it's meant to check if
        // the chunk number is valid and it's allocated to this worker. The test will
        // also ensure that the database is in the PUBLISHED state.

        if (_isPartitioned) {

            vector<ReplicaInfo> replicas;       // Chunk replicas at the current worker found
                                                // among the unpublished databases only
            bool const allDatabases = false;
            bool const isPublished = true;

            _serviceProvider->databaseServices()->findWorkerReplicas(
                replicas,
                _chunk,
                _workerName,
                _databaseInfo.family,
                allDatabases,
                isPublished
            );
            if (replicas.cend() == find_if(replicas.cbegin(), replicas.cend(),
                    [&](ReplicaInfo const& replica) {
                        return replica.database() == _databaseInfo.name;
                    })) {
                throw invalid_argument(
                        "chunk " + to_string(_chunk) + " of the PUBLISHED database '" +
                        _databaseInfo.name + "' is not allocated to worker '" + _workerName + "'");
            }
        }

    } catch (DatabaseServicesNotFound const& ex) {
        _failed("invalid database name", isHandshakeError);
        return;
    } catch (invalid_argument const& ex) {
        _failed(ex.what(), isHandshakeError);
        return;
    }

    // The file name is made of a fixed part based on a scope of the client's request
    // and a variable part based on some random number generator. This tactics has two
    // goals:
    // - easier investigate problems (should they happen) with the table export operations
    // - and eliminate a possibility of the naming conflicts in case if two (or many)
    //   similar requests were made by clients.
    try {
        auto const baseFileName =
            _databaseInfo.name + "." +
            (_isPartitioned ? ChunkedTable(_table, _chunk, _isOverlap).name() : _table);
        _fileName = FileUtils::createTemporaryFile(
            _workerInfo.exporterTmpDir,
            baseFileName,
            ".%%%%-%%%%-%%%%-%%%%",
            ".csv"
        );
        boost::system::error_code ec;
        fs::remove(fs::path(_fileName), ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  file removal failed: " << ec.message());
        }
    } catch (exception const& ex) {
        _failed("failed to generate a unique name for a temporary file, ex: " + string(ex.what()), isHandshakeError);
        return;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << __func__ << "  output file: " << _fileName);

    // Note that, depending on a size of the table and the current load on
    // the database server and the underlying file system, this operation may take
    // a while.
    try {
        _dumpTableIntoFile();

        fs::path const p = _fileName;
        if (not fs::exists(p)) {
            _failed("table dump file " + _fileName + " can't be located", isHandshakeError);
            return;
        }
        if (not fs::is_regular(p)) {
            _failed("table dump file " + _fileName + " is not a regular file", isHandshakeError);
            return;
        }
        _fileSizeBytes = fs::file_size(p);

    } catch (exception const& ex) {
        _failed("failed to dump the table into a temporary file, ex: " + string(ex.what()), isHandshakeError);
        return;
    }

    // Keep the file open for the duration of the communication with the client
    _file.open(_fileName, ofstream::in);
    if (not _file.is_open()) {
        _failed("failed to open the table dump file: " + _fileName, isHandshakeError);
        return;
    }

    // Tell the client to proceed with the data transfer protocol.
    _sendHandshakeResponse();
}


void ExportServerConnection::_sendHandshakeResponse(string const& error) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    ProtocolExportHandshakeResponse response;
    if (error.empty()) response.set_file_size(_fileSizeBytes);
    else               response.set_error(error);

    _bufferPtr->resize();
    _bufferPtr->serialize(response);

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
        bind(&ExportServerConnection::_handshakeResponseSent, shared_from_this(), _1, _2)
    );
}


void ExportServerConnection::_handshakeResponseSent(boost::system::error_code const& ec,
                                                    size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) _closeFile();
    if (not _file.is_open()) return;

    _receiveDataRequest();
}


void ExportServerConnection::_receiveDataRequest() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);
    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), bytes),
        boost::asio::transfer_at_least(bytes),
        bind(&ExportServerConnection::_dataRequestReceived, shared_from_this(), _1, _2)
    );
}


void ExportServerConnection::_dataRequestReceived(boost::system::error_code const& ec,
                                                  size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__ << " bytes_transferred=" << bytes_transferred);

    if (::isErrorCode(ec, __func__)) {
        _closeFile();
        return;
    }

    ProtocolExportRequest request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) {
        _closeFile();
        return;
    }
    if (request.status() != ProtocolExportRequest::READY_TO_READ_DATA) {
        _closeFile();
        return;
    }

    auto maxRowsPerSend = request.max_rows();
    if (maxRowsPerSend == 0) {
        _closeFile();
        return;
    }

    // Read up to maxRowsPerSend from the file and insert into the message.
    // If the file has exactly or fewer rows then set flag 'last' in the message
    // to indicate end of the transmission.
    //
    // NOTE: the ugly static_cast is needed due to a problme in the Protobuf API
    // of methods returning sizes of collection as signed integer numbers. This makes
    // no sense.

    ProtocolExportResponse response;

    bool eof = false;
    string row;
    while (static_cast<decltype(maxRowsPerSend)>(response.rows_size()) < maxRowsPerSend) {
        if (getline(_file, row)) {
            response.add_rows(row);
        } else {
            eof = true;
            break;
        }
    }
    response.set_last(eof);

    // Send the message, even if the number of rows is zero
    _bufferPtr->resize();
    _bufferPtr->serialize(response);

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
        bind(&ExportServerConnection::_dataResponseSent, shared_from_this(), _1, _2)
    );

}


void ExportServerConnection::_dataResponseSent(boost::system::error_code const& ec,
                                               size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) _closeFile();
    if (not _file.is_open()) return;

    _receiveDataRequest();
}


void ExportServerConnection::_dumpTableIntoFile() const {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    // ATTENTION: the table dump method used in this implementation requires
    // that the MySQL server has the write access to files in a folder in which
    // the CSV file will be stored by this server. So, make proper adjustments
    // to the Configuration of the Replication system.

    try {
        database::mysql::ConnectionHandler handler(
            database::mysql::Connection::open(
                database::mysql::ConnectionParams(
                    _workerInfo.dbHost,
                    _workerInfo.dbPort,
                    _workerInfo.dbUser,
                    _serviceProvider->config()->qservWorkerDatabasePassword(),
                    ""
                )
            )
        );
        string const statement =
            "SELECT * FROM " + handler.conn->sqlId(_databaseInfo.name) + "." +
                handler.conn->sqlId(_isPartitioned ? ChunkedTable(_table, _chunk, _isOverlap).name() : _table) +
            " INTO OUTFILE " + handler.conn->sqlValue(_fileName) +
            " FIELDS TERMINATED BY " + handler.conn->sqlValue(string() + _columnSeparator);

        LOGS(_log, LOG_LVL_DEBUG, context << __func__ << "  statement: " << statement);

        handler.conn->execute([&statement](decltype(handler.conn) const& conn_) {
            conn_->begin();
            conn_->execute(statement);
            conn_->commit();
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  exception: " << ex.what());
        throw;
    }
}


void ExportServerConnection::_closeFile() {
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
