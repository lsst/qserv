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
#include "replica/ExportClient.h"

// System headers
#include <algorithm>
#include <fstream>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/protocol.pb.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ExportClient");

size_t const defaultBufferCapacity = 1024 * 1024;

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

ExportClient::Ptr ExportClient::connect(
        string const& workerHost,
        uint16_t workerPort,
        string const& databaseName,
        string const& tableName,
        unsigned int chunk,
        bool isOverlap,
        string const& outputFilePath,
        ColumnSeparator columnSeparator) {
    ExportClient::Ptr ptr(new ExportClient(
        workerHost,
        workerPort,
        databaseName,
        tableName,
        chunk,
        isOverlap,
        outputFilePath,
        columnSeparator
    ));
    ptr->_connectImpl();
    return ptr;
}


ExportClient::ExportClient(string const& workerHost,
                           uint16_t workerPort,
                           string const& databaseName,
                           string const& tableName,
                           unsigned int chunk,
                           bool isOverlap,
                           string const& outputFilePath,
                           ColumnSeparator columnSeparator)
    :   _workerHost(workerHost),
        _workerPort(workerPort),
        _databaseName(databaseName),
        _tableName(tableName),
        _chunk(chunk),
        _isOverlap(isOverlap),
        _outputFilePath(outputFilePath),
        _columnSeparator(columnSeparator),
        _bufferCapacity(defaultBufferCapacity),
        _bufferPtr(new ProtocolBuffer(defaultBufferCapacity)),
        _io_service(),
        _socket(_io_service) {

    if (outputFilePath.empty()) {
        _abort(__func__, "the file name can't be empty");
    }
}


ExportClient::~ExportClient() {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));
    _closeConnection();
}


void ExportClient::receive() {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    if (_received) return;

    // Make the handshake with the server and wait for the reply.
    ProtocolExportHandshakeRequest handshakeRequest;
    handshakeRequest.set_database(_databaseName);
    handshakeRequest.set_table(_tableName);
    handshakeRequest.set_chunk(_chunk);
    handshakeRequest.set_is_overlap(_isOverlap);
    handshakeRequest.set_column_separator(_columnSeparator == ColumnSeparator::COMMA ?
        ProtocolExportHandshakeRequest::COMMA :
        ProtocolExportHandshakeRequest::TAB
    );
    _send(handshakeRequest, "handshake request send");


    // Read and analyze the response
    ProtocolExportHandshakeResponse handshakeResponse;
    _receive(handshakeResponse, "handshake response receive");
    if (not handshakeResponse.error().empty()) {
        _abort(__func__, "handshake response receive, server error: " + handshakeResponse.error());
    }
    _totalSizeBytes = handshakeResponse.file_size();
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "_totalSizeBytes: " << _totalSizeBytes);

    // The output file is open in the "binary" mode to preserve the original
    // content (including newlines and binary data) received from a remote server.
    ofstream file(_outputFilePath, ofstream::binary);
    if (not file.good()) {
        // Notify the server regarding an abnormal completion of the data transfer
        // before bailing out.
        ProtocolExportRequest request;
        request.set_status(ProtocolExportRequest::FINISHED);
        _send(request, "finish confirmation on the abnormal condition send");
        _abort(__func__, "failed to open/create the file: " + _outputFilePath);
    }

    // Begin requesting and receiving data packets from the server.
    // The data will get written into the output file as they'd be received.
    // The operation's progress monitoring counters will also get updated.
    do {
        // Request the next data packet
        ProtocolExportRequest request;
        request.set_status(ProtocolExportRequest::READY_TO_READ_DATA);
        request.set_max_rows(_numRowsPerReceive);
        _send(request, "data request send");

        // Receive the data
        ProtocolExportResponse response;
        _receive(response, "data response receive");
        if (response.error() != string()) {
            _abort(__func__, "failed to read data, server error: " + response.error());
        }
        size_t numBytes = 0;
        int const numRows = response.rows_size();
        for (int i = 0; i < numRows; ++i) {
            auto&& row = response.rows(i);
            file << row << "\n";
            numBytes += row.size();
        }
        _sizeBytes += numBytes;
        _totalNumRows += numRows;

        // The second check for the number of rows is made just in case. In theory
        // (unless something bad happened) the number of rows should never be less
        // than 1 in normal circumstances. 
        if (response.last() or numRows < 1) {
            // Send a confirmation to the server regarding a completion
            // of the data transfer.
            ProtocolExportRequest request;
            request.set_status(ProtocolExportRequest::FINISHED);
            _send(request, "finish confirmation send");
            break;
        }

        // Adjust the number of rows (only if the current number is too small)
        // not to exceed the network buffer capacity.
        if (_numRowsPerReceive == 1) {
            auto const avgRowSize = numBytes / numRows;
            _numRowsPerReceive = max<long>(_numRowsPerReceive, defaultBufferCapacity / avgRowSize);
            LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "_numRowsPerReceive: " << _numRowsPerReceive);
        }

    } while (true);

    file.flush();
    file.close();
    _closeConnection();

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "_totalNumRows: " << _totalNumRows
         << " _sizeBytes: " << _sizeBytes);

    // As a sanity check, verify if the local file has the same size as
    // the remote one before declaring a success.
    auto const localFileSizeBytes = fs::file_size(fs::path(_outputFilePath));
    if (localFileSizeBytes != _totalSizeBytes) {
        _abort(__func__, "local file: " + _outputFilePath + " size: " + to_string(localFileSizeBytes)
                + " doesn't match the remote file size: " + to_string(_totalSizeBytes));
    }
    _received = true;
}


void ExportClient::_connectImpl() {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    boost::system::error_code ec;

    // Connect to the server synchronously using error codes to process errors
    // instead of exceptions.
    boost::asio::ip::tcp::resolver resolver(_io_service);
    boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(
        boost::asio::ip::tcp::resolver::query(_workerHost,to_string(_workerPort)),
        ec
    );
    _assertErrorCode(ec, __func__, "host/port resolve");

    boost::asio::connect(_socket, iter, ec);
    _assertErrorCode(ec, __func__, "server connect");
}


uint32_t ExportClient::_receiveFrameHeaderAndBody(string const& context) {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    // First, read the fixed frame header carrying the length of
    // a subsequent message.
    const size_t frameLengthBytes = sizeof(uint32_t);

    _bufferPtr->resize(frameLengthBytes);

    boost::system::error_code ec;
    boost::asio::read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), frameLengthBytes),
        boost::asio::transfer_at_least(frameLengthBytes),
        ec
    );
    _assertErrorCode(ec, __func__, "frame header receive, " + context);

    // Parse the length of the message and try reading the message body
    // from the socket.
    const uint32_t messageLengthBytes = _bufferPtr->parseLength();

    _bufferPtr->resize(messageLengthBytes); // make sure the buffer has enough space to
                                            // accommodate the data of the message.
    boost::asio::read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), messageLengthBytes),
        boost::asio::transfer_at_least(messageLengthBytes),
        ec
    );
    _assertErrorCode(ec, __func__, "message body receive, " + context);

    return messageLengthBytes;
}


void ExportClient::_assertErrorCode(boost::system::error_code const& ec,
                                    string const& func,
                                    string const& msg) {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    if (ec.value() != 0) {
        _abort(func, msg + ", error: " + ec.message());
    }
}


void ExportClient::_abort(string const& func,
                          string const& error) {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));
    _closeConnection();
    string const msg = _context(func) + error;
    LOGS(_log, LOG_LVL_ERROR, msg);
    throw ExportClientError(msg);
}

                
void ExportClient::_closeConnection() {

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    // Always attempt to shutdown and close the socket. This code deliberately
    // ignores any abnormal conditions should they happen during the operation.
    boost::system::error_code ec;
    _socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    _socket.close(ec);
}

}}} // namespace lsst::qserv::replica
