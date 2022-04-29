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
#include "replica/IngestClient.h"

// System headers
#include <cstring>
#include <fstream>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/ProtocolBuffer.h"
#include "replica/protocol.pb.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestClient");

size_t const defaultBufferCapacity = 1024;

}  // namespace

namespace lsst { namespace qserv { namespace replica {

IngestClient::Ptr IngestClient::connect(string const& workerHost, uint16_t workerPort,
                                        TransactionId transactionId, string const& tableName,
                                        unsigned int chunk, bool isOverlap, string const& inputFilePath,
                                        string const& authKey, csv::DialectInput const& dialectInput,
                                        size_t recordSizeBytes) {
    IngestClient::Ptr const ptr(new IngestClient(workerHost, workerPort, transactionId, tableName, chunk,
                                                 isOverlap, inputFilePath, authKey, dialectInput,
                                                 recordSizeBytes));
    ptr->_connectImpl();
    return ptr;
}

IngestClient::IngestClient(string const& workerHost, uint16_t workerPort, TransactionId transactionId,
                           string const& tableName, unsigned int chunk, bool isOverlap,
                           string const& inputFilePath, string const& authKey,
                           csv::DialectInput const& dialectInput, size_t recordSizeBytes)
        : _workerHost(workerHost),
          _workerPort(workerPort),
          _transactionId(transactionId),
          _tableName(tableName),
          _chunk(chunk),
          _isOverlap(isOverlap),
          _inputFilePath(inputFilePath),
          _authKey(authKey),
          _dialectInput(dialectInput),
          _recordSizeBytes(recordSizeBytes),
          _bufferPtr(new ProtocolBuffer(defaultBufferCapacity)),
          _io_service(),
          _socket(_io_service) {
    if (inputFilePath.empty()) _abort(__func__, "the file name can't be empty");
    if (_recordSizeBytes == 0) _abort(__func__, "the record size can't be 0");
}

IngestClient::~IngestClient() {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));
    _closeConnection();
}

void IngestClient::send() {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    if (_sent) return;

    ifstream file(_inputFilePath, ofstream::binary);
    if (!file.good()) {
        _abort(__func__, "failed to open the file: '" + _inputFilePath + "'.");
    }

    unique_ptr<char[]> const record(new char[_recordSizeBytes]);
    bool eof = false;
    do {
        ProtocolIngestData data;
        eof = !file.read(record.get(), _recordSizeBytes);
        if (eof && !file.eof()) {
            _abort(__func__, "failed to open the file: '" + _inputFilePath + +"', error: '" +
                                     strerror(errno) + "', errno: " + to_string(errno));
        }
        size_t const num = file.gcount();
        _sizeBytes += num;
        data.set_data(record.get(), num);
        data.set_last(eof);

        // Send the message, even if the number of rows is zero
        _bufferPtr->resize();
        _bufferPtr->serialize(data);

        boost::system::error_code ec;
        boost::asio::write(_socket, boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()), ec);
        _assertErrorCode(ec, __func__, "data send");

        // Read and analyze the response
        ProtocolIngestResponse response;
        _readResponse(response);
        _retryAllowed = response.retry_allowed();

        switch (response.status()) {
            case ProtocolIngestResponse::READY_TO_READ_DATA:
                if (eof) {
                    _abort(__func__,
                           "protocol error: the server is still waiting for data"
                           " after receiving the last batch of rows.");
                }
                break;

            case ProtocolIngestResponse::FINISHED:
                if (!eof) {
                    _abort(__func__,
                           "protocol error: the server unexpectedly finished accepting"
                           " new data");
                }
                break;

            case ProtocolIngestResponse::FAILED:
                _abort(__func__, "data send, server error: " + response.error());
                break;

            default:
                _abort(__func__, "protocol error #4");
                break;
        }

    } while (!eof);

    LOGS(_log, LOG_LVL_DEBUG, _context(__func__) << "_sizeBytes: " << _sizeBytes);

    _sent = true;
    _closeConnection();
}

bool IngestClient::retryAllowed() const {
    if (!_sent) throw logic_error(_context(__func__) + "the request hasn't been sent");
    return _retryAllowed;
}

void IngestClient::_connectImpl() {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    boost::system::error_code ec;

    // Connect to the server synchronously using error codes to process errors
    // instead of exceptions.
    boost::asio::ip::tcp::resolver resolver(_io_service);
    boost::asio::ip::tcp::resolver::iterator iter =
            resolver.resolve(boost::asio::ip::tcp::resolver::query(_workerHost, to_string(_workerPort)), ec);
    _assertErrorCode(ec, __func__, "host/port resolve");

    boost::asio::connect(_socket, iter, ec);
    _assertErrorCode(ec, __func__, "server connect");

    string const host = boost::asio::ip::host_name(ec);
    _assertErrorCode(ec, __func__, "get the name of the local host");

    fs::path const inputFilePathAbsolute = fs::canonical(fs::path(_inputFilePath), ec);
    _assertErrorCode(ec, __func__, "absolute file path");

    // Make the handshake with the server and wait for the reply.
    ProtocolIngestHandshakeRequest request;
    request.set_transaction_id(_transactionId);
    request.set_table(_tableName);
    request.set_chunk(_chunk);
    request.set_is_overlap(_isOverlap);
    request.set_auth_key(_authKey);
    request.set_url("file://" + host + inputFilePathAbsolute.string());
    request.set_allocated_dialect_input(_dialectInput.toProto().release());
    _bufferPtr->resize();
    _bufferPtr->serialize(request);

    boost::asio::write(_socket, boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()), ec);
    _assertErrorCode(ec, __func__, "handshake send");

    // Read and analyze the response
    ProtocolIngestResponse response;
    _readResponse(response);
    if (response.status() != ProtocolIngestResponse::READY_TO_READ_DATA) {
        _abort(__func__, "handshake receive, server error: " + response.error());
    }
}

void IngestClient::_readResponse(ProtocolIngestResponse& response) {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    const size_t frameLengthBytes = sizeof(uint32_t);

    _bufferPtr->resize(frameLengthBytes);

    boost::system::error_code ec;
    boost::asio::read(_socket, boost::asio::buffer(_bufferPtr->data(), frameLengthBytes),
                      boost::asio::transfer_at_least(frameLengthBytes), ec);
    _assertErrorCode(ec, __func__, "frame receive");

    // Get the length of the message and try reading the message itself
    // from the socket.
    const uint32_t responseLengthBytes = _bufferPtr->parseLength();

    _bufferPtr->resize(responseLengthBytes);  // make sure the buffer has enough space to
                                              // accommodate the data of the message.
    boost::asio::read(_socket, boost::asio::buffer(_bufferPtr->data(), responseLengthBytes),
                      boost::asio::transfer_at_least(responseLengthBytes), ec);
    _assertErrorCode(ec, __func__, "response receive");

    // Parse and analyze the response
    try {
        _bufferPtr->parse(response, responseLengthBytes);
    } catch (exception const& ex) {
        _abort(__func__, "response processing failed: " + string(ex.what()));
    }
}

void IngestClient::_assertErrorCode(boost::system::error_code const& ec, string const& func,
                                    string const& msg) {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    if (ec.value() != 0) {
        _abort(func, msg + ", error: " + ec.message());
    }
}

void IngestClient::_abort(string const& func, string const& error) {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));
    _closeConnection();
    string const msg = _context(func) + error;
    LOGS(_log, LOG_LVL_ERROR, msg);
    throw IngestClientError(msg);
}

void IngestClient::_closeConnection() {
    LOGS(_log, LOG_LVL_DEBUG, _context(__func__));

    // Always attempt to shutdown and close the socket. This code deliberately
    // ignores any abnormal conditions should they happen during the operation.
    boost::system::error_code ec;
    _socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    _socket.close(ec);
}

}}}  // namespace lsst::qserv::replica
