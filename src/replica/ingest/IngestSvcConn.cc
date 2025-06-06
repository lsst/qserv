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
#include "replica/ingest/IngestSvcConn.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <functional>
#include <thread>
#include <stdexcept>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/constants.h"
#include "http/Exceptions.h"
#include "http/Url.h"
#include "replica/config/Configuration.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ReplicaInfo.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestSvcConn");

/// The context for diagnostic & debug printouts
string const context = "INGEST-SVC-CONN  ";

bool isErrorCode(boost::system::error_code const& ec, string const& scope) {
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

bool readIntoBuffer(boost::asio::ip::tcp::socket& socket, shared_ptr<ProtocolBuffer> const& ptr,
                    size_t bytes) {
    // Make sure the buffer has enough space to accommodate the data
    // of the message. Note, this call may throw an exception which is
    // supposed to be caught by the method's caller.
    ptr->resize(bytes);

    boost::system::error_code ec;
    boost::asio::read(socket, boost::asio::buffer(ptr->data(), bytes), boost::asio::transfer_at_least(bytes),
                      ec);
    return not::isErrorCode(ec, __func__);
}

template <class T>
bool readMessage(boost::asio::ip::tcp::socket& socket, shared_ptr<ProtocolBuffer> const& ptr, size_t bytes,
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
}  // namespace

namespace lsst::qserv::replica {

size_t IngestSvcConn::networkBufSizeBytes = 1024 * 1024;

IngestSvcConn::Ptr IngestSvcConn::create(ServiceProvider::Ptr const& serviceProvider,
                                         string const& workerName, boost::asio::io_service& io_service) {
    return IngestSvcConn::Ptr(new IngestSvcConn(serviceProvider, workerName, io_service));
}

IngestSvcConn::IngestSvcConn(ServiceProvider::Ptr const& serviceProvider, string const& workerName,
                             boost::asio::io_service& io_service)
        : IngestFileSvc(serviceProvider, workerName),
          _socket(io_service),
          _bufferPtr(make_shared<ProtocolBuffer>(
                  serviceProvider->config()->get<size_t>("common", "request-buf-size-bytes"))) {}

void IngestSvcConn::beginProtocol() { _receiveHandshake(); }

void IngestSvcConn::_receiveHandshake() {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(_socket, boost::asio::buffer(_bufferPtr->data(), bytes),
                            boost::asio::transfer_at_least(bytes),
                            bind(&IngestSvcConn::_handshakeReceived, shared_from_this(), _1, _2));
}

void IngestSvcConn::_handshakeReceived(boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) return;

    // Now read the body of the request

    ProtocolIngestHandshakeRequest request;
    if (not::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) return;

    // Check if the client is authorized for the operation
    if (request.auth_key() != serviceProvider()->authKey()) {
        _failed("not authorized");
        return;
    }

    // Initialize parameters of the contribution descriptor
    auto const config = serviceProvider()->config();
    _contrib.transactionId = request.transaction_id();
    _contrib.table = request.table();
    _contrib.chunk = request.chunk();
    _contrib.isOverlap = request.is_overlap();
    _contrib.worker = workerName();
    _contrib.url = request.url();
    if (request.has_charset_name()) {
        _contrib.charsetName = request.charset_name();
    }
    if (_contrib.charsetName.empty()) {
        _contrib.charsetName = config->get<string>("worker", "ingest-charset-name");
    }
    _contrib.dialectInput = csv::DialectInput(request.dialect_input());
    _contrib.retryAllowed = true;  // stays like this before loading data into MySQL
    _contrib.maxNumWarnings = request.max_num_warnings();
    if (_contrib.maxNumWarnings == 0) {
        _contrib.maxNumWarnings = config->get<unsigned int>("worker", "loader-max-warnings");
    }

    // Attempts to pass invalid transaction identifiers or tables are not recorded
    // as transaction contributions since it's impossible to determine a context
    // of these operations.
    auto const databaseServices = serviceProvider()->databaseServices();
    auto const trans = databaseServices->transaction(_contrib.transactionId);
    _contrib.database = trans.database;

    if (!config->databaseInfo(_contrib.database).tableExists(_contrib.table)) {
        _failed("no such table '" + _contrib.table + "' in database '" + _contrib.database + "'.");
    }

    // Prescreen parameters of the request to ensure they're valid in the given
    // contex. Check the state of the transaction. Refuse to proceed with the request
    // if any issues were detected.

    bool const failed = true;

    if (trans.state != TransactionInfo::State::STARTED) {
        _contrib.error = context + string(__func__) + " transactionId=" + to_string(_contrib.transactionId) +
                         " is not active";
        _contrib = databaseServices->createdTransactionContrib(_contrib, failed);
        _failed(_contrib.error);
        return;
    }

    csv::Dialect dialect;
    try {
        http::Url const resource(_contrib.url);
        if (resource.scheme() != http::Url::FILE) {
            throw invalid_argument(context + string(__func__) + " unsupported url '" + _contrib.url + "'");
        }
        dialect = csv::Dialect(_contrib.dialectInput);
        _parser.reset(new csv::Parser(dialect));
    } catch (exception const& ex) {
        _contrib.error = ex.what();
        _contrib = databaseServices->createdTransactionContrib(_contrib, failed);
        _failed(_contrib.error);
        return;
    }

    // Register the contribution
    _contrib = databaseServices->createdTransactionContrib(_contrib);

    // This is where the actual processing of the request begins.
    try {
        _contrib.tmpFile = openFile(_contrib.transactionId, _contrib.table, dialect, _contrib.charsetName,
                                    _contrib.chunk, _contrib.isOverlap);
        _contrib = databaseServices->startedTransactionContrib(_contrib);
    } catch (http::Error const& ex) {
        json const errorExt = ex.errorExt();
        if (!errorExt.empty()) {
            _contrib.httpError = errorExt["http_error"];
            _contrib.systemError = errorExt["system_error"];
            _contrib.retryAllowed = errorExt["retry_allowed"].get<int>() != 0;
        }
        _contrib.error = ex.what();
        _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
        _failed(_contrib.error);
        return;
    } catch (exception const& ex) {
        _contrib.systemError = errno;
        _contrib.error = ex.what();
        _contrib = databaseServices->startedTransactionContrib(_contrib, failed);
        _failed(_contrib.error);
        return;
    }

    // Ask a client to begin sending data.
    _reply(ProtocolIngestResponse::READY_TO_READ_DATA);
}

void IngestSvcConn::_sendResponse() {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    boost::asio::async_write(_socket, boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
                             bind(&IngestSvcConn::_responseSent, shared_from_this(), _1, _2));
}

void IngestSvcConn::_responseSent(boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (!isOpen()) return;
    if (::isErrorCode(ec, __func__)) {
        auto const databaseServices = serviceProvider()->databaseServices();
        bool const failed = true;
        _contrib.error = context + string(__func__) + " " + ec.message();
        _contrib.systemError = ec.value();
        _contrib = databaseServices->readTransactionContrib(_contrib, failed);
        closeFile();
        return;
    }
    _receiveData();
}

void IngestSvcConn::_receiveData() {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(_socket, boost::asio::buffer(_bufferPtr->data(), bytes),
                            boost::asio::transfer_at_least(bytes),
                            bind(&IngestSvcConn::_dataReceived, shared_from_this(), _1, _2));
}

void IngestSvcConn::_dataReceived(boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (!isOpen()) return;

    auto const databaseServices = serviceProvider()->databaseServices();
    bool const failed = true;

    if (::isErrorCode(ec, __func__)) {
        _contrib.error = context + string(__func__) +
                         " failed to receive the data packet from the client, error: " + ec.message();
        _contrib.systemError = ec.value();
        _contrib = databaseServices->readTransactionContrib(_contrib, failed);
        closeFile();
        return;
    }

    ProtocolIngestData request;
    if (not::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) {
        _contrib.error =
                context + string(__func__) + " failed to parse the data packet received from the client";
        _contrib.systemError = errno;
        _contrib = databaseServices->readTransactionContrib(_contrib, failed);
        closeFile();
        return;
    }

    // Parse and process the input data, write the processed data into
    // the output file to be ingested into MySQL.
    _parser->parse(request.data().data(), request.data().size(), request.last(),
                   [&](char const* buf, size_t size) {
                       writeRowIntoFile(buf, size);
                       _contrib.numRows++;
                   });
    _contrib.numBytes += request.data().size();  // count unmodified input data

    ProtocolIngestResponse response;
    if (request.last()) {
        // Finished reading and preprocessing the input file.
        _contrib = databaseServices->readTransactionContrib(_contrib);
        // Irreversible changes to the destination table are about to be made.
        _retryAllowed = false;
        _contrib.retryAllowed = false;
        try {
            loadDataIntoTable(_contrib.maxNumWarnings);
            _contrib.numWarnings = numWarnings();
            _contrib.warnings = warnings();
            _contrib.numRowsLoaded = numRowsLoaded();
            serviceProvider()->databaseServices()->loadedTransactionContrib(_contrib);
            _finished();
        } catch (exception const& ex) {
            _contrib.error = context + string(__func__) + string(" data load failed: ") + ex.what();
            _contrib.systemError = errno;
            serviceProvider()->databaseServices()->loadedTransactionContrib(_contrib, failed);
            _failed(_contrib.error);
        }
    } else {
        _reply(ProtocolIngestResponse::READY_TO_READ_DATA);
    }
}

void IngestSvcConn::_failed(std::string const& msg) {
    LOGS(_log, LOG_LVL_ERROR, msg);
    closeFile();
    _reply(ProtocolIngestResponse::FAILED, msg);
}

void IngestSvcConn::_reply(ProtocolIngestResponse::Status status, string const& msg) {
    ProtocolIngestResponse response;
    response.set_id(_contrib.id);
    response.set_status(status);
    response.set_error(msg);
    response.set_retry_allowed(_retryAllowed);
    response.set_num_warnings(_contrib.numWarnings);
    response.set_num_rows(_contrib.numRows);
    response.set_num_rows_loaded(_contrib.numRowsLoaded);

    _bufferPtr->resize();
    _bufferPtr->serialize(response);

    _sendResponse();
}

}  // namespace lsst::qserv::replica
