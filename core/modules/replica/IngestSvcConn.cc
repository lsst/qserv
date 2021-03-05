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
#include "replica/IngestSvcConn.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <functional>
#include <thread>
#include <stdexcept>

// Qserv headers
#include "global/constants.h"
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestSvcConn");

/// The context for diagnostic & debug printouts
string const context = "INGEST-SVC-CONN  ";

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
    // Make sure the buffer has enough space to accommodate the data
    // of the message. Note, this call may throw an exception which is
    // supposed to be caught by the method's caller.
    ptr->resize(bytes);

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

size_t IngestSvcConn::networkBufSizeBytes = 1024 * 1024;


IngestSvcConn::Ptr IngestSvcConn::create(
        ServiceProvider::Ptr const& serviceProvider,
        string const& workerName,
        string const& authKey,
        boost::asio::io_service& io_service) {
    return IngestSvcConn::Ptr(
        new IngestSvcConn(
            serviceProvider,
            workerName,
            authKey,
            io_service));
}


IngestSvcConn::IngestSvcConn(ServiceProvider::Ptr const& serviceProvider,
                             string const& workerName,
                             string const& authKey,
                             boost::asio::io_service& io_service)
    :   IngestFileSvc(serviceProvider,
                      workerName),
        _authKey(authKey),
        _socket(io_service),
        _bufferPtr(make_shared<ProtocolBuffer>(
            serviceProvider->config()->get<size_t>("common", "request_buf_size_bytes"))) {
}


void IngestSvcConn::beginProtocol() {
    _receiveHandshake();
}


void IngestSvcConn::_receiveHandshake() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), bytes),
        boost::asio::transfer_at_least(bytes),
        bind(&IngestSvcConn::_handshakeReceived, shared_from_this(), _1, _2)
    );
}


void IngestSvcConn::_handshakeReceived(boost::system::error_code const& ec,
                                       size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) return;

    // Now read the body of the request

    ProtocolIngestHandshakeRequest request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) return;

    // Check if the client is authorized for the operation
    if (request.auth_key() != _authKey) {
        _failed("not authorized");
        return;
    }

    try {
        openFile(request.transaction_id(),
                 request.table(),
                 request.column_separator() == ProtocolIngestHandshakeRequest::COMMA ? ',' : '\t',
                 request.chunk(),
                 request.is_overlap());
    } catch (exception const& ex) {
        _failed(ex.what());
        return;
    }

    // Ask a client to send 1 row to begin with. An optimal number of rows will be
    // calculated based later upon a completion of that row and measuring its size.
    _sendReadyToReadData(1);
}


void IngestSvcConn::_sendResponse() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
        bind(&IngestSvcConn::_responseSent, shared_from_this(), _1, _2)
    );
}


void IngestSvcConn::_responseSent(boost::system::error_code const& ec,
                                  size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) {
        closeFile();
        return;
    }
    _receiveData();
}

void IngestSvcConn::_receiveData() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), bytes),
        boost::asio::transfer_at_least(bytes),
        bind(&IngestSvcConn::_dataReceived, shared_from_this(), _1, _2)
    );
}


void IngestSvcConn::_dataReceived(boost::system::error_code const& ec,
                                  size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) {
        closeFile();
        return;
    }

    ProtocolIngestData request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) {
        closeFile();
        return;
    }

    // Prepend each row with the transaction identifier and write it into the output file.
    // Compute the maximum length of the rows. It's value will be used on the next step
    // to advise a client on the most optimal number of rows to be sent with the next
    // batch (of rows).

    size_t rowSize = 0;
    for (int i = 0, num = request.rows_size(); i < num; ++i) {
        auto&& row = request.rows(i);
        writeRowIntoFile(row);
        rowSize = max(rowSize, row.size());
    }

    ProtocolIngestResponse response;
    if (request.last()) {
        try {
            loadDataIntoTable();
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


void IngestSvcConn::_reply(ProtocolIngestResponse::Status status,
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

}}} // namespace lsst::qserv::replica
