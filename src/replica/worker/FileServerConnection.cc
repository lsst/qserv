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
#include "replica/worker/FileServerConnection.h"

// System headers
#include <cerrno>
#include <cstring>
#include <ctime>
#include <functional>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/services/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
namespace fs = boost::filesystem;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FileServerConnection");

/// The limit of 16 MB for the maximum record size for file I/O and
/// network operations.
size_t const maxFileBufSizeBytes = 16 * 1024 * 1024;

/// The context for diagnostic & debug printouts
string const context = "FILE-SERVER-CONNECTION  ";

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
    ptr->resize(bytes);  // make sure the buffer has enough space to accommodate
                         // the data of the message.
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

FileServerConnection::Ptr FileServerConnection::create(ServiceProvider::Ptr const& serviceProvider,
                                                       string const& workerName,
                                                       boost::asio::io_service& io_service) {
    return FileServerConnection::Ptr(new FileServerConnection(serviceProvider, workerName, io_service));
}

FileServerConnection::FileServerConnection(ServiceProvider::Ptr const& serviceProvider,
                                           string const& workerName, boost::asio::io_service& io_service)
        : _serviceProvider(serviceProvider),
          _workerName(workerName),
          _socket(io_service),
          _bufferPtr(make_shared<ProtocolBuffer>(
                  serviceProvider->config()->get<size_t>("common", "request-buf-size-bytes"))),
          _fileName(),
          _filePtr(0),
          _fileBufSize(serviceProvider->config()->get<size_t>("worker", "fs-buf-size-bytes")),
          _fileBuf(0) {
    if (not _fileBufSize or (_fileBufSize > maxFileBufSizeBytes)) {
        throw invalid_argument("FileServerConnection: the buffer size must be in a range of: 0-" +
                               to_string(maxFileBufSizeBytes) + " bytes. Check the configuration.");
    }
    _fileBuf = new uint8_t[_fileBufSize];
    if (not _fileBuf) {
        throw runtime_error("FileServerConnection: failed to allocate the buffer, size: " +
                            to_string(maxFileBufSizeBytes) + " bytes.");
    }
}

FileServerConnection::~FileServerConnection() { delete[] _fileBuf; }

void FileServerConnection::beginProtocol() { _receiveRequest(); }

void FileServerConnection::_receiveRequest() {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that a client sends the whole message (its frame and
    // the message itself) at once.

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(_socket, boost::asio::buffer(_bufferPtr->data(), bytes),
                            boost::asio::transfer_at_least(bytes),
                            bind(&FileServerConnection::_requestReceived, shared_from_this(), _1, _2));
}

void FileServerConnection::_requestReceived(boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) return;

    // Now read the body of the request

    ProtocolFileRequest request;
    if (not::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) return;

    LOGS(_log, LOG_LVL_INFO,
         context << __func__ << "  <OPEN> database: " << request.database() << ", file: " << request.file());

    // Find a file requested by a client

    bool available = false;
    bool foreignInstance = false;
    uint64_t size = 0;
    time_t mtime = 0;
    do {
        if (not _serviceProvider->config()->isKnownDatabase(request.database())) {
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  unknown database: " << request.database());
            break;
        }
        if (_serviceProvider->instanceId() != request.instance_id()) {
            LOGS(_log, LOG_LVL_ERROR,
                 context << __func__ << "  Qserv instance of the request: '" << request.instance_id() << "'"
                         << " doesn't match the one of this service: '" << _serviceProvider->instanceId()
                         << "'");
            foreignInstance = true;
            break;
        }
        boost::system::error_code ec;

        fs::path const file = fs::path(_serviceProvider->config()->get<string>("worker", "data-dir")) /
                              request.database() / request.file();
        fs::file_status const stat = fs::status(file, ec);
        if (stat.type() == fs::status_error) {
            LOGS(_log, LOG_LVL_ERROR,
                 context << __func__ << "  failed to check the status of file: " << file);
            break;
        }
        if (not fs::exists(stat)) {
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  file does not exist: " << file);
            break;
        }

        size = fs::file_size(file, ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  failed to get the file size of: " << file);
            break;
        }
        mtime = fs::last_write_time(file, ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_ERROR, context << __func__ << "  failed to get file mtime of: " << file);
            break;
        }

        // If the file content was requested then open the file and leave
        // its descriptor open.

        _fileName = file.string();
        if (request.send_content()) {
            _filePtr = fopen(file.string().c_str(), "rb");
            if (not _filePtr) {
                LOGS(_log, LOG_LVL_ERROR,
                     context << __func__ << "  file open error: " << strerror(errno) << ", file: " << file);
                break;
            }
        }
        available = true;

    } while (false);

    // Serialize the response into the buffer and send it back to a caller

    ProtocolFileResponse response;
    response.set_available(available);
    response.set_size(size);
    response.set_mtime(mtime);
    response.set_foreign_instance(foreignInstance);

    _bufferPtr->resize();
    _bufferPtr->serialize(response);

    _sendResponse();
}

void FileServerConnection::_sendResponse() {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    boost::asio::async_write(_socket, boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
                             bind(&FileServerConnection::_responseSent, shared_from_this(), _1, _2));
}

void FileServerConnection::_responseSent(boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) return;

    // If the file pointer is not set it means one of two reasons:
    //
    // - there was a problem with locating/accessing/opening the file, or
    // - a client indicated no interest in receiving the content of the file
    //
    // In either case just finish the protocol right here.

    if (not _filePtr) return;

    // The file is open. Begin streaming its content.
    _sendData();
}

void FileServerConnection::_sendData() {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__ << "  file: " << _fileName);

    // Read next record if possible (a failure or EOF)

    size_t const bytes = fread(_fileBuf, sizeof(uint8_t), _fileBufSize, _filePtr);
    if (not bytes) {
        if (ferror(_filePtr)) {
            LOGS(_log, LOG_LVL_ERROR,
                 context << __func__ << "  file read error: " << strerror(errno) << ", file: " << _fileName);
        } else if (feof(_filePtr)) {
            LOGS(_log, LOG_LVL_INFO, context << __func__ << "  <CLOSE> file: " << _fileName);
        } else {
            ;  // This file was empty, or the previous read was aligned exactly on
               // the end of the file.
        }
        fclose(_filePtr);
        return;
    }

    // Send the record

    boost::asio::async_write(_socket, boost::asio::buffer(_fileBuf, bytes),
                             bind(&FileServerConnection::_dataSent, shared_from_this(), _1, _2));
}

void FileServerConnection::_dataSent(boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);
    if (::isErrorCode(ec, __func__)) return;
    _sendData();
}

}  // namespace lsst::qserv::replica
