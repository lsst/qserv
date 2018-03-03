/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/FileServerConnection.h"

// System headers
#include <cerrno>
#include <cstring>
#include <ctime>
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ServiceProvider.h"

namespace fs    = boost::filesystem;
namespace proto = lsst::qserv::proto;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FileServerConnection");

/// The limit of 16 MB fo rthe maximum record size for file I/O and
/// network operations.
size_t const maxFileBufSizeBytes = 16 * 1024 * 1024;

} /// namespace

namespace {
    
typedef std::shared_ptr<lsst::qserv::replica::ProtocolBuffer> ProtocolBufferPtr;

/// The context for diagnostic & debug printouts
std::string const context = "FILE-SERVER-CONNECTION  ";

bool isErrorCode(boost::system::error_code const& ec,
                 std::string const& scope) {
    if (ec) {
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
                    ProtocolBufferPtr const& ptr,
                    size_t bytes) {

    ptr->resize(bytes);     // make sure the buffer has enough space to accomodate
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
    return not ::isErrorCode(ec, "readIntoBuffer");
}

template <class T>
bool readMessage(boost::asio::ip::tcp::socket& socket,
                 ProtocolBufferPtr const& ptr,
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

FileServerConnection::pointer FileServerConnection::create(ServiceProvider& serviceProvider,
                                                           std::string const& workerName,
                                                           boost::asio::io_service& io_service) {
    return FileServerConnection::pointer(
        new FileServerConnection(
            serviceProvider,
            workerName,
            io_service));
}

FileServerConnection::FileServerConnection(ServiceProvider& serviceProvider,
                                           std::string const& workerName,
                                           boost::asio::io_service& io_service)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
        _workerInfo(serviceProvider.config()->workerInfo(workerName)),
        _socket(io_service),
        _bufferPtr(
            std::make_shared<ProtocolBuffer>(
                serviceProvider.config()->requestBufferSizeBytes())),
        _fileName(),
        _filePtr(0),
        _fileBufSize(serviceProvider.config()->workerFsBufferSizeBytes()),
        _fileBuf(0) {

    if (not _fileBufSize or (_fileBufSize > maxFileBufSizeBytes)) {
        throw std::invalid_argument(
                  "FileServerConnection: the buffer size must be in a range of: 0-" +
                  std::to_string(maxFileBufSizeBytes) + " bytes. Check the configuration.");
    }
    _fileBuf = new uint8_t[_fileBufSize];
    if (not _fileBuf) {
        throw std::runtime_error("FileServerConnection: failed to allocate the buffer, size: " +
                                 std::to_string(maxFileBufSizeBytes) + " bytes.");
    }
}

FileServerConnection::~FileServerConnection() {
    delete [] _fileBuf;
}

void FileServerConnection::beginProtocol() {
    receiveRequest();
}

void FileServerConnection::receiveRequest() {

    LOGS(_log, LOG_LVL_DEBUG, context << "receiveRequest");

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that a client sends the whole message (its frame and
    // the message itsef) at once.

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        boost::bind(
            &FileServerConnection::requestReceived,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void FileServerConnection::requestReceived(boost::system::error_code const& ec,
                                           size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << "requestReceived");

    if (::isErrorCode (ec, "requestReceived")) { return; }

    // Now read the body of the request

    proto::ReplicationFileRequest request;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) { return; }
 
    LOGS(_log, LOG_LVL_INFO, context << "requestReceived  <OPEN> database: " << request.database()
         << ", file: " << request.file());
    
    // Find a file requested by a client
    
    bool        available = false; 
    uint64_t    size      = 0;
    std::time_t mtime     = 0;
    do {
        if (not _serviceProvider.config()->isKnownDatabase(request.database())) {
            LOGS(_log, LOG_LVL_ERROR, context << "requestReceived  unknown database: "
                 << request.database());
            break;
        }
        boost::system::error_code ec;

        fs::path const file = fs::path(_workerInfo.dataDir) / request.database() / request.file();
        fs::file_status const stat = fs::status(file, ec);
        if (stat.type() == fs::status_error) {
            LOGS(_log, LOG_LVL_ERROR, context
                 << "requestReceived  failed to check the status of file: " << file);
            break;
        }
        if (not fs::exists(stat)) {
            LOGS(_log, LOG_LVL_ERROR, context << "requestReceived  file does not exist: " << file);
            break;
        }

        size = fs::file_size(file, ec);
        if (ec) {
            LOGS(_log, LOG_LVL_ERROR, context
                 << "requestReceived  failed to get the file size of: " << file);
            break;
        }
        mtime = fs::last_write_time(file, ec);
        if (ec) {
            LOGS(_log, LOG_LVL_ERROR, context
                 << "requestReceived  failed to get file mtime of: " << file);
            break;
        }

        // If requested open the file and leave its descriptor open

        _fileName = file.string();
        if (request.send_content()) {
            _filePtr  = std::fopen(file.string().c_str(), "rb");
            if (not _filePtr) {
                LOGS(_log, LOG_LVL_ERROR, context
                     << "requestReceived  file open error: " << std::strerror(errno)
                     << ", file: " << file);
                break;
            }
        }
        available = true;
        
    } while (false);

    // Serialize the response into the buffer and send it back to a caller

    proto::ReplicationFileResponse response;
    response.set_available(available);
    response.set_size(size);
    response.set_mtime(mtime);

    _bufferPtr->resize();
    _bufferPtr->serialize(response);

    sendResponse();
}

void FileServerConnection::sendResponse() {

    LOGS(_log, LOG_LVL_DEBUG, context << "sendResponse");

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind(
            &FileServerConnection::responseSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void FileServerConnection::responseSent(boost::system::error_code const& ec,
                                        size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << "responseSent");

    if (::isErrorCode (ec, "sent")) { return; }
 
    // If the file pointer is not set it means one of two reasons:
    //
    // - there was a problem with locating/accessing/opening the file, or
    // - a client indicated no interest in receiving the content of the file
    //
    // In either case just finish the protocol right here.

    if (not _filePtr) { return; }

    // The file is open. Begin streaming its content.
    sendData();
}

void FileServerConnection::sendData() {

    LOGS(_log, LOG_LVL_DEBUG, context << "sendData  file: " << _fileName);
    
    // Read next record if possible (a failure or EOF)

    size_t const bytes =
        std::fread(_fileBuf,
                   sizeof(uint8_t),
                   _fileBufSize,
                   _filePtr);
    if (not bytes) {
        if (std::ferror(_filePtr)) {
            LOGS(_log, LOG_LVL_ERROR, context
                 << "sendData  file read error: " << std::strerror(errno)
                 << ", file: " << _fileName);
        } else if (std::feof(_filePtr)) {
            LOGS(_log, LOG_LVL_INFO, context << "sendData  <CLOSE> file: " << _fileName);
        } else {
            ;   // This file was empty, or the previous read was aligned exactly on
                // the end of the file.
        }
        std::fclose(_filePtr);
        return;
    }

    // Send the record

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(
            _fileBuf,
            bytes
        ),
        boost::bind(
            &FileServerConnection::dataSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void FileServerConnection::dataSent(boost::system::error_code const& ec,
                                    size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << "dataSent");

    if (::isErrorCode (ec, "dataSent")) { return; }

    sendData();
}

}}} // namespace lsst::qserv::replica