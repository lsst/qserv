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
#if 0
// Class header
#include "loader/FileServer.h"

// System headers
#include <boost/bind.hpp>
#include <thread>
#include <boost/filesystem.hpp> // &&& delete?
#include <cerrno>
#include <cstring>
#include <ctime>
#include <stdexcept>

// Qserv headers
#include "proto/loader.pb.h"

#include "lsst/log/Log.h"

namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.FileServer");

/// The limit of 16 MB fo rthe maximum record size for file I/O and
/// network operations.
const size_t maxFileBufSizeBytes = 16 * 1024 * 1024; // &&& there must be a better way to store this.

} /// namespace

namespace lsst {
namespace qserv {
namespace loader {


FileServer::Ptr FileServer::create(FileServerConfig::Ptr const& fileServerConfig) {
    return FileServer::Ptr (new FileServer(fileServerConfig));
}


FileServer::FileServer (FileServerConfig::Ptr const& fileServerConfig)
    : _fileServerConfig(fileServerConfig),
      _acceptor(_io_service,
          boost::asio::ip::tcp::endpoint (
              boost::asio::ip::tcp::v4(),
              _fileServerConfig->getPort())) {
    // Allow socket recycling ports after catastrophic failures.
    _acceptor.set_option(boost::asio::socket_base::reuse_address(true));
}


void FileServer::run() {
    // Start with this so there is something for the pool to do.
    beginAccept();

    // Launch all threads in the pool

    // std::vector<std::shared_ptr<std::thread>> threads(_fileServerConfig->getTargetPoolSize()); &&& old?
    // std::vector<std::shared_ptr<std::thread>> threads(_serviceProvider.config()->workerNumFsProcessingThreads()); &&& old?
    std::vector<std::shared_ptr<std::thread>> threads(_fileServerConfig->getTargetPoolSize());

    for (std::size_t i = 0; i < threads.size(); ++i) {
        std::shared_ptr<std::thread> ptr(
            new std::thread(boost::bind(&boost::asio::io_service::run, &_io_service))); // &&& Why is boost::bind needed?
        threads[i] = ptr;
    }

    // Wait for all threads in the pool to exit.
    for (std::size_t i = 0; i < threads.size(); ++i)
        threads[i]->join();
}


void FileServer::beginAccept () {
    auto connection = FileServerConnection::create(shared_from_this());

    _acceptor.async_accept (
        connection->socket(),
        boost::bind (
            &FileServer::handleAccept,
            shared_from_this(),
            connection,
            boost::asio::placeholders::error
        )
    );
}

void FileServer::handleAccept (FileServerConnection::Ptr const& connection, boost::system::error_code const& err) {
    if (!err) {
        connection->beginProtocol();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, context() << "handleAccept  err:" << err);
    }
    beginAccept();
}


//////////////////////// &&& delete this

// typedef std::shared_ptr<lsst::qserv::replica_core::ProtocolBuffer> ProtocolBufferPtr; &&&
typedef std::shared_ptr<lsst::qserv::replica_core::ProtocolBuffer> ProtocolBufferPtr;

/// The context for diagnostic & debug printouts
const std::string context = "FILE-SERVER-CONNECTION  "; // &&& replace with identifier for file request.

bool FileServerConnection::_isErrorCode (boost::system::error_code ec, std::string const& scope) {

    if (ec) {
        if (ec == boost::asio::error::eof)
            LOGS(_log, LOG_LVL_DEBUG, context << scope << "  ** closed **");
        else
            LOGS(_log, LOG_LVL_ERROR, context << scope << "  ** failed: " << ec << " **");
        return true;
    }
    return false;
}

/* &&& old
FileServer::DataSizeType FileServerConnection::_parseMsgLength(FileServer::DataBuffer &buff) {
    if (buff.size() < sizeof(FileServer::DataSizeType)) {
        std::overflow_error("not enough data to describe message length");
    }

    FileServer::DataType *data = &(buff[0]);
    return ntohl(*(reinterpret_cast<FileServer::DataSizeType const*>(data)));
}


bool FileServerConnection::_readIntoBuffer(FileServer::DataBuffer &buff) {
    boost::system::error_code ec;
    FileServer::DataType *data = &(buff[0]);
    auto bytes = buff.size();
    boost::asio::read(_socket, boost::asio::buffer(data, bytes), boost::asio::transfer_at_least(bytes), ec);
*/

/* &&& old
bool FileServerConnection::_readIntoBuffer (size_t bytes) {
    _bufferPtr->resize(bytes);     // make sure the buffer has enough space to accomodate
                            // the data of the message.
*/

FileServer::DataSizeType FileServerConnection::_parseMsgLength(FileServer::DataBuffer &buff) {
    if (buff.size() < sizeof(FileServer::DataSizeType)) {
        std::overflow_error("not enough data to describe message length");
    }

    FileServer::DataType *data = &(buff[0]);
    return ntohl(*(reinterpret_cast<FileServer::DataSizeType const*>(data)));
}


bool FileServerConnection::_readIntoBuffer(FileServer::DataBuffer &buff) {
    boost::system::error_code ec;
    FileServer::DataType *data = &(buff[0]);
    auto bytes = buff.size();
    boost::asio::read(_socket, boost::asio::buffer(data, bytes), boost::asio::transfer_at_least(bytes), ec);
    return !_isErrorCode(ec, "readIntoBuffer");
}


/* &&& old?
bool FileServerConnection::_readMessage(FileServer::DataBuffer &lengthBuff,
                                        proto::LoaderFileRequest &message) {
    FileServer::DataSizeType bytes = _parseMsgLength(lengthBuff);
    _buffer.reset(new FileServer::DataBuffer(bytes));

    if (!_readIntoBuffer(*_buffer)) return false;

    FileServer::DataType *data = &((*_buffer)[0]);
    if (!message.ParseFromArray(data, bytes)) {
        LOGS(_log, LOG_LVL_WARN, context << " failed to parse message");
    }
*/
bool FileServerConnection::_readMessage(size_t bytes, proto::ReplicationFileRequest &message) {
    if (!_readIntoBuffer (bytes)) return false;

    return true;
}

/* &&&
FileServerConnection::Ptr
FileServerConnection::create(ServiceProvider         &serviceProvider,
                              const std::string       &workerName,
                              boost::asio::io_service &io_service) {
    return FileServerConnection::pointer (
        new FileServerConnection (
            serviceProvider,
            workerName,
            io_service));
}
*/

FileServerConnection::Ptr FileServerConnection::create(FileServer::Ptr const& fileServer) {
    return FileServerConnection::Ptr (new FileServerConnection(fileServer, fileServer->getConfig()));
}

/// &&& it looks like injecting a bufferPtr object would be a better way to go... ??
FileServerConnection::FileServerConnection(FileServer::Ptr const& fileServer,
                                           FileServerConfig::Ptr const& fileServerConfig)
    : _fileServer(fileServer),
/* &&& old?
      _socket(fileServer->getIoService()),
     _fileBufSize(fileServerConfig->getFileBufferSize()) {
*/
/* &&& old?
      _bufferPtr (std::make_shared<ProtocolBuffer>(fileServerConfig()->requestBufferSizeBytes())),
      _socket(fileServer->getIOService()),
     _fileBufSize(serviceProvider.config()->workerFsBufferSizeBytes()) {
*/
      _socket(fileServer->getIoService()),
     _fileBufSize(fileServerConfig->getFileBufferSize()) {

    if (!_fileBufSize || _fileBufSize > maxFileBufSizeBytes)
        throw std::invalid_argument("FileServerConnection: the buffer size must be in a range of: 0-" +
                std::to_string(maxFileBufSizeBytes) + " bytes. Check the configuration.");

    _fileVect.resize(_fileBufSize);
    // _fileBuf = new uint8_t[_fileBufSize]; &&&
    _fileBuf = &(_fileVect[0]);

    if (!_fileBuf)
        throw std::runtime_error("FileServerConnection: failed to allocate the buffer, size: " +
                std::to_string(maxFileBufSizeBytes) + " bytes.");
}



FileServerConnection::~FileServerConnection () {
    // delete [] _fileBuf; &&& delete, destroyed with vector now.
}

void FileServerConnection::beginProtocol () {
    receiveRequest();
}

/* &&& old?
void FileServerConnection::receiveRequest() {

    LOGS(_log, LOG_LVL_DEBUG, context << "receiveRequest");

    // Receive the size of the subsequent message (in bytes).
    //
    // The message itself will be read using the synchronous read method.
    // The client should send it's entire message at once.
    const size_t bytes = sizeof(FileServer::DataSizeType);
    FileServer::DataType *data = &(_sizeBufVect[0]);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(data, bytes),
*/
void FileServerConnection::receiveRequest() {

    LOGS(_log, LOG_LVL_DEBUG, context << "receiveRequest");

    // Receive the size of the subsequent message (in bytes).
    //
    // The message itself will be read using the synchronous read method.
    // The client should send it's entire message at once.
    const size_t bytes = sizeof(FileServer::DataSizeType);
    FileServer::DataType *data = &(_sizeBufVect[0]);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(data, bytes),
        boost::asio::transfer_at_least(bytes),
        boost::bind (
            &FileServerConnection::requestReceived,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}


void FileServerConnection::requestReceived (boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << "requestReceived");

<<<<<<< 5b7a8886eb7965917f9ed0b73048fee54786e72e
/* &&& old?
    if (_isErrorCode(ec, "requestReceived")) return;

    // Now read the body of the request
    proto::LoaderFileRequest request;
    //if (!_readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) return;
    // _sizeBuf should contain the results of FileServerConnection::receiveRequest()
    // &&& maybe add bytes transferred check
    if (!_readMessage(_sizeBufVect, request)) {
        LOGS(_log, LOG_LVL_WARN, context << " failed to read message.");
        return;
    }

    LOGS(_log, LOG_LVL_INFO, context << "requestReceived  <OPEN> file: " << request.file());
*/
/* &&& old?
    if ( ::isErrorCode (ec, "requestReceived")) return;
*/
    if (_isErrorCode(ec, "requestReceived")) return;

    // Now read the body of the request
    proto::LoaderFileRequest request;
    //if (!_readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), request)) return;
    // _sizeBuf should contain the results of FileServerConnection::receiveRequest()
    // &&& maybe add bytes transferred check
    if (!_readMessage(_sizeBufVect, request)) {
        LOGS(_log, LOG_LVL_WARN, context << " failed to read message.");
        return;
    }

    LOGS(_log, LOG_LVL_INFO, context << "requestReceived  <OPEN> file: " << request.file());

    // Find a file requested by a client

    bool        available = false;
    uint64_t    size      = 0;
    std::time_t mtime     = 0;
    do {
        boost::system::error_code ec;
        const fs::path        file = fs::path(_workerInfo.dataDir) / request.database() / request.file();
        const fs::file_status stat = fs::status(file, ec);

        if (stat.type() == fs::status_error) {
            LOGS(_log, LOG_LVL_ERROR, context << "requestReceived  failed to check the status of file: " << file);
            break;
        }
        if (!fs::exists(stat)) {
            LOGS(_log, LOG_LVL_ERROR, context << "requestReceived  file does not exist: " << file);
            break;
        }
        size = fs::file_size(file, ec);
        if (ec) {
            LOGS(_log, LOG_LVL_ERROR, context << "requestReceived  failed to get the file size of: " << file);
            break;
        }
        mtime = fs::last_write_time(file, ec);
        if (ec) {
            LOGS(_log, LOG_LVL_ERROR, context << "requestReceived  failed to get file mtime of: " << file);
            break;
        }

        // If requested open the file and leave its descriptor open

        _fileName = file.string();
        if (request.send_content()) {
            _filePtr  = std::fopen (file.string().c_str(), "rb");
            if (!_filePtr) {
                LOGS(_log, LOG_LVL_ERROR, context << "requestReceived  file open error: " << std::strerror(errno) << ", file: " << file);
                break;
            }
        }
        available = true;

    } while (false); // &&& why bother with do..while ?

    // Serialize the response into the buffer and send it back to a caller

    proto::ReplicationFileResponse response;
    response.set_available (available);
    response.set_size      (size);
    response.set_mtime     (mtime);

    _bufferPtr->resize();
    _bufferPtr->serialize(response);

    sendResponse ();
}

void FileServerConnection::sendResponse () {

    LOGS(_log, LOG_LVL_DEBUG, context << "sendResponse");

    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind (
            &FileServerConnection::responseSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void FileServerConnection::responseSent (const boost::system::error_code &ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG, context << "responseSent");

    if (_isErrorCode (ec, "sent") || !_filePtr) {
        // &&& Need some internal indication that transfer has failed?
        return;
    }

    // The file is open. Begin streaming its content.
    sendData();
}

void FileServerConnection::sendData () {

    LOGS(_log, LOG_LVL_DEBUG, context << "sendData  file: " << _fileName);

    // Read next record if possible (a failure or EOF)
    const size_t bytes = std::fread(_fileBuf, sizeof(uint8_t), _fileBufSize, _filePtr);
    if (!bytes) {
        if (std::ferror(_filePtr)) {
            LOGS(_log, LOG_LVL_ERROR,
                 context << "sendData  file read error: " << std::strerror(errno) << ", file: " << _fileName);
        } else if (std::feof(_filePtr)) {
            LOGS(_log, LOG_LVL_INFO, context << "sendData  <CLOSE> file: " << _fileName);
            // &&& indicate success ?
        } else {
            ; // This file was empty, or the previous read was aligned exactly on the end of the file.
        }

        std::fclose(_filePtr);
        return;
    }

    // Send the record
    boost::asio::async_write (
        _socket,
        boost::asio::buffer (
            _fileBuf,
            bytes
        ),
        boost::bind (
            &FileServerConnection::dataSent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void FileServerConnection::dataSent (const boost::system::error_code &ec,
                                size_t                           bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << "dataSent");

    if (::isErrorCode (ec, "dataSent")) return;

    sendData();
}


}}} // namespace lsst::qserv::loader

#endif


