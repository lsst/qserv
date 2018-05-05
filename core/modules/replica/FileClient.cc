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
#include "replica/FileClient.h"

// Third party headers
#include <boost/bind.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

namespace proto = lsst::qserv::proto;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FileClient");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

FileClient::Ptr FileClient::instance(ServiceProvider::Ptr const& serviceProvider,
                                     std::string const& workerName,
                                     std::string const& databaseName,
                                     std::string const& fileName,
                                     bool readContent) {
    try {
        FileClient::Ptr ptr(
            new FileClient(serviceProvider,
                           workerName,
                           databaseName,
                           fileName,
                           readContent));

        if (ptr->openImpl()) return ptr;

    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, "FileClient::instance  failed to construct an object for "
             << "worker: " << workerName
             << "database: " << databaseName
             << "file: " << fileName
             << ", error: " << ex.what());
    }
    return nullptr;
}

FileClient::FileClient(ServiceProvider::Ptr const& serviceProvider,
                       std::string const& workerName,
                       std::string const& databaseName,
                       std::string const& fileName,
                       bool readContent)
    :   _workerInfo(serviceProvider->config()->workerInfo(workerName)),
        _databaseInfo(serviceProvider->config()->databaseInfo(databaseName)),
        _fileName(fileName),
        _readContent(readContent),
        _bufferPtr(new ProtocolBuffer(serviceProvider->config()->requestBufferSizeBytes())),
        _io_service(),
        _socket(_io_service),
        _size(0),
        _mtime(0),
        _eof(false) {
}

std::string const& FileClient::worker() const {
    return _workerInfo.name;
}

std::string const& FileClient::database() const {
    return _databaseInfo.name;
}

std::string const& FileClient::file() const {
    return _fileName;
}

bool FileClient::openImpl() {

    static std::string const context = "FileClient::openImpl  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    boost::system::error_code ec;

    // Connect to the server synchroniously using error codes to process errors
    // insteda of exceptions.

    boost::asio::ip::tcp::resolver::query query(
        _workerInfo.svcHost,
        std::to_string(_workerInfo.fsPort)
    );
    boost::asio::ip::tcp::resolver resolver(_io_service);
    boost::asio::ip::tcp::resolver::iterator iter =
        resolver.resolve(
            boost::asio::ip::tcp::resolver::query(
                _workerInfo.svcHost,
                std::to_string(_workerInfo.fsPort)),
        ec
    );
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed to resolve the server: "
             << _workerInfo.svcHost << ":" << _workerInfo.fsPort
             << ", error: " << ec.message());
        return false;
    }
    boost::asio::connect(_socket, iter, ec);
    if (ec) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed to connect to the server: "
             << _workerInfo.svcHost << ":" << _workerInfo.fsPort
             << ", error: " << ec.message());
        return false;
    }

    // Send the file request to the server and wait for the reply.
    // This step is also implemented through a series of synchronous operations.
    //
    // NOTE: this sequence of steps is happening inside the exception
    //       trap block to catch exceptions thrown by the Google protobuf
    //       and message buffer manipulation operations. Any exception
    //       happened inside the block will fail the file open operation
    //       with a message posted into the log stream.

    try {

        // Serialize the file open request and send the one to the server

        _bufferPtr->resize();

        proto::ReplicationFileRequest request;
        request.set_database(database());
        request.set_file(file());
        request.set_send_content(_readContent);

        _bufferPtr->serialize(request);

        boost::asio::write(
            _socket,
            boost::asio::buffer(
                _bufferPtr->data(),
                _bufferPtr->size()
            ),
            ec
        );
        if (ec) {
            LOGS(_log, LOG_LVL_ERROR, context
                 << "failed to send the file open request to the server: "
                 << _workerInfo.svcHost << ":" << _workerInfo.fsPort
                 << ", database: " << database() << ", file: " << file()
                 << ", error: " << ec.message());
            return false;
        }

        // Read the response and parse it to see if the file is available

        // Start with receiving the fixed length frame carrying
        // the size (in bytes) the length of the subsequent message.

        const size_t frameLengthBytes = sizeof(uint32_t);

        _bufferPtr->resize(frameLengthBytes);

        boost::asio::read(
            _socket,
            boost::asio::buffer(
                _bufferPtr->data(),
                frameLengthBytes
            ),
            boost::asio::transfer_at_least(frameLengthBytes),
            ec
        );
        if (ec) {
            LOGS(_log, LOG_LVL_ERROR, context
                 << "failed to receive the file open response frame header from the server: "
                 << _workerInfo.svcHost << ":" << _workerInfo.fsPort
                 << ", database: " << database() << ", file: " << file()
                 << ", error: " << ec.message());
            return false;
        }

        // Get the length of the message and try reading the message itself
        // from the socket.

        const uint32_t responseLengthBytes = _bufferPtr->parseLength();

        _bufferPtr->resize(responseLengthBytes);    // make sure the buffer has enough space to accomodate
                                                    // the data of the message.
        boost::asio::read(
            _socket,
            boost::asio::buffer(
                _bufferPtr->data(),
                responseLengthBytes
            ),
            boost::asio::transfer_at_least(responseLengthBytes),
            ec
        );
        if (ec) {
            LOGS(_log, LOG_LVL_ERROR, context
                 << "failed to receive the file open response from the server: "
                 << _workerInfo.svcHost << ":" << _workerInfo.fsPort
                 << ", database: " << database() << ", file: " << file()
                 << ", error: " << ec.message());
            return false;
        }

        // Parse na analyze the response

        proto::ReplicationFileResponse response;
        _bufferPtr->parse(response, responseLengthBytes);

        if (response.available()) {
            _size  = response.size();
            _mtime = response.mtime();
            return true;
        }

    } catch (std::exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context
             << "an exception occured while processing response from the server: "
             << _workerInfo.svcHost << ":" << _workerInfo.fsPort
             << ", database: " << database() << ", file: " << file()
             << ", error: " << ex.what());
    }
    return false;
}


size_t FileClient::read(uint8_t* buf, size_t bufSize) {

    static std::string const context = "FileClient::read  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _readContent) {
        throw FileClientError(
            context + "this file was open in 'stat' mode, server: "
            + _workerInfo.svcHost + ":" + std::to_string(_workerInfo.fsPort) +
            ", database: " + database() +
            ", file: " + file());
    }
    if (not buf or not bufSize) {
        throw std::invalid_argument(
                    context + "  zero buffer pointer or buffer size passed into the method");
    }

    // If EOF was detected earlier
    if (_eof) return 0;

    // Read the specified number of bytes
    boost::system::error_code ec;
    const size_t num = boost::asio::read(
        _socket,
        boost::asio::buffer(
            buf,
            bufSize
        ),
        boost::asio::transfer_at_least(bufSize),
        ec
    );
    if (ec) {
        // The connection may be closed by the server after transferring
        // some amount of byte. We just need to store this status for future attempts
        // to read data from the file.

        if (ec == boost::asio::error::eof) {
            _eof = true;
        } else {
            throw FileClientError(
                        "failed to receive a data record from the server: " +
                        _workerInfo.svcHost + ":" + std::to_string(_workerInfo.fsPort) +
                        ", database: " + database() +
                        ", file: " + file() +
                        ", bufSize: " + std::to_string(bufSize) +
                        ", error code: " + std::to_string(ec.value()) +
                        ", error message: " + ec.message());
        }
    } else {
        if (not num) _eof = true;
    }
    return num;
}

}}} // namespace lsst::qserv::replica
