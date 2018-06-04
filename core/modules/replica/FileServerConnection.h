// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_FILESERVERCONNECTION_H
#define LSST_QSERV_REPLICA_FILESERVERCONNECTION_H

/// FileServerConnection.h declares:
///
/// class FileServerConnection
/// (see individual class documentation for more information)

// System headers
#include <cstdio>       // std::FILE, C-style file I/O
#include <memory>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

namespace proto = lsst::qserv::proto;

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class FileServerConnection is used for handling file read requests from
  * remote clients. One instance of the class serves one file from one client
  * at a time.
  *
  * Objects of this class are inistantiated by FileServer. After that
  * the server calls this class's method beginProtocol() which startes
  * a series of asynchronous operations to communicate with remote client.
  * When all details of an incoming request are obtained from the client
  * the connection object begins actual processing of the request and
  * communicates with a client as required by the file transfer protocol.
  * All communications are asynchronious and they're using Google protobuf.
  *
  * The lifespan of this object is exactly one request until it's fully
  * satisfied or any failure during request execution (when reading a file,
  * or communicating with a client) occures. When this happens the object
  * stops doing anyting.
  */
class FileServerConnection
    :   public std::enable_shared_from_this<FileServerConnection> {

public:

    /// Shared pointer type for the class
    typedef std::shared_ptr<FileServerConnection> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - provider of various services
     * @param workerName      - worker name
     * @param io_service      - service object for the network I/O operations
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& workerName,
                      boost::asio::io_service& io_service);

    // Default construction and copy semantics are prohibited

    FileServerConnection() = delete;
    FileServerConnection(FileServerConnection const&) = delete;
    FileServerConnection& operator=(FileServerConnection const&) = delete;

    /// Destructor (non-trivial because some resources need to be properly released)
    ~FileServerConnection();

    /// @return network socket associated with the connection.
    boost::asio::ip::tcp::socket& socket() { return _socket; }

    /**
     * Begin communicating asynchroniously with a client. This is essentially
     * an RPC protocol which runs in a loop this sequence of steps:
     * 
     *   - ASYNC: read a frame header of a request
     *   -  SYNC: read the request header (a specification of a file, additional
     *            instructions, etc.)
     *   - ASYNC: write a frame header of a reply to the request
     *            followed by a status (to tell a client if the specified file
     *            is available or not, and if so then what would be its size, etc.)
     *   - ASYNC: if the request is accepted then begin streaming the content of
     *            a file in a series of records until it's done.
     *
     * NOTES: A reason why the read phase is split into three steps is
     *        that a client is expected to send all components of the request
     *        (frame header and request header) at once. This means
     *        the whole incomming message will be already available on the server's
     *        host memory when an asyncronous handler for the freame header will fire.
     *        However, due to a variable length of the request we should know its length
     *        before attempting to read the rest of the incomming message as this (the later)
     *        will require two things: 1) to ensure enough we have enough buffer space
     *        allocated, and 2) to tell the asynchrnous reader function
     *        how many bytes exactly are we going to read.
     * 
     * The chain ends when a client disconnects or when an error condition
     * is met.
     */
    void beginProtocol();

private:

    /**
     * The constructor of the class.
     */
    FileServerConnection(ServiceProvider::Ptr const& serviceProvider,
                         std::string const& workerName,
                         boost::asio::io_service& io_service);

    /**
     * Begin reading (asynchronosly) the frame header of a new request
     *
     * The frame header is presently a 32-bit unsigned integer
     * representing the length of the subsequent message.
     */
    void receiveRequest();

    /**
     * The calback on finishing (either successfully or not) of aynchronious
     * reads. The request will be parsed, analysed and if everything is right
     * the file transfer will begin.
     *
     * @param ec                - error code to be evaluated
     * @param bytes_transferred - number of bytes received from a client
     */
    void requestReceived(boost::system::error_code const& ec,
                         size_t bytes_transferred);

    /**
     * Begin sending (asynchronosly) a result back to a client
     */
    void sendResponse();

    /**
     * The calback on finishing (either successfully or not) of aynchronious writes.
     *
     * @param ec                - error code to be evaluated
     * @param bytes_transferred - number of bytes sent to a client in a response
     */
    void responseSent(boost::system::error_code const& ec,
                      size_t bytes_transferred);

    /**
     * Read the next record from the currently open file, and if succeeded
     * then begin streaming (asynchronosly) it to a client.
     */
    void sendData();

    /**
     * The calback on finishing (either successfully or not) of aynchronious writes.
     *
     * @param ec                - error code to be evaluated
     * @param bytes_transferred - number of bytes of the file payload sent to a client 
     */
    void dataSent(boost::system::error_code const& ec,
                  size_t bytes_transferred);

private:

    ServiceProvider::Ptr _serviceProvider;
    std::string _workerName;

    /// Cached worker descriptor obtained from the configuration
    WorkerInfo _workerInfo;

    /// A socket for communication with clients
    boost::asio::ip::tcp::socket _socket;

    /// Buffer management class facilitating serialization/deserialization
    /// of data sent over the network
    std::shared_ptr<ProtocolBuffer> _bufferPtr;

    /// The name of a file during on-going transfer
    std::string _fileName;

    /// For a file during on-going transfer
    std::FILE* _filePtr;
    
    /// The file record buffer size (bytes)
    size_t _fileBufSize;

    /// The file record buffer
    uint8_t* _fileBuf;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_FILESERVERCONNECTION_H
