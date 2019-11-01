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
#ifndef LSST_QSERV_REPLICA_INGESTSERVERCONNECTION_H
#define LSST_QSERV_REPLICA_INGESTSERVERCONNECTION_H

// System headers
#include <cstdio>
#include <fstream>
#include <memory>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/protocol.pb.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class IngestServerConnection is used in the server-side implementation of
 * the point-to-point catalog data ingest service of the Replication system.
 * The class handles catalog data ingest requests initiated by remote clients.
 * One instance of the class serves one file from one client at a time.
 *
 * Objects of this class are instantiated by IngestServer. After that
 * the server calls this class's method beginProtocol() which starts
 * a series of asynchronous operations to communicate with remote client.
 * When all details of an incoming request are obtained from the client
 * the connection object begins actual processing of the request and
 * communicates with a client as required by the file transfer protocol.
 * All communications are asynchronous and they're using Google Protobuf.
 *
 * The lifespan of this object is exactly one request until it's fully
 * satisfied or any failure during request execution (when loading data into
 * a database, or communicating with a client) occurs. When this happens the object
 * stops doing anything.
 */
class IngestServerConnection : public std::enable_shared_from_this<IngestServerConnection> {

public:

    /// Shared pointer type for the class
    typedef std::shared_ptr<IngestServerConnection> Ptr;

    /// This parameter determines a suggested size of the messages sent by clients
    static size_t networkBufSizeBytes;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider is needed to access Configuration
     * @param workerName  the name of a worker this service is acting
     * upon (used to pull worker-specific configuration options for
     * the service)
     * @param io_service service object for the network I/O operations
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& workerName,
                      boost::asio::io_service& io_service);

    // Default construction and copy semantics are prohibited

    IngestServerConnection() = delete;
    IngestServerConnection(IngestServerConnection const&) = delete;
    IngestServerConnection& operator=(IngestServerConnection const&) = delete;

    /// Destructor (non-trivial because some resources need to be properly released)
    ~IngestServerConnection();

    /// @return network socket associated with the connection.
    boost::asio::ip::tcp::socket& socket() { return _socket; }

    /**
     * Begin communicating asynchronously with a client. This is essentially
     * an RPC protocol which runs in a loop this sequence of steps:
     * 
     *   1. ASYNC: read a frame header of a request
     *       SYNC: read the request header (a scope and parameters of the request, etc.)
     *   2. ASYNC: write a frame header of a reply to the request
     *             followed by a status (to tell a client if parameters of the request
     *             are valid, or if the operation is possible, etc.). If there was
     *             a problem with the request then send ILLEGAL_PARAMETERS or
     *             FAILED and be done.
     *             Otherwise send READY_TO_READ_DATA to invite the client to send the first
     *             batch of rows.
     *   3. ASYNC: read a frame header of the first data request
     *       SYNC: read the body of the data request with rows to be loaded.
     *       SYNC: process and load rows into the destination table
     *   4. ASYNC: if there was a problem with loading rows then send FAILED
     *             with an explanation of the problem and be done.
     *             If not then check flag 'last' in the data request, and if the one
     *             is present send FINISHED to confirm the completion of
     *             the loading and be done.
     *             Otherwise send READ_TO_READ_DATA to encourage the client to send
     *             the next batch of rows. The reply may also be adjusted to
     *             notify the client on the maximum number of rows to be send in
     *             the next request.
     *   5 -> 3:   repeat this in the loop until ll rows are received from the client
     *             and loaded into the database, or until a problem at any stage occurs. 
     *
     * @note
     *   A reason why the read phase is split into two phases (ASYNC, SYNC) is
     *   that a client is expected to send all components of the request
     *   (frame header and request body) at once. This means the whole incoming
     *   message will be already available on the server's host memory when an
     *   asynchronous handler for the frame header will fire.
     *   However, due to a variable length of the request we should know its length
     *   before attempting to read the rest of the incoming message as this (the later)
     *   will require two things: 1) to ensure enough we have enough buffer space
     *   allocated, and 2) to tell the asynchronous reader function
     *   how many bytes exactly are we going to read.
     * 
     * The chain ends when a client disconnects or when an error condition
     * is met.
     */
    void beginProtocol();

private:

    /// @see IngestServerConnection::create()
    IngestServerConnection(ServiceProvider::Ptr const& serviceProvider,
                           std::string const& workerName,
                           boost::asio::io_service& io_service);

    /// Initiate (ASYNC) read of the handshake request from a client)
    void _receiveHandshake();

    /**
     * The callback on finishing (either successfully or not) of the asynchronous
     * read of the handshake request from a client. The request will be parsed,
     * analyzed and if everything is right an invitation to send data will be sent
     * asynchronously to the client.
     *
     * @param ec  error code to be evaluated
     * @param bytes_transferred  number of bytes received from a client
     */
    void _handshakeReceived(boost::system::error_code const& ec,
                            size_t bytes_transferred);

    /**
     * Begin sending (asynchronously) a result back to a client
     */
    void _sendResponse();

    /**
     * The callback on finishing (either successfully or not) of asynchronous writes.
     *
     * @param ec  error code to be evaluated
     * @param bytes_transferred  number of bytes sent to a client in a response
     */
    void _responseSent(boost::system::error_code const& ec,
                       size_t bytes_transferred);

    /**
     * Read asynchronously the next batch of rows from a client. The method
     * will only read the frame header (4 byte length) of a client message. The rest
     * of the message will be requested upon a successful completion of the frame
     * header.
     */
    void _receiveData();

    /**
     * The callback on finishing (either successfully or not) of asynchronous reads.
     * This method will read (SYNC) a body of the message. Then the data received
     * from a client will get processed.
     *
     * @param ec  a error code to be evaluated
     * @param bytes_transferred  the number of bytes of the file payload sent to a client 
     */
    void _dataReceived(boost::system::error_code const& ec,
                       size_t bytes_transferred);

    /**
     * Send back a message with an invitation to a client to send more data.
     *
     * @param maxRows  the maximum number of rows to be requested from a client
     */
    void _sendReadyToReadData(size_t maxRows) {
        _reply(ProtocolIngestResponse::READY_TO_READ_DATA, std::string(), maxRows);
    }

    /**
     * Send back a message with status FAILED and the error message.
     *
     * @param msg  a message to be delivered to a client
     */
    void _failed(std::string const& msg) {
        _closeFile();
        _reply(ProtocolIngestResponse::FAILED, msg);
    }

    /// Send back a message with status FINISHED and no error message.
    void _finished() {
        _closeFile();
        _reply(ProtocolIngestResponse::FINISHED);
    }

    /**
     * Send back a message with status ILLEGAL_PARAMETERS and the error message.
     *
     * @param msg  a message to be delivered to a client
     */
    void _illegalParameters(std::string const& msg) {
        _closeFile();
        _reply(ProtocolIngestResponse::ILLEGAL_PARAMETERS, msg);
    }

    /**
     * Send back a message with a specific status and the error message.
     *
     * @param status  a status code indicating a general reason for the failure
     * @param msg  (optional) message to be delivered to a client
     * @param maxRows (optional) the maximum number of rows to be requested from a client
     */
    void _reply(ProtocolIngestResponse::Status status,
                std::string const& msg=std::string(),
                size_t maxRows=1);

    /// Load the content of the current file into a table
    void _loadDataIntoTable();

    /// Make sure the currently open/created file gets closed and deleted
    void _closeFile();

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;
    std::string          const _workerName;

    /// Cached worker descriptor obtained from the configuration
    WorkerInfo const _workerInfo;

    /// A socket for communication with clients
    boost::asio::ip::tcp::socket _socket;

    /// Buffer management class facilitating serialization/de-serialization
    /// of data sent over the network
    std::shared_ptr<ProtocolBuffer> const _bufferPtr;

    // Parameters defining a scope of the operation are set from the handshake
    // request received from a client.

    unsigned int _transactionId = 0;
    std::string  _database;
    std::string  _table;
    unsigned int _chunk = 0;
    bool         _isOverlap = false;
    char         _columnSeparator = ',';

    // A file for storing rows received from a client before ingesting
    // its content into the database. This file is created after the handshake
    // request is obtained from a client.

    std::string   _fileName;    /// an absolute path name for the file
    std::ofstream _file;        /// The output file stream

    size_t _totalNumRows = 0;   /// The number of rows received and recorded
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_INGESTSERVERCONNECTION_H
