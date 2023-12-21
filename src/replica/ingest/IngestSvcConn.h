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
#ifndef LSST_QSERV_REPLICA_INGESTSVCCONN_H
#define LSST_QSERV_REPLICA_INGESTSVCCONN_H

// System headers
#include <memory>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/ingest/IngestFileSvc.h"
#include "replica/proto/protocol.pb.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Csv.h"
#include "replica/util/ProtocolBuffer.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class IngestSvcConn is used in the server-side implementation of
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
class IngestSvcConn : public IngestFileSvc, public std::enable_shared_from_this<IngestSvcConn> {
public:
    typedef std::shared_ptr<IngestSvcConn> Ptr;

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
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                      boost::asio::io_service& io_service);

    // Default construction and copy semantics are prohibited

    IngestSvcConn() = delete;
    IngestSvcConn(IngestSvcConn const&) = delete;
    IngestSvcConn& operator=(IngestSvcConn const&) = delete;

    virtual ~IngestSvcConn() = default;

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
     *             a problem with the request then send FAILED and be done.
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
     *   5 -> 3:   repeat this in the loop until all rows are received from the client
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
    /// @see IngestSvcConn::create()
    IngestSvcConn(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
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
    void _handshakeReceived(boost::system::error_code const& ec, size_t bytes_transferred);

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
    void _responseSent(boost::system::error_code const& ec, size_t bytes_transferred);

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
    void _dataReceived(boost::system::error_code const& ec, size_t bytes_transferred);

    /**
     * Send back a message with status FAILED and the error message.
     *
     * @param msg  a message to be delivered to a client
     */
    void _failed(std::string const& msg);

    /// Send back a message with status FINISHED and no error message.
    void _finished() {
        closeFile();
        _reply(ProtocolIngestResponse::FINISHED);
    }

    /**
     * Send back a message with a specific status and the error message.
     *
     * @param status  a status code indicating a general reason for the failure
     * @param msg  (optional) message to be delivered to a client
     * @param maxRows (optional) the maximum number of rows to be requested from a client
     */
    void _reply(ProtocolIngestResponse::Status status, std::string const& msg = std::string());

    /// A socket for communication with clients
    boost::asio::ip::tcp::socket _socket;

    /// Buffer management class facilitating serialization/de-serialization
    /// of data sent over the network
    std::shared_ptr<ProtocolBuffer> const _bufferPtr;

    /// A value of this flag is sent to a client in a response message in case of
    /// a failure to indicate if the contribution could be retried. The flag is set
    /// to 'false' when irreversible changes to the content of the destination table
    /// are about to be made.
    bool _retryAllowed = true;

    /// The row and size counters of the object get updated as more data received
    /// from a client are being processed. The object gets synchronized with the database
    /// after finishing the ingest.
    TransactionContribInfo _contrib;

    /// The parse of the input stream as configured for the CSV dialect reported
    /// by a client.
    std::unique_ptr<csv::Parser> _parser;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTSVCCONN_H
