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
#ifndef LSST_QSERV_REPLICA_WORKERSERVERCONNECTION_H
#define LSST_QSERV_REPLICA_WORKERSERVERCONNECTION_H

// System headers
#include <atomic>
#include <memory>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/proto/protocol.pb.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ProtocolBuffer.h"
#include "replica/worker/WorkerProcessor.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerServerConnection is used for handling connections from
 * remote clients. One instance of the class serves one client at a time.
 *
 * Objects of this class are instantiated by WorkerServer. After that
 * the server calls this class's method beginProtocol() which starts
 * a series of asynchronous operations to communicate with remote client.
 * When all details of an incoming request are obtained from the client
 * the connection object forwards this request for actual processing
 * to an instance of the WorkerProcessor class. A response resieved from
 * the processor is serialized and sent back (asynchronously) to
 * the client.
 */
class WorkerServerConnection : public std::enable_shared_from_this<WorkerServerConnection> {
public:
    typedef std::shared_ptr<WorkerServerConnection> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider A provider is needed to access the Configuration of a setup.
     * @param processor A processor for long (queued) requests.
     * @param io_service An endpoint for network I/O, timers, etc.
     *
     * @return A pointer to the new object created by the factory.
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, WorkerProcessor::Ptr const& processor,
                      boost::asio::io_service& io_service);

    WorkerServerConnection() = delete;
    WorkerServerConnection(WorkerServerConnection const&) = delete;
    WorkerServerConnection& operator=(WorkerServerConnection const&) = delete;

    /// Non-default destructor is needed for the purposes of logging the connection
    /// termination event in the log stream.
    ~WorkerServerConnection();

    /// @return network socket associated with the connection
    boost::asio::ip::tcp::socket& socket() { return _socket; }

    /**
     * Begin communicating asynchronously with a client. This is essentially
     * an RPC protocol which runs in a loop this sequence of steps:
     *
     *   - ASYNC: read a frame header of a request
     *   -  SYNC: read the request header (request type, etc.)
     *   -  SYNC: read the request body (depends on a type of the request)
     *   - ASYNC: write a frame header of a reply to the request
     *            then write the reply itself
     *
     * @note A reason why the read phase is split into four steps is
     *   that a client is expected to send all components of the request
     *   (frame header, request header and request body) at once. This means
     *   the whole incoming message will be already available on the server's
     *   host memory when an asynchronous handler for the frame header will fire.
     *   However, due to a variable length of the request we should know its length
     *   before attempting to read the rest of the incoming message as this (the later)
     *   will require two things: 1) to ensure enough we have enough buffer space
     *   allocated, and 2) to tell the asynchronous reader function how many bytes
     *   exactly are we going to read.
     *
     * The chain ends when a client disconnects or when an error condition is met.
     */
    void beginProtocol();

private:
    /// @see WorkerServerConnection::create()
    WorkerServerConnection(ServiceProvider::Ptr const& serviceProvider, WorkerProcessor::Ptr const& processor,
                           boost::asio::io_service& io_service);

    /// @return A context string for error reporting and logging purposes.
    std::string const& context() const { return _context; }

    /**
     * Begin reading (asynchronously) the frame header of a new request
     *
     * The frame header is presently a 32-bit unsigned integer
     * representing the length of the subsequent message.
     */
    void _receive();

    /**
     * The callback on finishing (either successfully or not) of asynchronous reads.
     *
     * @param ec  А error condition to be checked for.
     * @param bytes_transferred  Тhe number of bytes received (if successful).
     */
    void _received(boost::system::error_code const& ec, size_t bytes_transferred);

    /**
     * Process queued requests (REPLICATE, DELETE, FIND, FIND-ALL, ECHO, etc.)
     *
     * @param hdr  А request header to be inspected.
     */
    void _processQueuedRequest(ProtocolRequestHeader const& hdr);

    /**
     * Process requests about replication requests (STOP, STATUS)
     *
     * @param hdr  A request header to be inspected.
     */
    void _processManagementRequest(ProtocolRequestHeader const& hdr);

    /**
     * Process requests affecting the service
     *
     * @param hdr  A request header to be inspected.
     */
    void _processServiceRequest(ProtocolRequestHeader const& hdr);

    /**
     * Serialize an identifier of a request into response header
     * followed by the Protobuf response body Protobuf object and
     * send it all back to a client.
     *
     * @param id  A unique identifier of a request to which the reply is sent.
     * @param body A body of the response.
     */
    template <class T>
    void _reply(std::string const& id, T&& body) {
        _bufferPtr->resize();
        ProtocolResponseHeader hdr;
        hdr.set_id(id);
        _bufferPtr->serialize(hdr);
        _bufferPtr->serialize(body);
        _send(id);
    }

    /**
     * Begin sending (asynchronously) a result back to a client
     * @param id  A unique identifier of a request to which the reply is sent
     *   (this parameter is passed into the for the logging and debugging purposes).
     */
    void _send(std::string const& id);

    /**
     * The callback on finishing (either successfully or not) of asynchronous writes.
     *
     * @param ec  A error condition to be checked for.
     * @param bytes_transferred  The number of bytes sent (if successful).
     */
    void _sent(boost::system::error_code const& ec, size_t bytes_transferred);

    /**
     * Verify if the name of a Qserv instance found in the request header matches
     * the one expected by the worker. If that's not the case then fill out
     * the response message with error codes explaining the problem.
     *
     * @note This method is compatible with responses sent for the queued messages.
     *
     * @param hdr  A request header message.
     * @param response  A response to be pre-filled with error codes in case if
     *   a mismatching instance found in the protocol header.
     *
     * @return 'true' if the instance found in the protocol header matches the one
     *   expected by the current worker.
     */
    template <class RESPONSE>
    bool _verifyInstance(ProtocolRequestHeader const& hdr, RESPONSE& response) const {
        if (hdr.instance_id() == _serviceProvider->instanceId()) return true;
        WorkerProcessor::setDefaultResponse(response, ProtocolStatus::BAD,
                                            ProtocolStatusExt::FOREIGN_INSTANCE);
        return false;
    }

    /// The specialized version of the above defined template method for responses
    /// to the requests disposals.
    bool _verifyInstance(ProtocolRequestHeader const& hdr, ProtocolResponseDispose& response) const {
        if (hdr.instance_id() == _serviceProvider->instanceId()) return true;
        response.set_status(ProtocolStatus::BAD);
        response.set_status_ext(ProtocolStatusExt::FOREIGN_INSTANCE);
        return false;
    }

    /// The specialized version of the above defined template method for responses
    /// to the worker services management requests.
    bool _verifyInstance(ProtocolRequestHeader const& hdr, ProtocolServiceResponse& response) const {
        if (hdr.instance_id() == _serviceProvider->instanceId()) return true;
        response.set_status(ProtocolStatus::BAD);
        response.set_status_ext(ProtocolStatusExt::FOREIGN_INSTANCE);
        return false;
    }

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;

    // Data strctures related to unique identifiers of connections.

    static std::atomic<unsigned int>
            _connectionIdSeries;       ///< The generator of unique connection identifiers.
    unsigned int const _connectionId;  ///< A unique identifier of the current connection.
    std::string const
            _context;  ///< A string for logging and error reporting (includes connection identifier).

    /// This is pointer onto an object where the requests would
    /// get processed.
    WorkerProcessor::Ptr _processor;

    boost::asio::ip::tcp::socket _socket;

    /// Buffer management class facilitating serialization/de-serialization
    /// of data sent over the network
    std::shared_ptr<ProtocolBuffer> _bufferPtr;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERSERVERCONNECTION_H
