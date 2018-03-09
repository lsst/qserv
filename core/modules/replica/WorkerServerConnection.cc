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
#include "replica/WorkerServerConnection.h"

// System headers

// Third party headers
#include <boost/bind.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerProcessor.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerServerConnection");

} /// namespace

namespace {
    
using ProtocolBufferPtr = std::shared_ptr<lsst::qserv::replica::ProtocolBuffer>;

/// The context for diagnostic & debug printouts
std::string const context = "CONNECTION  ";

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

bool readLength(boost::asio::ip::tcp::socket& socket,
                ProtocolBufferPtr const& ptr,
                uint32_t& bytes) {

    if (not readIntoBuffer(socket,
                           ptr,
                           sizeof(uint32_t))) return false;
    
    bytes = ptr->parseLength();
    return true;
} 
}   // namespace

namespace lsst {
namespace qserv {
namespace replica {

WorkerServerConnection::pointer WorkerServerConnection::create(
                                    ServiceProvider::pointer const& serviceProvider,
                                    WorkerProcessor& processor,
                                    boost::asio::io_service& io_service) {
    return WorkerServerConnection::pointer(
        new WorkerServerConnection(
            serviceProvider,
            processor,
            io_service));
}

WorkerServerConnection::WorkerServerConnection(ServiceProvider::pointer const& serviceProvider,
                                               WorkerProcessor& processor,
                                               boost::asio::io_service& io_service)
    :   _serviceProvider(serviceProvider),
        _processor(processor),
        _socket(io_service),
        _bufferPtr(std::make_shared<ProtocolBuffer>(
                       serviceProvider->config()->requestBufferSizeBytes())) {
}

void WorkerServerConnection::beginProtocol() {
    receive();
}

void WorkerServerConnection::receive() {

    LOGS(_log, LOG_LVL_DEBUG, context << "receive");

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whol emessage (its frame and
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
            &WorkerServerConnection::received,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void WorkerServerConnection::received(boost::system::error_code const& ec,
                                      size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << "received");

    if (::isErrorCode(ec, "received")) { return; }

    // Now read the request header

    proto::ReplicationRequestHeader hdr;
    if (not ::readMessage(_socket,
                          _bufferPtr,
                          _bufferPtr->parseLength(),
                          hdr)) { return; }

    // Analyse the header of the request. Note that the header message categorizes
    // requests in two layers:
    // - first goes the class of requests as defined by member 'type'
    // - then  goes a choice of a specific request witin its class. Those specific
    //   request codes are obtained from the corresponding members

    switch (hdr.type()) {

        case proto::ReplicationRequestHeader::REPLICA: processReplicaRequest(   hdr); break;
        case proto::ReplicationRequestHeader::REQUEST: processManagementRequest(hdr); break;
        case proto::ReplicationRequestHeader::SERVICE: processServiceRequest(   hdr); break;
 
        default:
            throw std::logic_error(
                  "WorkerServerConnection::received() unhandled request class: '" +
                  proto::ReplicationRequestHeader::RequestType_Name(hdr.type()));
    }
}

void WorkerServerConnection::processReplicaRequest(proto::ReplicationRequestHeader& hdr) {

    // Read the request length
    uint32_t bytes;
    if (not ::readLength (_socket,
                          _bufferPtr,
                          bytes)) { return; }

    switch (hdr.replica_type()) {

        case proto::ReplicationReplicaRequestType::REPLICA_CREATE: {

            // Read the request body
            proto::ReplicationRequestReplicate request;
            if (not ::readMessage(_socket,
                                  _bufferPtr,
                                  bytes,
                                  request)) { return; }

            proto::ReplicationResponseReplicate response;
            _processor.enqueueForReplication(hdr.id(),
                                             request,
                                             response);
            reply(hdr.id(),
                  response);

            break;
        }
        case proto::ReplicationReplicaRequestType::REPLICA_DELETE: {

            // Read the request body
            proto::ReplicationRequestDelete request;
            if (not ::readMessage(_socket,
                                  _bufferPtr,
                                  bytes,
                                  request)) { return; }

            proto::ReplicationResponseDelete response;
            _processor.enqueueForDeletion(hdr.id(),
                                          request,
                                          response);
            reply(hdr.id(),
                  response);

            break;
        }
        case proto::ReplicationReplicaRequestType::REPLICA_FIND: {

            // Read the request body
            proto::ReplicationRequestFind request;
            if (not ::readMessage(_socket,
                                  _bufferPtr,
                                  bytes,
                                  request)) { return; }

            proto::ReplicationResponseFind response;
            _processor.enqueueForFind(hdr.id(),
                                      request,
                                      response);
            reply(hdr.id(),
                  response);

            break;
        }
        case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL: {

            // Read the request body
            proto::ReplicationRequestFindAll request;
            if (not ::readMessage(_socket,
                                  _bufferPtr,
                                  bytes,
                                  request)) { return; }

            proto::ReplicationResponseFindAll response;
            _processor.enqueueForFindAll(hdr.id(),
                                         request,
                                         response);
            reply(hdr.id(),
                  response);

            break;
        }
        default:
            throw std::logic_error(
                  "WorkerServerConnection::processReplicaRequest() unhandled request type: '" +
                  proto::ReplicationReplicaRequestType_Name(hdr.replica_type()));
    }
}

void WorkerServerConnection::processManagementRequest(proto::ReplicationRequestHeader& hdr) {

    // Read the request length
    uint32_t bytes;
    if (not ::readLength (_socket,
                          _bufferPtr,
                          bytes)) { return; }

    switch (hdr.management_type()) {

        case proto::ReplicationManagementRequestType::REQUEST_STOP: {

            // Read the request body
            proto::ReplicationRequestStop request;
            if (not ::readMessage(_socket,
                                  _bufferPtr,
                                  bytes,
                                  request)) { return; }

            switch (request.type()) {

                case proto::ReplicationReplicaRequestType::REPLICA_CREATE: {
                    proto::ReplicationResponseReplicate response;
                    _processor.dequeueOrCancel(hdr.id(),
                                               request,
                                               response);
                    reply(hdr.id(),
                          response);
                    break;
                }

                case proto::ReplicationReplicaRequestType::REPLICA_DELETE: {
                    proto::ReplicationResponseDelete response;
                    _processor.dequeueOrCancel(hdr.id(),
                                               request,
                                               response);
                    reply(hdr.id(),
                          response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_FIND: {
                    proto::ReplicationResponseFind response;
                    _processor.dequeueOrCancel(hdr.id(),
                                               request,
                                               response);
                    reply(hdr.id(),
                          response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL: {
                    proto::ReplicationResponseFindAll response;
                    _processor.dequeueOrCancel(hdr.id(),
                                               request,
                                               response);
                    reply(hdr.id(),
                          response);
                    break;
                }
            }
            break;
        }
        case proto::ReplicationManagementRequestType::REQUEST_STATUS : {

            // Read the request body
            proto::ReplicationRequestStatus request;
            if (not ::readMessage(_socket,
                                  _bufferPtr,
                                  bytes,
                                  request)) { return; }

            switch (request.type()) {

                case proto::ReplicationReplicaRequestType::REPLICA_CREATE: {
                    proto::ReplicationResponseReplicate response;
                    _processor.checkStatus(hdr.id(),
                                           request,
                                           response);
                    reply(hdr.id(),
                          response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_DELETE: {
                    proto::ReplicationResponseDelete response;
                    _processor.checkStatus(hdr.id(),
                                           request,
                                           response);
                    reply(hdr.id(),
                          response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_FIND: {
                    proto::ReplicationResponseFind response;
                    _processor.checkStatus(hdr.id(),
                                           request,
                                           response);
                    reply(hdr.id(),
                          response);
                    break;
                }
                case proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL: {
                    proto::ReplicationResponseFindAll response;
                    _processor.checkStatus(hdr.id(),
                                           request,
                                           response);
                    reply(hdr.id(),
                          response);
                    break;
                }
            }
            break;
        }
        default:
            throw std::logic_error(
                  "WorkerServerConnection::processManagementRequest() unhandled request type: '" +
                  proto::ReplicationManagementRequestType_Name(hdr.management_type()));
    }
}

void WorkerServerConnection::processServiceRequest(proto::ReplicationRequestHeader& hdr) {

    proto::ReplicationServiceResponse response;

    // All performance counters for this type of requests should be
    // equal because this is the instantenous request

    WorkerPerformance performance;
    performance.setUpdateStart();
    performance.setUpdateFinish();
    response.set_allocated_performance(performance.info());

    switch (hdr.service_type()) {

        case proto::ReplicationServiceRequestType::SERVICE_SUSPEND: {

            // This operation is allowed to be asynchronious as it may take
            // extra time for the processor's threads to finish on-going processing

            _processor.stop();
            _processor.setServiceResponse(
                  response,
                  hdr.id(),
                  _processor.state() == WorkerProcessor::State::STATE_IS_RUNNING ?
                      proto::ReplicationServiceResponse::FAILED :
                      proto::ReplicationServiceResponse::SUCCESS);

            reply(hdr.id(),
                  response);
            break;
        }
        case proto::ReplicationServiceRequestType::SERVICE_RESUME: {
  
            // This is a synchronus operation. The state transition request should happen
            // (or be denied) instantaneously.
      
            _processor.run();
            _processor.setServiceResponse(
                  response,
                  hdr.id(),
                  _processor.state() == WorkerProcessor::State::STATE_IS_RUNNING ?
                      proto::ReplicationServiceResponse::SUCCESS :
                      proto::ReplicationServiceResponse::FAILED);

            reply(hdr.id(),
                  response);
            break;
        }
        case proto::ReplicationServiceRequestType::SERVICE_STATUS: {

            _processor.setServiceResponse(
                  response,
                  hdr.id(),
                  proto::ReplicationServiceResponse::SUCCESS);

            reply(hdr.id(),
                  response);
            break;
        }
        case proto::ReplicationServiceRequestType::SERVICE_REQUESTS: {

            const bool extendedReport = true;   // to return detailed info on all known
                                                // replica-related requests
            _processor.setServiceResponse(
                  response,
                  hdr.id(),
                  proto::ReplicationServiceResponse::SUCCESS,
                  extendedReport);

            reply(hdr.id(),
                  response);
            break;
        }
        case proto::ReplicationServiceRequestType::SERVICE_DRAIN: {

            _processor.drain();

            const bool extendedReport = true;   // to return detailed info on all known
                                                // replica-related requests
            _processor.setServiceResponse(
                  response,
                  hdr.id(),
                  proto::ReplicationServiceResponse::SUCCESS,
                  extendedReport);

            reply(hdr.id(),
                  response);
            break;
        }
        default:
            throw std::logic_error(
                  "WorkerServerConnection::processServiceRequest() unhandled request type: '" +
                  proto::ReplicationServiceRequestType_Name(hdr.service_type()));
    }
}

void WorkerServerConnection::send() {

    LOGS(_log, LOG_LVL_DEBUG, context << "send");

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(
            _bufferPtr->data(),
            _bufferPtr->size()
        ),
        boost::bind(
            &WorkerServerConnection::sent,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred
        )
    );
}

void WorkerServerConnection::sent(boost::system::error_code const& ec,
                                  size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << "sent");

    if (::isErrorCode(ec, "sent")) { return; }

    // Go wait for another request

    receive();
}

}}} // namespace lsst::qserv::replica