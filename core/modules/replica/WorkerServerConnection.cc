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
#include "replica/WorkerServerConnection.h"

// System headers
#include <functional>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Performance.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerServerConnection");

} /// namespace

namespace {

/// The context for diagnostic & debug printouts
string const context = "CONNECTION  ";

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

    ptr->resize(bytes);     // make sure the buffer has enough space to accommodate
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
    return not ::isErrorCode(ec, __func__);
}


template <class T>
bool readMessage(boost::asio::ip::tcp::socket& socket,
                 shared_ptr<ProtocolBuffer> const& ptr,
                 size_t bytes,
                 T& message) {

    if (not readIntoBuffer(socket,
                           ptr,
                           bytes)) {
        return false;
    }

    // Parse the response to see what should be done next.

    ptr->parse(message, bytes);
    return true;
}


bool readLength(boost::asio::ip::tcp::socket& socket,
                shared_ptr<ProtocolBuffer> const& ptr,
                uint32_t& bytes) {

    if (not readIntoBuffer(socket,
                           ptr,
                           sizeof(uint32_t))) {
        return false;
    }
    bytes = ptr->parseLength();
    return true;
}
}   // namespace

namespace lsst {
namespace qserv {
namespace replica {

WorkerServerConnection::Ptr WorkerServerConnection::create(
                                    ServiceProvider::Ptr const& serviceProvider,
                                    WorkerProcessor::Ptr const& processor,
                                    boost::asio::io_service& io_service) {
    return WorkerServerConnection::Ptr(
        new WorkerServerConnection(
            serviceProvider,
            processor,
            io_service));
}


WorkerServerConnection::WorkerServerConnection(ServiceProvider::Ptr const& serviceProvider,
                                               WorkerProcessor::Ptr const& processor,
                                               boost::asio::io_service& io_service)
    :   _serviceProvider(serviceProvider),
        _processor(processor),
        _socket(io_service),
        _bufferPtr(make_shared<ProtocolBuffer>(
                       serviceProvider->config()->requestBufferSizeBytes())) {
}


void WorkerServerConnection::beginProtocol() {
    _receive();
}


void WorkerServerConnection::_receive() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whole message (its frame and
    // the message itself) at once.

    const size_t bytes = sizeof(uint32_t);

    _bufferPtr->resize(bytes);

    boost::asio::async_read(
        _socket,
        boost::asio::buffer(
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        bind(&WorkerServerConnection::_received, shared_from_this(), _1, _2)
    );
}


void WorkerServerConnection::_received(boost::system::error_code const& ec,
                                       size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    if (::isErrorCode(ec, __func__)) return;

    // Now read the request header
    ProtocolRequestHeader hdr;
    if (not ::readMessage(_socket, _bufferPtr, _bufferPtr->parseLength(), hdr)) return;


    // Analyze the header of the request. Note that the header message categorizes
    // requests in two layers:
    // - first goes the class of requests as defined by member 'type'
    // - then  goes a choice of a specific request within its class. Those specific
    //   request codes are obtained from the corresponding members

    switch (hdr.type()) {

        case ProtocolRequestHeader::QUEUED:  _processQueuedRequest(    hdr); break;
        case ProtocolRequestHeader::REQUEST: _processManagementRequest(hdr); break;
        case ProtocolRequestHeader::SERVICE: _processServiceRequest(   hdr); break;

        default:
            throw logic_error(
                    "WorkerServerConnection::" + string(__func__) + " unhandled request class: '" +
                    ProtocolRequestHeader::RequestType_Name(hdr.type()));
    }
}


void WorkerServerConnection::_processQueuedRequest(ProtocolRequestHeader& hdr) {

    // Read the request length
    uint32_t bytes;
    if (not ::readLength(_socket, _bufferPtr, bytes)) return;

    switch (hdr.queued_type()) {

        case ProtocolQueuedRequestType::REPLICA_CREATE: {

            // Read the request body
            ProtocolRequestReplicate request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseReplicate response;
            _processor->enqueueForReplication(hdr.id(), request, response);
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::REPLICA_DELETE: {

            // Read the request body
            ProtocolRequestDelete request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseDelete response;
            _processor->enqueueForDeletion(hdr.id(), request, response);
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::REPLICA_FIND: {

            // Read the request body
            ProtocolRequestFind request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseFind response;
            _processor->enqueueForFind(hdr.id(), request, response);
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::REPLICA_FIND_ALL: {

            // Read the request body
            ProtocolRequestFindAll request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseFindAll response;
            _processor->enqueueForFindAll(hdr.id(), request, response);
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::TEST_ECHO: {

            // Read the request body
            ProtocolRequestEcho request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseEcho response;
            _processor->enqueueForEcho(hdr.id(), request, response);
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::INDEX: {

            // Read the request body
            ProtocolRequestIndex request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseIndex response;
            _processor->enqueueForIndex(hdr.id(), request, response);
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::SQL: {

            // Read the request body
            ProtocolRequestSql request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseSql response;
            _processor->enqueueForSql(hdr.id(), request, response);
            _reply(hdr.id(), response);
            break;
        }
        default:
            throw logic_error(
                    "WorkerServerConnection::" + string(__func__) + "  unhandled request type: '" +
                    ProtocolQueuedRequestType_Name(hdr.queued_type()));
    }
}


void WorkerServerConnection::_processManagementRequest(ProtocolRequestHeader& hdr) {

    // Read the request length
    uint32_t bytes;
    if (not ::readLength(_socket,
                         _bufferPtr,
                         bytes)) {
        return;
    }
    switch (hdr.management_type()) {

        case ProtocolManagementRequestType::REQUEST_STOP: {

            // Read the request body
            ProtocolRequestStop request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            switch (request.queued_type()) {

                case ProtocolQueuedRequestType::REPLICA_CREATE: {
                    ProtocolResponseReplicate response;
                    _processor->dequeueOrCancel(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_DELETE: {
                    ProtocolResponseDelete response;
                    _processor->dequeueOrCancel(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_FIND: {
                    ProtocolResponseFind response;
                    _processor->dequeueOrCancel(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_FIND_ALL: {
                    ProtocolResponseFindAll response;
                    _processor->dequeueOrCancel(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::TEST_ECHO: {
                    ProtocolResponseEcho response;
                    _processor->dequeueOrCancel(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::INDEX: {
                    ProtocolResponseIndex response;
                    _processor->dequeueOrCancel(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::SQL: {
                    ProtocolResponseSql response;
                    _processor->dequeueOrCancel(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                default:
                    throw logic_error(
                            "WorkerServerConnection::" + string(__func__) + "  unhandled request type: '" +
                            ProtocolQueuedRequestType_Name(request.queued_type()));
            }
            break;
        }
        case ProtocolManagementRequestType::REQUEST_STATUS: {

            // Read the request body
            ProtocolRequestStatus request;
            if (not ::readMessage(_socket, _bufferPtr, bytes, request)) return;

            switch (request.queued_type()) {

                case ProtocolQueuedRequestType::REPLICA_CREATE: {
                    ProtocolResponseReplicate response;
                    _processor->checkStatus(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_DELETE: {
                    ProtocolResponseDelete response;
                    _processor->checkStatus(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_FIND: {
                    ProtocolResponseFind response;
                    _processor->checkStatus(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_FIND_ALL: {
                    ProtocolResponseFindAll response;
                    _processor->checkStatus(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::TEST_ECHO: {
                    ProtocolResponseEcho response;
                    _processor->checkStatus(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::INDEX: {
                    ProtocolResponseIndex response;
                    _processor->checkStatus(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::SQL: {
                    ProtocolResponseSql response;
                    _processor->checkStatus(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                default:
                    throw logic_error(
                            "WorkerServerConnection::" + string(__func__) + "  unhandled request type: '" +
                            ProtocolQueuedRequestType_Name(request.queued_type()));
            }
            break;
        }
        default:
            throw logic_error(
                    "WorkerServerConnection::" + string(__func__) + "  unhandled request type: '" +
                    ProtocolManagementRequestType_Name(hdr.management_type()));
    }
}


void WorkerServerConnection::_processServiceRequest(ProtocolRequestHeader& hdr) {

    ProtocolServiceResponse response;

    // All performance counters for this type of requests should be
    // equal because this is the instantaneous request

    WorkerPerformance performance;
    performance.setUpdateStart();
    performance.setUpdateFinish();
    response.set_allocated_performance(performance.info().release());

    switch (hdr.service_type()) {

        case ProtocolServiceRequestType::SERVICE_SUSPEND: {

            // This operation is allowed to be asynchronous as it may take
            // extra time for the processor's threads to finish on-going processing

            _processor->stop();
            _processor->setServiceResponse(
                  response,
                  hdr.id(),
                  _processor->state() == WorkerProcessor::State::STATE_IS_RUNNING ?
                      ProtocolServiceResponse::FAILED :
                      ProtocolServiceResponse::SUCCESS
            );
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolServiceRequestType::SERVICE_RESUME: {

            // This is a synchronous operation. The state transition request should happen
            // (or be denied) instantaneously.

            _processor->run();
            _processor->setServiceResponse(
                  response,
                  hdr.id(),
                  _processor->state() == WorkerProcessor::State::STATE_IS_RUNNING ?
                      ProtocolServiceResponse::SUCCESS :
                      ProtocolServiceResponse::FAILED);

            _reply(hdr.id(), response);
            break;
        }
        case ProtocolServiceRequestType::SERVICE_STATUS: {

            _processor->setServiceResponse(
                  response,
                  hdr.id(),
                  ProtocolServiceResponse::SUCCESS);

            _reply(hdr.id(), response);
            break;
        }
        case ProtocolServiceRequestType::SERVICE_REQUESTS: {

            const bool extendedReport = true;   // to return detailed info on all known
                                                // replica-related requests
            _processor->setServiceResponse(
                  response,
                  hdr.id(),
                  ProtocolServiceResponse::SUCCESS,
                  extendedReport);

            _reply(hdr.id(), response);
            break;
        }
        case ProtocolServiceRequestType::SERVICE_DRAIN: {

            _processor->drain();

            const bool extendedReport = true;   // to return detailed info on all known
                                                // replica-related requests
            _processor->setServiceResponse(
                  response,
                  hdr.id(),
                  ProtocolServiceResponse::SUCCESS,
                  extendedReport);

            _reply(hdr.id(), response);
            break;
        }
        case ProtocolServiceRequestType::SERVICE_RECONFIG: {

            _processor->reconfig();

            const bool extendedReport = true;   // to return detailed info on all known
                                                // replica-related requests
            _processor->setServiceResponse(
                  response,
                  hdr.id(),
                  ProtocolServiceResponse::SUCCESS,
                  extendedReport);

            _reply(hdr.id(), response);
            break;
        }
        default:
            throw logic_error(
                    "WorkerServerConnection::" + string(__func__) + "  unhandled request type: '" +
                    ProtocolServiceRequestType_Name(hdr.service_type()));
    }
}


void WorkerServerConnection::_send() {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);

    boost::asio::async_write(
        _socket,
        boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
        bind(&WorkerServerConnection::_sent, shared_from_this(), _1, _2)
    );
}


void WorkerServerConnection::_sent(boost::system::error_code const& ec,
                                   size_t bytes_transferred) {

    LOGS(_log, LOG_LVL_DEBUG, context << __func__);
    if (::isErrorCode(ec, __func__)) return;

    // Go wait for another request
    _receive();
}

}}} // namespace lsst::qserv::replica
