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
#include "replica/worker/WorkerServerConnection.h"

// System headers
#include <functional>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/util/Performance.h"
#include "replica/util/ProtocolBuffer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;
using namespace lsst::qserv::replica;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerServerConnection");
}  // namespace

namespace {

bool isErrorCode(string const& context, boost::system::error_code const& ec, string const& scope) {
    if (ec.value() != 0) {
        if (ec == boost::asio::error::eof) {
            LOGS(_log, LOG_LVL_DEBUG, context << scope << "  ** CLOSED **");
        } else {
            LOGS(_log, LOG_LVL_ERROR, context << scope << "  ** FAILED ec=" << ec.value() << " **");
        }
        return true;
    }
    return false;
}

bool readIntoBuffer(string const& context, boost::asio::ip::tcp::socket& socket,
                    shared_ptr<ProtocolBuffer> const& ptr, size_t bytes) {
    ptr->resize(bytes);  // make sure the buffer has enough space to accommodate
                         // the data of the message.
    boost::system::error_code ec;
    boost::asio::read(socket, boost::asio::buffer(ptr->data(), bytes), boost::asio::transfer_at_least(bytes),
                      ec);
    return not ::isErrorCode(context, ec, __func__);
}

template <class T>
bool readMessage(string const& context, boost::asio::ip::tcp::socket& socket,
                 shared_ptr<ProtocolBuffer> const& ptr, size_t bytes, T& message) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__ << " bytes=" << bytes);
    try {
        if (readIntoBuffer(context, socket, ptr, bytes)) {
            ptr->parse(message, bytes);
            return true;
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << __func__ << " " << ex.what());
    }
    return false;
}

bool readLength(string const& context, boost::asio::ip::tcp::socket& socket,
                shared_ptr<ProtocolBuffer> const& ptr, uint32_t& bytes) {
    LOGS(_log, LOG_LVL_DEBUG, context << __func__);
    try {
        if (readIntoBuffer(context, socket, ptr, sizeof(uint32_t))) {
            bytes = ptr->parseLength();
            return true;
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << __func__ << " " << ex.what());
    }
    return false;
}
}  // namespace

namespace lsst::qserv::replica {

atomic<unsigned int> WorkerServerConnection::_connectionIdSeries{0};

WorkerServerConnection::Ptr WorkerServerConnection::create(ServiceProvider::Ptr const& serviceProvider,
                                                           WorkerProcessor::Ptr const& processor,
                                                           boost::asio::io_service& io_service) {
    return WorkerServerConnection::Ptr(new WorkerServerConnection(serviceProvider, processor, io_service));
}

WorkerServerConnection::WorkerServerConnection(ServiceProvider::Ptr const& serviceProvider,
                                               WorkerProcessor::Ptr const& processor,
                                               boost::asio::io_service& io_service)
        : _serviceProvider(serviceProvider),
          _connectionId(_connectionIdSeries++),
          _context("WORKER-SERVER-CONNECTION[" + to_string(_connectionId) + "]  "),
          _processor(processor),
          _socket(io_service),
          _bufferPtr(make_shared<ProtocolBuffer>(
                  serviceProvider->config()->get<size_t>("common", "request-buf-size-bytes"))) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << " CREATED");
}

WorkerServerConnection::~WorkerServerConnection() {
    boost::system::error_code ec;
    _socket.cancel(ec);
    _socket.close(ec);
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << " DELETED");
}

void WorkerServerConnection::beginProtocol() { _receive(); }

void WorkerServerConnection::_receive() {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Start with receiving the fixed length frame carrying
    // the size (in bytes) the length of the subsequent message.
    //
    // The message itself will be read from the handler using
    // the synchronous read method. This is based on an assumption
    // that the worker server sends the whole message (its frame and
    // the message itself) at once.
    const size_t bytes = sizeof(uint32_t);
    _bufferPtr->resize(bytes);
    boost::asio::async_read(_socket, boost::asio::buffer(_bufferPtr->data(), bytes),
                            boost::asio::transfer_at_least(bytes),
                            bind(&WorkerServerConnection::_received, shared_from_this(), _1, _2));
}

void WorkerServerConnection::_received(boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << " ec=" << ec.value() << " bytes_transferred=" << bytes_transferred);
    if (::isErrorCode(context(), ec, __func__)) return;

    // Now read the request header
    ProtocolRequestHeader hdr;
    if (not ::readMessage(context(), _socket, _bufferPtr, _bufferPtr->parseLength(), hdr)) return;

    // Analyze the header of the request. Note that the header message categorizes
    // requests in two layers:
    // - first goes the class of requests as defined by member 'type'
    // - then  goes a choice of a specific request within its class. Those specific
    //   request codes are obtained from the corresponding members
    switch (hdr.type()) {
        case ProtocolRequestHeader::QUEUED:
            _processQueuedRequest(hdr);
            break;
        case ProtocolRequestHeader::REQUEST:
            _processManagementRequest(hdr);
            break;
        case ProtocolRequestHeader::SERVICE:
            _processServiceRequest(hdr);
            break;
        default:
            throw logic_error("WorkerServerConnection::" + string(__func__) + " unhandled request class: '" +
                              ProtocolRequestHeader::RequestType_Name(hdr.type()));
    }
}

void WorkerServerConnection::_processQueuedRequest(ProtocolRequestHeader const& hdr) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << " id=" << hdr.id()
                   << " type=" << ProtocolQueuedRequestType_Name(hdr.queued_type()));

    // Read the request length
    uint32_t bytes;
    if (not ::readLength(context(), _socket, _bufferPtr, bytes)) return;

    switch (hdr.queued_type()) {
        case ProtocolQueuedRequestType::REPLICA_CREATE: {
            // Read the request body
            ProtocolRequestReplicate request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseReplicate response;
            if (_verifyInstance(hdr, response)) {
                _processor->enqueueForReplication(hdr.id(), hdr.priority(), hdr.timeout(), request, response);
            }
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::REPLICA_DELETE: {
            // Read the request body
            ProtocolRequestDelete request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseDelete response;
            if (_verifyInstance(hdr, response)) {
                _processor->enqueueForDeletion(hdr.id(), hdr.priority(), hdr.timeout(), request, response);
            }
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::REPLICA_FIND: {
            // Read the request body
            ProtocolRequestFind request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseFind response;
            if (_verifyInstance(hdr, response)) {
                _processor->enqueueForFind(hdr.id(), hdr.priority(), hdr.timeout(), request, response);
            }
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::REPLICA_FIND_ALL: {
            // Read the request body
            ProtocolRequestFindAll request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseFindAll response;
            if (_verifyInstance(hdr, response)) {
                _processor->enqueueForFindAll(hdr.id(), hdr.priority(), hdr.timeout(), request, response);
            }
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::TEST_ECHO: {
            // Read the request body
            ProtocolRequestEcho request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseEcho response;
            if (_verifyInstance(hdr, response)) {
                _processor->enqueueForEcho(hdr.id(), hdr.priority(), hdr.timeout(), request, response);
            }
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::INDEX: {
            // Read the request body
            ProtocolRequestDirectorIndex request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseDirectorIndex response;
            if (_verifyInstance(hdr, response)) {
                _processor->enqueueForDirectorIndex(hdr.id(), hdr.priority(), hdr.timeout(), request,
                                                    response);
            }
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolQueuedRequestType::SQL: {
            // Read the request body
            ProtocolRequestSql request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;

            ProtocolResponseSql response;
            if (_verifyInstance(hdr, response)) {
                _processor->enqueueForSql(hdr.id(), hdr.priority(), hdr.timeout(), request, response);
            }
            _reply(hdr.id(), response);
            break;
        }
        default:
            throw logic_error("WorkerServerConnection::" + string(__func__) + "  unhandled request type: '" +
                              ProtocolQueuedRequestType_Name(hdr.queued_type()));
    }
}

void WorkerServerConnection::_processManagementRequest(ProtocolRequestHeader const& hdr) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << " id=" << hdr.id()
                   << " type=" << ProtocolManagementRequestType_Name(hdr.management_type()));

    // Read the request length
    uint32_t bytes;
    if (not ::readLength(context(), _socket, _bufferPtr, bytes)) {
        return;
    }
    switch (hdr.management_type()) {
        case ProtocolManagementRequestType::REQUEST_STATUS: {
            // Read the request body
            ProtocolRequestStatus request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;
            ProtocolResponseStatus response;
            WorkerPerformance performance;
            performance.setUpdateStart();
            if (_verifyInstance(hdr, response)) _processor->checkStatus(request, response);
            performance.setUpdateFinish();
            response.set_allocated_performance(performance.info().release());
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolManagementRequestType::REQUEST_STOP: {
            // Read the request body
            ProtocolRequestStop request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;
            ProtocolResponseStop response;
            WorkerPerformance performance;
            performance.setUpdateStart();
            if (_verifyInstance(hdr, response)) _processor->dequeueOrCancel(request, response);
            performance.setUpdateFinish();
            response.set_allocated_performance(performance.info().release());
            _reply(hdr.id(), response);
            break;
        }
        case ProtocolManagementRequestType::REQUEST_TRACK: {
            // Read the request body
            ProtocolRequestTrack request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;
            switch (request.queued_type()) {
                case ProtocolQueuedRequestType::REPLICA_CREATE: {
                    ProtocolResponseReplicate response;
                    if (_verifyInstance(hdr, response)) _processor->trackRequest(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_DELETE: {
                    ProtocolResponseDelete response;
                    if (_verifyInstance(hdr, response)) _processor->trackRequest(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_FIND: {
                    ProtocolResponseFind response;
                    if (_verifyInstance(hdr, response)) _processor->trackRequest(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::REPLICA_FIND_ALL: {
                    ProtocolResponseFindAll response;
                    if (_verifyInstance(hdr, response)) _processor->trackRequest(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::TEST_ECHO: {
                    ProtocolResponseEcho response;
                    if (_verifyInstance(hdr, response)) _processor->trackRequest(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::INDEX: {
                    ProtocolResponseDirectorIndex response;
                    if (_verifyInstance(hdr, response)) _processor->trackRequest(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                case ProtocolQueuedRequestType::SQL: {
                    ProtocolResponseSql response;
                    if (_verifyInstance(hdr, response)) _processor->trackRequest(request, response);
                    _reply(hdr.id(), response);
                    break;
                }
                default:
                    throw logic_error("WorkerServerConnection::" + string(__func__) +
                                      "  unhandled request type: '" +
                                      ProtocolQueuedRequestType_Name(request.queued_type()));
            }
            break;
        }
        case ProtocolManagementRequestType::REQUEST_DISPOSE: {
            // Read the request body
            ProtocolRequestDispose request;
            if (not ::readMessage(context(), _socket, _bufferPtr, bytes, request)) return;
            ProtocolResponseDispose response;
            if (_verifyInstance(hdr, response)) {
                for (int i = 0; i < request.ids_size(); ++i) {
                    string const id = request.ids(i);
                    auto ptr = response.add_ids();
                    ptr->set_id(id);
                    ptr->set_disposed(_processor->dispose(id));
                }
            }
            _reply(hdr.id(), response);
            break;
        }
        default:
            throw logic_error("WorkerServerConnection::" + string(__func__) + "  unhandled request type: '" +
                              ProtocolManagementRequestType_Name(hdr.management_type()));
    }
}

void WorkerServerConnection::_processServiceRequest(ProtocolRequestHeader const& hdr) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << " id=" << hdr.id()
                   << " type=" << ProtocolServiceRequestType_Name(hdr.service_type()));

    ProtocolServiceResponse response;

    // All performance counters for this type of requests should be
    // equal because this is the instantaneous request
    WorkerPerformance performance;
    performance.setUpdateStart();
    performance.setUpdateFinish();
    response.set_allocated_performance(performance.info().release());
    if (_verifyInstance(hdr, response)) {
        switch (hdr.service_type()) {
            case ProtocolServiceRequestType::SERVICE_SUSPEND: {
                // This operation is allowed to be asynchronous as it may take
                // extra time for the processor's threads to finish on-going processing
                _processor->stop();
                _processor->setServiceResponse(response, hdr.id(),
                                               _processor->state() == WorkerProcessor::State::STATE_IS_RUNNING
                                                       ? ProtocolStatus::FAILED
                                                       : ProtocolStatus::SUCCESS);
                break;
            }
            case ProtocolServiceRequestType::SERVICE_RESUME: {
                // This is a synchronous operation. The state transition request should happen
                // (or be denied) instantaneously.
                _processor->run();
                _processor->setServiceResponse(response, hdr.id(),
                                               _processor->state() == WorkerProcessor::State::STATE_IS_RUNNING
                                                       ? ProtocolStatus::SUCCESS
                                                       : ProtocolStatus::FAILED);
                break;
            }
            case ProtocolServiceRequestType::SERVICE_STATUS: {
                _processor->setServiceResponse(response, hdr.id(), ProtocolStatus::SUCCESS);
                break;
            }
            case ProtocolServiceRequestType::SERVICE_REQUESTS: {
                const bool extendedReport = true;  // to return detailed info on all known
                                                   // replica-related requests
                _processor->setServiceResponse(response, hdr.id(), ProtocolStatus::SUCCESS, extendedReport);
                break;
            }
            case ProtocolServiceRequestType::SERVICE_DRAIN: {
                const bool extendedReport = true;  // to return detailed info on all known
                                                   // replica-related requests
                _processor->drain();
                _processor->setServiceResponse(response, hdr.id(), ProtocolStatus::SUCCESS, extendedReport);
                break;
            }
            case ProtocolServiceRequestType::SERVICE_RECONFIG: {
                const bool extendedReport = true;  // to return detailed info on all known
                                                   // replica-related requests
                _processor->reconfig();
                _processor->setServiceResponse(response, hdr.id(), ProtocolStatus::SUCCESS, extendedReport);
                break;
            }
            default:
                throw logic_error("WorkerServerConnection::" + string(__func__) +
                                  "  unhandled request type: '" +
                                  ProtocolServiceRequestType_Name(hdr.service_type()));
        }
    }
    _reply(hdr.id(), response);
}

void WorkerServerConnection::_send(string const& id) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << " id=" << id << " size=" << _bufferPtr->size());

    boost::asio::async_write(_socket, boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()),
                             bind(&WorkerServerConnection::_sent, shared_from_this(), _1, _2));
}

void WorkerServerConnection::_sent(boost::system::error_code const& ec, size_t bytes_transferred) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << " ec=" << ec.value() << " bytes_transferred=" << bytes_transferred);
    if (::isErrorCode(context(), ec, __func__)) return;

    // Go wait for another request
    _receive();
}

}  // namespace lsst::qserv::replica
