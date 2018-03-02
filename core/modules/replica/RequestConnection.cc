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
#include "replica/RequestConnection.h"

// System headers
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.RequestConnection");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

RequestConnection::RequestConnection (ServiceProvider&         serviceProvider,
                                      boost::asio::io_service& io_service,
                                      std::string const&       type,
                                      std::string const&       worker,
                                      int                      priority,
                                      bool                     keepTracking,
                                      bool                     allowDuplicate)
    :   Request (serviceProvider,
                 io_service,
                 type,
                 worker,
                 priority,
                 keepTracking,
                 allowDuplicate),
        _resolver (io_service),
        _socket   (io_service) {
}

void RequestConnection::startImpl () {
    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");
    resolve();
}

void RequestConnection::finishImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finishImpl");

    // Close all operations on BOOST ASIO if needed

    _resolver.cancel();
    _socket.cancel();
    _socket.close();
    _timer.cancel();
}

void RequestConnection::restart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "restart");

    switch (_state) {

        case CREATED:
            break;

        case IN_PROGRESS:
            _resolver.cancel();
            _socket.cancel();
            _socket.close();
            _timer.cancel();
            break;

        default:
            break;
    }

    // Reset the state so that we could begin all over again

    setState(CREATED, NONE);
    
    resolve();
}

void RequestConnection::resolve () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "resolve");

    boost::asio::ip::tcp::resolver::query query (
        _workerInfo.svcHost,
        std::to_string(_workerInfo.svcPort)
    );
    _resolver.async_resolve (
        query,
        boost::bind (
            &RequestConnection::resolved,
            shared_from_base<RequestConnection>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::iterator
        )
    );
    setState(IN_PROGRESS, NONE);
}

void RequestConnection::resolved (boost::system::error_code const& ec,
                                  boost::asio::ip::tcp::resolver::iterator iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "resolved");

    if (isAborted(ec)) { return; }

    if (ec) { waitBeforeRestart(); }
    else    { connect(iter); }
}

void RequestConnection::connect (boost::asio::ip::tcp::resolver::iterator iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "connect");

    boost::asio::async_connect (
        _socket,
        iter,
        boost::bind (
            &RequestConnection::connected,
            shared_from_base<RequestConnection>(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::iterator
        )
    );
}

void RequestConnection::connected (boost::system::error_code const& ec,
                                   boost::asio::ip::tcp::resolver::iterator iter) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "connected");

    if (isAborted(ec)) { return; }

    if (ec) { waitBeforeRestart(); }
    else    { beginProtocol(); }
}

void RequestConnection::waitBeforeRestart () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "waitBeforeRestart");

    // Allways need to set the interval before launching the timer.
    
    _timer.expires_from_now(boost::posix_time::seconds(_timerIvalSec));
    _timer.async_wait (
        boost::bind (
            &RequestConnection::awakenForRestart,
            shared_from_base<RequestConnection>(),
            boost::asio::placeholders::error
        )
    );
}

void RequestConnection::awakenForRestart (boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << "awakenForRestart");

    if (isAborted(ec)) { return; }

    // Also ignore this event if the request expired
    if (_state== State::FINISHED) { return; }

    restart();
}

boost::system::error_code RequestConnection::syncReadFrame (size_t& bytes) {

    size_t const frameLength = sizeof(uint32_t);
    _bufferPtr->resize(frameLength);

    boost::system::error_code ec;
    boost::asio::read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            frameLength
        ),
        boost::asio::transfer_at_least(frameLength),
        ec
    );
    if (!ec) {
        bytes = _bufferPtr->parseLength();
    }
    return ec;
}

boost::system::error_code RequestConnection::syncReadMessageImpl (size_t const bytes) {

    _bufferPtr->resize(bytes);

    boost::system::error_code ec;
    boost::asio::read (
        _socket,
        boost::asio::buffer (
            _bufferPtr->data(),
            bytes
        ),
        boost::asio::transfer_at_least(bytes),
        ec
    );
    return ec;
}
    
boost::system::error_code RequestConnection::syncReadVerifyHeader (size_t const bytes) {

    proto::ReplicationResponseHeader hdr;
    boost::system::error_code ec = syncReadMessage (bytes, hdr);
    if (!ec) {
        if (remoteId() != hdr.id()) {
            throw std::logic_error (
                    "RequestConnection::syncReadVerifyHeader()  got unexpected id: " + hdr.id() +
                    " instead of: " + remoteId());
        }
    }
    return ec;
}

}}} // namespace lsst::qserv::replica