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
#include "replica/IngestSvc.h"

// System headers
#include <functional>
#include <thread>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestSvc");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

IngestSvc::Ptr IngestSvc::create(ServiceProvider::Ptr const& serviceProvider,
                                 string const& workerName,
                                 string const& authKey) {
    return IngestSvc::Ptr(new IngestSvc(serviceProvider, workerName, authKey));
}


IngestSvc::IngestSvc(ServiceProvider::Ptr const& serviceProvider,
                     string const& workerName,
                     string const& authKey)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
        _authKey(authKey),
        _workerInfo(serviceProvider->config()->workerInfo(workerName)),
        _io_service(),
        _acceptor(
            _io_service,
            boost::asio::ip::tcp::endpoint(
                boost::asio::ip::tcp::v4(),
                _workerInfo.loaderPort)) {

    // Set the socket reuse option to allow recycling ports after catastrophic
    // failures.
    _acceptor.set_option(boost::asio::socket_base::reuse_address(true));
}


void IngestSvc::run() {

    // Queue some work for the io_service, so it doesn't immediately tail out
    // when started.
    _beginAccept();

    // Launch all threads in the pool
    vector<shared_ptr<thread>> threads(_serviceProvider->config()->loaderNumProcessingThreads());
    for (auto&& ptr: threads) {
        ptr = shared_ptr<thread>(new thread([&]() {
            _io_service.run();
        }));
    }

    // Wait for all threads in the pool to exit.
    for (auto&& ptr: threads) {
        ptr->join();
    }
}


void IngestSvc::_beginAccept() {

    IngestSvcConn::Ptr const connection =
        IngestSvcConn::create(
            _serviceProvider,
            _workerName,
            _authKey,
            _io_service);

    _acceptor.async_accept(
        connection->socket(),
        bind(&IngestSvc::_handleAccept, shared_from_this(), connection, _1)
    );
}


void IngestSvc::_handleAccept(IngestSvcConn::Ptr const& connection,
                              boost::system::error_code const& ec) {
    if (ec.value() == 0) {
        connection->beginProtocol();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  ec:" << ec);
    }
    _beginAccept();
}

}}} // namespace lsst::qserv::replica
