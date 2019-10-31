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
#include "replica/IngestServer.h"

// System headers
#include <thread>

// Third party headers
#include <boost/bind.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestServer");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

IngestServer::Ptr IngestServer::create(ServiceProvider::Ptr const& serviceProvider,
                                       string const& workerName) {
    return IngestServer::Ptr(
        new IngestServer(
            serviceProvider,
            workerName));
}


IngestServer::IngestServer(ServiceProvider::Ptr const& serviceProvider,
                           string const& workerName)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
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


void IngestServer::run() {

    // We shall do so before running the io_service. Otherwise it will
    // immediately finish as soon as it will discover that there are
    // outstanding operations.
    _beginAccept();

    // Launch all threads in the pool
    vector<shared_ptr<thread>> threads(_serviceProvider->config()->loaderNumProcessingThreads());

    for (size_t i = 0; i < threads.size(); ++i) {
        shared_ptr<thread> ptr(
            new thread(boost::bind(&boost::asio::io_service::run,
                       &_io_service))
        );
        threads[i] = ptr;
    }

    // Wait for all threads in the pool to exit.
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}


void IngestServer::_beginAccept() {

    IngestServerConnection::Ptr connection =
        IngestServerConnection::create(
            _serviceProvider,
            _workerName,
            _io_service);

    _acceptor.async_accept(
        connection->socket(),
        boost::bind(
            &IngestServer::_handleAccept,
            shared_from_this(),
            connection,
            boost::asio::placeholders::error));
}


void IngestServer::_handleAccept(IngestServerConnection::Ptr const& connection,
                                 boost::system::error_code const& ec) {
    if (ec.value() == 0) {
        connection->beginProtocol();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, _context() << __func__ << "  ec:" << ec);
    }
    _beginAccept();
}

}}} // namespace lsst::qserv::replica
