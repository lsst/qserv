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
#include "replica/FileServer.h"

// System headers
#include <thread>

// Third party headers
#include <boost/bind.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FileServer");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

FileServer::pointer FileServer::create(ServiceProvider::pointer const& serviceProvider,
                                       std::string const& workerName) {
    return FileServer::pointer(
        new FileServer(
            serviceProvider,
            workerName));
}

FileServer::FileServer(ServiceProvider::pointer const& serviceProvider,
                       std::string const& workerName)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
        _workerInfo(serviceProvider->config()->workerInfo(workerName)),
        _io_service(),
        _acceptor(
            _io_service,
            boost::asio::ip::tcp::endpoint(
                boost::asio::ip::tcp::v4(),
                _workerInfo.fsPort)) {

    // Set the socket reuse option to allow recycling ports after catastrophic
    // failures.
    _acceptor.set_option(boost::asio::socket_base::reuse_address(true));
}

void FileServer::run() {

    // We shall do so before running the io_service. Otherwise it will
    // immediatelly finish as soon as it will discover that there are
    // outstanding operations.
    beginAccept();

    // Launch all threads in the pool    
    std::vector<std::shared_ptr<std::thread>> threads(
                    _serviceProvider->config()->workerNumFsProcessingThreads());

    for (std::size_t i = 0; i < threads.size(); ++i) {
        std::shared_ptr<std::thread> ptr(
            new std::thread(
                boost::bind(&boost::asio::io_service::run,
                            &_io_service)));
        threads[i] = ptr;
    }

    // Wait for all threads in the pool to exit.
    for (std::size_t i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

void FileServer::beginAccept() {

    FileServerConnection::pointer connection =
        FileServerConnection::create(
            _serviceProvider,
            _workerName,
            _io_service);
        
    _acceptor.async_accept(
        connection->socket(),
        boost::bind(
            &FileServer::handleAccept,
            shared_from_this(),
            connection,
            boost::asio::placeholders::error));
}

void FileServer::handleAccept(FileServerConnection::pointer const& connection,
                              boost::system::error_code     const& ec) {
    if (not ec) {
        connection->beginProtocol();
    } else {
        LOGS(_log, LOG_LVL_DEBUG, context() << "handleAccept  ec:" << ec);
    }
    beginAccept();
}

}}} // namespace lsst::qserv::replica