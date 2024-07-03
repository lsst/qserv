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
#ifndef LSST_QSERV_CZAR_HTTPCZARSVC_H
#define LSST_QSERV_CZAR_HTTPCZARSVC_H

// System headers
#include <memory>
#include <string>
#include <thread>
#include <vector>

// Third party headers
#include "boost/asio.hpp"

// Forward declarations
namespace httplib {
class SSLServer;
}  // namespace httplib

namespace lsst::qserv::wcontrol {
class Foreman;
}  // namespace lsst::qserv::wcontrol

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpCzarSvc is the HTTP server for processing user requests.
 */
class HttpCzarSvc : public std::enable_shared_from_this<HttpCzarSvc> {
public:
    static std::shared_ptr<HttpCzarSvc> create(int port, unsigned int numThreads,
                                               std::string const& sslCertFile,
                                               std::string const& sslPrivateKeyFile);
    int port() const { return _port; }
    void startAndWait();

private:
    HttpCzarSvc(int port, unsigned int numThreads, std::string const& sslCertFile,
                std::string const& sslPrivateKeyFile);
    void _createAndConfigure();
    void _registerHandlers();

    int _port;
    unsigned int const _numThreads;
    std::string const _sslCertFile;
    std::string const _sslPrivateKeyFile;
    std::size_t const _maxQueuedRequests = 0;  // 0 means unlimited
    std::string const _bindAddr = "0.0.0.0";
    std::unique_ptr<httplib::SSLServer> _svr;

    // The BOOST ASIO I/O services and a thread pool for async communication with
    // the Replication Controller and workers.
    // TODO: Consider a configuration option for setting the desired number
    // of threads in the pool.

    unsigned int const _numBoostAsioThreads = 2;

    std::unique_ptr<boost::asio::io_service::work> _work;
    boost::asio::io_service _io_service;
    std::vector<std::unique_ptr<std::thread>> _threads;
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARSVC_H
