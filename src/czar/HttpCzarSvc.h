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
namespace lsst::qserv::czar::ingest {
class Processor;
}  // namespace lsst::qserv::czar::ingest

namespace lsst::qserv::http {
class ClientConnPool;
}  // namespace lsst::qserv::http

namespace httplib {
class SSLServer;
}  // namespace httplib

// This header declarations
namespace lsst::qserv::czar {

/**
 * Structure HttpCzarConfig encapculates configuration parameters
 * of the service HttpCzarSvc;
 */
struct HttpCzarConfig {
    std::uint16_t port = 4048;               ///< 0 to allocate the first available port
    std::size_t numThreads = 0;              ///< 0 implies the number of hardware threads
    std::size_t numWorkerIngestThreads = 0;  ///< 0 implies the number of hardware threads
    std::string sslCertFile = "/config-etc/ssl/czar-cert.pem";
    std::string sslPrivateKeyFile = "/config-etc/ssl/czar-key.pem";
    std::string tmpDir = "/tmp";
    std::size_t maxQueuedRequests = 0;    ///< 0 implies unlimited
    std::size_t clientConnPoolSize = 0;   ///< 0 implies the default set by libcurl
    std::size_t numBoostAsioThreads = 0;  ///< 0 implies the number of hardware threads
};

/**
 * Class HttpCzarSvc is the HTTP server for processing user requests.
 */
class HttpCzarSvc : public std::enable_shared_from_this<HttpCzarSvc> {
public:
    static std::shared_ptr<HttpCzarSvc> create(HttpCzarConfig const& httpCzarConfig);
    int port() const { return _httpCzarConfig.port; }
    void startAndWait();

private:
    HttpCzarSvc(HttpCzarConfig const& httpCzarConfig);
    void _createAndConfigure();
    void _registerHandlers();

    HttpCzarConfig _httpCzarConfig;
    std::string const _bindAddr = "0.0.0.0";
    std::unique_ptr<httplib::SSLServer> _svr;

    /// The BOOST ASIO I/O services and a thread pool for async communication with
    /// the Replication Controller and workers.
    std::unique_ptr<boost::asio::io_service::work> _work;
    boost::asio::io_service _io_service;
    std::vector<std::unique_ptr<std::thread>> _threads;

    std::shared_ptr<http::ClientConnPool> _clientConnPool;
    std::shared_ptr<ingest::Processor> _workerIngestProcessor;
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARSVC_H
