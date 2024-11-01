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
#include "czar/HttpCzarSvc.h"

// System headers
#include <stdexcept>

// Third-party headers
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
#define CPPHTTPLIB_OPENSSL_SUPPORT 1
#endif
#include <httplib.h>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "czar/HttpCzarIngestCsvModule.h"
#include "czar/HttpCzarIngestModule.h"
#include "czar/HttpCzarQueryModule.h"
#include "czar/WorkerIngestProcessor.h"
#include "http/ClientConnPool.h"
#include "http/ChttpMetaModule.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.HttpCzarSvc");

string const serviceName = "CZAR-FRONTEND ";

template <typename T>
void throwIf(bool condition, string const& message) {
    if (condition) throw T(message);
}

}  // namespace

namespace lsst::qserv::czar {

shared_ptr<HttpCzarSvc> HttpCzarSvc::create(int port, unsigned int numThreads,
                                            unsigned int numWorkerIngestThreads, string const& sslCertFile,
                                            string const& sslPrivateKeyFile, string const& tmpDir,
                                            unsigned int clientConnPoolSize) {
    return shared_ptr<HttpCzarSvc>(new HttpCzarSvc(port, numThreads, numWorkerIngestThreads, sslCertFile,
                                                   sslPrivateKeyFile, tmpDir, clientConnPoolSize));
}

HttpCzarSvc::HttpCzarSvc(int port, unsigned int numThreads, unsigned int numWorkerIngestThreads,
                         string const& sslCertFile, string const& sslPrivateKeyFile, string const& tmpDir,
                         unsigned int clientConnPoolSize)
        : _port(port),
          _numThreads(numThreads),
          _numWorkerIngestThreads(numWorkerIngestThreads),
          _sslCertFile(sslCertFile),
          _sslPrivateKeyFile(sslPrivateKeyFile),
          _tmpDir(tmpDir),
          _clientConnPool(make_shared<http::ClientConnPool>(clientConnPoolSize)),
          _workerIngestProcessor(ingest::Processor::create(
                  numWorkerIngestThreads ? numWorkerIngestThreads : thread::hardware_concurrency())) {
    _createAndConfigure();
}

void HttpCzarSvc::startAndWait() {
    string const context = "czar::HttpCzarSvc::" + string(__func__) + " ";

    // IMPORTANT: Request handlers can't be registered in the constructor
    // because of the shared_from_this() call. This is because the shared
    // pointer is not yet initialized at the time of the constructor call.
    _registerHandlers();

    // This will prevent the I/O service from exiting the .run()
    // method event when it will run out of any requests to process.
    // Unless the service will be explicitly stopped.
    _work.reset(new boost::asio::io_service::work(_io_service));

    // Initialize the I/O context and start the service threads. At this point
    // the server will be ready to service incoming requests.
    for (unsigned int i = 0; i < _numBoostAsioThreads; ++i) {
        _threads.push_back(make_unique<thread>([self = shared_from_this()]() { self->_io_service.run(); }));
    }
    bool const started = _svr->listen_after_bind();
    ::throwIf<runtime_error>(!started, context + "Failed to start the server");
}

void HttpCzarSvc::_createAndConfigure() {
    string const context = "czar::HttpCzarSvc::" + string(__func__) + " ";

    ::throwIf<invalid_argument>(_sslCertFile.empty(), context + "SSL certificate file is not valid");
    ::throwIf<invalid_argument>(_sslPrivateKeyFile.empty(), context + "SSL private key file is not valid");

    _svr = make_unique<httplib::SSLServer>(_sslCertFile.data(), _sslPrivateKeyFile.data());
    ::throwIf<runtime_error>(!_svr->is_valid(), context + "Failed to create the server");

    _svr->new_task_queue = [&] {
        return new httplib::ThreadPool(_numThreads ? _numThreads : thread::hardware_concurrency(),
                                       _maxQueuedRequests);
    };
    if (_port == 0) {
        _port = _svr->bind_to_any_port(_bindAddr, _port);
        ::throwIf<runtime_error>(_port < 0, context + "Failed to bind the server to any port");
    } else {
        bool const bound = _svr->bind_to_port(_bindAddr, _port);
        ::throwIf<runtime_error>(!bound,
                                 context + "Failed to bind the server to the port: " + to_string(_port));
    }
    LOGS(_log, LOG_LVL_INFO, context + "started on port " + to_string(_port));
}

void HttpCzarSvc::_registerHandlers() {
    ::throwIf<logic_error>(_svr == nullptr,
                           "czar::HttpCzarSvc::" + string(__func__) + " the server is not initialized");
    auto const self = shared_from_this();
    _svr->Get("/meta/version", [self](httplib::Request const& req, httplib::Response& resp) {
        json const info =
                json::object({{"kind", "qserv-czar-query-frontend"},
                              {"id", cconfig::CzarConfig::instance()->id()},
                              {"instance_id", cconfig::CzarConfig::instance()->replicationInstanceId()}});
        http::ChttpMetaModule::process(::serviceName, info, req, resp, "VERSION");
    });
    _svr->Post("/query", [self](httplib::Request const& req, httplib::Response& resp) {
        HttpCzarQueryModule::process(::serviceName, req, resp, "SUBMIT");
    });
    _svr->Post("/query-async", [self](httplib::Request const& req, httplib::Response& resp) {
        HttpCzarQueryModule::process(::serviceName, req, resp, "SUBMIT-ASYNC");
    });
    _svr->Delete("/query-async/:qid", [self](httplib::Request const& req, httplib::Response& resp) {
        HttpCzarQueryModule::process(::serviceName, req, resp, "CANCEL");
    });
    _svr->Get("/query-async/status/:qid", [self](httplib::Request const& req, httplib::Response& resp) {
        HttpCzarQueryModule::process(::serviceName, req, resp, "STATUS");
    });
    _svr->Get("/query-async/result/:qid", [self](httplib::Request const& req, httplib::Response& resp) {
        HttpCzarQueryModule::process(::serviceName, req, resp, "RESULT");
    });
    _svr->Post("/ingest/csv", [self](httplib::Request const& req, httplib::Response& resp,
                                     httplib::ContentReader const& contentReader) {
        HttpCzarIngestCsvModule::process(self->_io_service, ::serviceName, self->_tmpDir, req, resp,
                                         contentReader, self->_clientConnPool, self->_workerIngestProcessor);
    });
    _svr->Post("/ingest/data", [self](httplib::Request const& req, httplib::Response& resp) {
        HttpCzarIngestModule::process(self->_io_service, ::serviceName, req, resp, "INGEST-DATA");
    });
    _svr->Delete("/ingest/database/:database", [self](httplib::Request const& req, httplib::Response& resp) {
        HttpCzarIngestModule::process(self->_io_service, ::serviceName, req, resp, "DELETE-DATABASE");
    });
    _svr->Delete(
            "/ingest/table/:database/:table", [self](httplib::Request const& req, httplib::Response& resp) {
                HttpCzarIngestModule::process(self->_io_service, ::serviceName, req, resp, "DELETE-TABLE");
            });
}

}  // namespace lsst::qserv::czar
