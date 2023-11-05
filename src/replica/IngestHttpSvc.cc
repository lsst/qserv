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
#include "replica/IngestHttpSvc.h"

// System headers
#include <functional>

// Qserv headers
#include "http/MetaModule.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/Configuration.h"
#include "replica/IngestHttpSvcMod.h"
#include "replica/IngestRequestMgr.h"

// LSST headers
#include "lsst/log/Log.h"

// Third party headers
#include "boost/filesystem.hpp"
#include "nlohmann/json.hpp"

using namespace nlohmann;
using namespace std;

namespace {
string const context_ = "INGEST-HTTP-SVC  ";
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestHttpSvc");
}  // namespace

namespace lsst::qserv::replica {

IngestHttpSvc::Ptr IngestHttpSvc::create(ServiceProvider::Ptr const& serviceProvider,
                                         string const& workerName) {
    return IngestHttpSvc::Ptr(new IngestHttpSvc(serviceProvider, workerName));
}

IngestHttpSvc::IngestHttpSvc(ServiceProvider::Ptr const& serviceProvider, string const& workerName)
        : HttpSvc(serviceProvider, serviceProvider->config()->get<uint16_t>("worker", "http-loader-port"),
                  serviceProvider->config()->get<unsigned int>("worker", "http-max-listen-conn"),
                  serviceProvider->config()->get<size_t>("worker", "num-http-loader-processing-threads")),
          _workerName(workerName),
          _requestMgr(IngestRequestMgr::create(serviceProvider, workerName)),
          _threads(serviceProvider->config()->get<size_t>("worker", "num-async-loader-processing-threads")) {}

string const& IngestHttpSvc::context() const { return context_; }

void IngestHttpSvc::registerServices() {
    auto const self = shared_from_base<IngestHttpSvc>();
    httpServer()->addHandlers(
            {{"GET", "/meta/version",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  json const info = json::object({{"kind", "replication-worker-ingest"},
                                                  {"id", self->_workerName},
                                                  {"instance_id", self->serviceProvider()->instanceId()}});
                  http::MetaModule::process(::context_, info, req, resp, "VERSION");
              }},
             {"POST", "/ingest/file",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName,
                                            req, resp, "SYNC-PROCESS");
              }},
             {"PUT", "/ingest/file/:id",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName,
                                            req, resp, "SYNC-RETRY");
              }},
             {"POST", "/ingest/file-async",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName,
                                            req, resp, "ASYNC-SUBMIT");
              }},
             {"PUT", "/ingest/file-async/:id",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName,
                                            req, resp, "ASYNC-RETRY");
              }},
             {"GET", "/ingest/file-async/:id",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName,
                                            req, resp, "ASYNC-STATUS-BY-ID", http::AuthType::NONE);
              }},
             {"DELETE", "/ingest/file-async/:id",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName,
                                            req, resp, "ASYNC-CANCEL-BY-ID");
              }},
             {"GET", "/ingest/file-async/trans/:id",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName,
                                            req, resp, "ASYNC-STATUS-BY-TRANS-ID", http::AuthType::NONE);
              }},
             {"DELETE", "/ingest/file-async/trans/:id",
              [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName,
                                            req, resp, "ASYNC-CANCEL-BY-TRANS-ID");
              }}});

    // Create the thread pool for processing asynchronous loading requests.
    for (auto&& ptr : _threads) {
        ptr.reset(new thread([self]() {
            while (true) {
                auto const request = self->_requestMgr->next();
                try {
                    request->process();
                } catch (exception const& ex) {
                    LOGS(_log, LOG_LVL_ERROR,
                         "IngestHttpSvc::" << __func__ << " request failed: "
                                           << request->transactionContribInfo().toJson().dump()
                                           << ", ex: " << ex.what());
                }
                self->_requestMgr->completed(request->transactionContribInfo().id);
            }
        }));
    }
}

}  // namespace lsst::qserv::replica
