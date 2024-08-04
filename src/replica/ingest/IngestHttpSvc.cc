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
#include "replica/ingest/IngestHttpSvc.h"

// System headers
#include <functional>
#include <stdexcept>

// Qserv headers
#include "http/ChttpMetaModule.h"
#include "replica/config/Configuration.h"
#include "replica/ingest/IngestDataHttpSvcMod.h"
#include "replica/ingest/IngestHttpSvcMod.h"
#include "replica/ingest/IngestRequest.h"
#include "replica/ingest/IngestRequestMgr.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"

// LSST headers
#include "lsst/log/Log.h"

// Third party headers
#include "httplib.h"
#include "nlohmann/json.hpp"

using namespace nlohmann;
using namespace std;

namespace {
string const context_ = "INGEST-HTTP-SVC  ";
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestHttpSvc");
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<IngestHttpSvc> IngestHttpSvc::create(shared_ptr<ServiceProvider> const& serviceProvider,
                                                string const& workerName) {
    return shared_ptr<IngestHttpSvc>(new IngestHttpSvc(serviceProvider, workerName));
}

IngestHttpSvc::IngestHttpSvc(shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName)
        : ChttpSvc(context_, serviceProvider,
                   serviceProvider->config()->get<uint16_t>("worker", "http-loader-port"),
                   serviceProvider->config()->get<size_t>("worker", "http-max-queued-requests"),
                   serviceProvider->config()->get<size_t>("worker", "num-http-loader-processing-threads")),
          _workerName(workerName),
          _requestMgr(IngestRequestMgr::create(serviceProvider, workerName)),
          _threads(serviceProvider->config()->get<size_t>("worker", "num-async-loader-processing-threads")) {}

void IngestHttpSvc::registerServices(unique_ptr<httplib::Server> const& server) {
    throwIf<logic_error>(server == nullptr, context_ + "the server is not initialized");
    auto const self = shared_from_base<IngestHttpSvc>();
    server->Get("/meta/version", [self](httplib::Request const& req, httplib::Response& resp) {
        json const info = json::object({{"kind", "replication-worker-ingest"},
                                        {"id", self->_workerName},
                                        {"instance_id", self->serviceProvider()->instanceId()}});
        http::ChttpMetaModule::process(context_, info, req, resp, "VERSION");
    });

    server->Post("/ingest/data", [self](httplib::Request const& req, httplib::Response& resp) {
        IngestDataHttpSvcMod::process(self->serviceProvider(), self->_workerName, req, resp,
                                      "SYNC-PROCESS-DATA");
    });
    server->Post("/ingest/file", [self](httplib::Request const& req, httplib::Response& resp) {
        IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName, req, resp,
                                  "SYNC-PROCESS");
    });
    server->Put("/ingest/file/:id", [self](httplib::Request const& req, httplib::Response& resp) {
        IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName, req, resp,
                                  "SYNC-RETRY");
    });
    server->Post("/ingest/file-async", [self](httplib::Request const& req, httplib::Response& resp) {
        IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName, req, resp,
                                  "ASYNC-SUBMIT");
    });
    server->Put("/ingest/file-async/:id", [self](httplib::Request const& req, httplib::Response& resp) {
        IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName, req, resp,
                                  "ASYNC-RETRY");
    });
    server->Get("/ingest/file-async/:id", [self](httplib::Request const& req, httplib::Response& resp) {
        IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName, req, resp,
                                  "ASYNC-STATUS-BY-ID", http::AuthType::NONE);
    });
    server->Delete("/ingest/file-async/:id", [self](httplib::Request const& req, httplib::Response& resp) {
        IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName, req, resp,
                                  "ASYNC-CANCEL-BY-ID");
    });
    server->Get("/ingest/file-async/trans/:id", [self](httplib::Request const& req, httplib::Response& resp) {
        IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr, self->_workerName, req, resp,
                                  "ASYNC-STATUS-BY-TRANS-ID", http::AuthType::NONE);
    });
    server->Delete("/ingest/file-async/trans/:id",
                   [self](httplib::Request const& req, httplib::Response& resp) {
                       IngestHttpSvcMod::process(self->serviceProvider(), self->_requestMgr,
                                                 self->_workerName, req, resp, "ASYNC-CANCEL-BY-TRANS-ID");
                   });

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
