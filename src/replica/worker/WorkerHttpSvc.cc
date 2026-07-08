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
#include "replica/worker/WorkerHttpSvc.h"

// System headers
#include <functional>
#include <filesystem>
#include <stdexcept>
#include <system_error>

// Qserv headers
#include "http/ChttpMetaModule.h"
#include "replica/config/Configuration.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"
#include "replica/worker/WorkerHttpProcessor.h"
#include "replica/worker/WorkerHttpSvcMod.h"

// LSST headers
#include "lsst/log/Log.h"

// Third party headers
#include "httplib.h"
#include "nlohmann/json.hpp"

using namespace nlohmann;
using namespace std;
namespace fs = std::filesystem;

namespace {
string const context_ = "WORKER-HTTP-SVC  ";
LOG_LOGGER _log = LOG_GET("lsst.qserv.worker.WorkerHttpSvc");
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<WorkerHttpSvc> WorkerHttpSvc::create(shared_ptr<ServiceProvider> const& serviceProvider,
                                                string const& workerName) {
    return shared_ptr<WorkerHttpSvc>(new WorkerHttpSvc(serviceProvider, workerName));
}

WorkerHttpSvc::WorkerHttpSvc(shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName)
        : ChttpSvc(context_, serviceProvider,
                   serviceProvider->config()->get<uint16_t>("worker", "http-svc-port"),
                   serviceProvider->config()->get<size_t>("worker", "http-svc-max-queued-requests"),
                   serviceProvider->config()->get<size_t>("worker", "num-http-svc-threads")),
          _workerName(workerName),
          _processor(WorkerHttpProcessor::create(serviceProvider, workerName)) {
    // Start the processor to allow processing requests.
    _processor->run();
}

void WorkerHttpSvc::registerServices(unique_ptr<httplib::Server> const& server) {
    throwIf<logic_error>(server == nullptr, context_ + "the server is not initialized");

    // Static content is served from here.
    string const staticContentMountPoint = "/worker/director-index";
    string const staticContentDir =
            serviceProvider()->config()->get<string>("worker", "loader-tmp-dir") + staticContentMountPoint;

    // Recreate the folder for static content to make sure it's empty. The folder is used to store
    // the "director" index data which is extracted by the worker from its MySQL/MariaDB server,
    // and then made available for download by the worker's clients (e.g., Replication system's Controller)
    // via the static HTTP service.
    // Note that errors are ignored when trying to remove the folder because it may not exist, and that's
    // not a problem. However, an error is reported if the folder can't be created after that.
    std::error_code ec;
    fs::remove_all(fs::path(staticContentDir), ec);
    fs::create_directories(fs::path(staticContentDir), ec);
    throwIf<runtime_error>(
            ec.value() != 0,
            context_ + "failed to create folder for static content, mount point: " + staticContentMountPoint +
                    ", directory: " + staticContentDir + ", error: " + ec.message());

    // Mount the folder for static content. Note that the folder must be created before that, and an error is
    // reported if the mounting fails.
    throwIf<logic_error>(!server->set_mount_point(staticContentMountPoint, staticContentDir),
                         context_ + "failed to set mount point for static content, mount point: " +
                                 staticContentMountPoint + ", directory: " + staticContentDir);

    // Dynamic modules
    auto const self = shared_from_base<WorkerHttpSvc>();
    server->Get("/meta/version", [self](httplib::Request const& req, httplib::Response& resp) {
        json const info = json::object({{"kind", "replication-worker-svc"},
                                        {"id", self->_workerName},
                                        {"instance_id", self->serviceProvider()->instanceId()}});
        http::ChttpMetaModule::process(context_, info, req, resp, "VERSION");
    });
    server->Post("/worker/echo", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "ECHO", http::AuthType::REQUIRED);
    });
    server->Post("/worker/replica/create", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "REPLICA-CREATE", http::AuthType::REQUIRED);
    });
    server->Post("/worker/replica/delete", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "REPLICA-DELETE", http::AuthType::REQUIRED);
    });
    server->Post("/worker/replica/find", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "REPLICA-FIND", http::AuthType::REQUIRED);
    });
    server->Post("/worker/replica/find-all", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "REPLICA-FIND-ALL", http::AuthType::REQUIRED);
    });
    server->Post("/worker/director-index", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "INDEX-GET", http::AuthType::REQUIRED);
    });
    server->Delete("/worker/director-index/:folder/:file",
                   [self](httplib::Request const& req, httplib::Response& resp) {
                       WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName,
                                                 req, resp, "INDEX-DELETE", http::AuthType::REQUIRED);
                   });
    server->Post("/worker/sql", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "SQL", http::AuthType::REQUIRED);
    });
    server->Get("/worker/request/track/:id", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "REQUEST-TRACK");
    });
    server->Get("/worker/request/status/:id", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "REQUEST-STATUS");
    });
    server->Put("/worker/request/stop/:id", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "REQUEST-STOP", http::AuthType::REQUIRED);
    });
    server->Put("/worker/request/dispose", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "REQUEST-DISPOSE", http::AuthType::REQUIRED);
    });
    server->Get("/worker/service/status", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "SERVICE-STATUS");
    });
    server->Get("/worker/service/requests", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "SERVICE-REQUESTS");
    });
    server->Put("/worker/service/suspend", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "SERVICE-SUSPEND", http::AuthType::REQUIRED);
    });
    server->Put("/worker/service/resume", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "SERVICE-RESUME", http::AuthType::REQUIRED);
    });
    server->Put("/worker/service/drain", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "SERVICE-DRAIN", http::AuthType::REQUIRED);
    });
    server->Put("/worker/service/reconfig", [self](httplib::Request const& req, httplib::Response& resp) {
        WorkerHttpSvcMod::process(self->serviceProvider(), self->_processor, self->_workerName, req, resp,
                                  "SERVICE-RECONFIG", http::AuthType::REQUIRED);
    });
}

}  // namespace lsst::qserv::replica
