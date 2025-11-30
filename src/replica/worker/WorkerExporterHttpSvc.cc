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
#include "replica/worker/WorkerExporterHttpSvc.h"

// System headers
#include <functional>
#include <stdexcept>

// Qserv headers
#include "http/ChttpMetaModule.h"
#include "replica/config/Configuration.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLTypes.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"
#include "replica/worker/WorkerExporterHttpSvcMod.h"

// LSST headers
#include "lsst/log/Log.h"

// Third party headers
#include "httplib.h"
#include "nlohmann/json.hpp"

using namespace nlohmann;
using namespace std;

namespace {
string const context_ = "WORKER-EXPORTER-HTTP-SVC  ";
LOG_LOGGER _log = LOG_GET("lsst.qserv.worker.WorkerExporterHttpSvc");
}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

shared_ptr<WorkerExporterHttpSvc> WorkerExporterHttpSvc::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName) {
    return shared_ptr<WorkerExporterHttpSvc>(new WorkerExporterHttpSvc(serviceProvider, workerName));
}

WorkerExporterHttpSvc::WorkerExporterHttpSvc(shared_ptr<ServiceProvider> const& serviceProvider,
                                             string const& workerName)
        : ChttpSvc(context_, serviceProvider,
                   serviceProvider->config()->get<uint16_t>("worker", "exporter-port"),
                   serviceProvider->config()->get<size_t>("worker", "exporter-max-queued-requests"),
                   serviceProvider->config()->get<size_t>("worker", "exporter-threads")),
          _workerName(workerName),
          _databaseConnectionPool(ConnectionPool::create(
                  serviceProvider->config()->qservWorkerDbParams(),
                  serviceProvider->config()->get<size_t>("worker", "exporter-threads"))) {}

void WorkerExporterHttpSvc::registerServices(unique_ptr<httplib::Server> const& server) {
    throwIf<logic_error>(server == nullptr, context_ + "the server is not initialized");
    auto const self = shared_from_base<WorkerExporterHttpSvc>();
    server->Get("/meta/version", [self](httplib::Request const& req, httplib::Response& resp) {
        json const info = json::object({{"kind", "replication-worker-exporter"},
                                        {"id", self->_workerName},
                                        {"instance_id", self->serviceProvider()->instanceId()}});
        http::ChttpMetaModule::process(context_, info, req, resp, "VERSION");
    });
    server->Get("/worker/export/:database/:table",
                [self](httplib::Request const& req, httplib::Response& resp) {
                    WorkerExporterHttpSvcMod::process(self->serviceProvider(), self->_workerName,
                                                      self->_databaseConnectionPool, req, resp, "TABLE");
                });
    server->Get("/worker/export/:database/:table/:chunk",
                [self](httplib::Request const& req, httplib::Response& resp) {
                    WorkerExporterHttpSvcMod::process(self->serviceProvider(), self->_workerName,
                                                      self->_databaseConnectionPool, req, resp, "CHUNK");
                });
}

}  // namespace lsst::qserv::replica
