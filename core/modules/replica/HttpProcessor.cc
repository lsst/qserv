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
#include "replica/HttpProcessor.h"

// Qserv headers
#include "qhttp/Server.h"
#include "replica/HttpCatalogsModule.h"
#include "replica/HttpConfigurationModule.h"
#include "replica/HttpControllersModule.h"
#include "replica/HttpExportModule.h"
#include "replica/HttpIngestChunksModule.h"
#include "replica/HttpIngestModule.h"
#include "replica/HttpJobsModule.h"
#include "replica/HttpQservMonitorModule.h"
#include "replica/HttpRequestsModule.h"
#include "replica/HttpReplicationLevelsModule.h"
#include "replica/HttpWorkerStatusModule.h"
#include "replica/HttpQservSqlModule.h"
#include "replica/ServiceProvider.h"

using namespace std;

namespace {
string const taskName = "HTTP-PROCESSOR";
}

namespace lsst {
namespace qserv {
namespace replica {

HttpProcessor::Ptr HttpProcessor::create(
                        Controller::Ptr const& controller,
                        HttpProcessorConfig const& processorConfig,
                        HealthMonitorTask::Ptr const& healthMonitorTask) {

    auto ptr = Ptr(new HttpProcessor(
        controller, processorConfig, healthMonitorTask
    ));
    ptr->_initialize();
    return ptr;
}


HttpProcessor::HttpProcessor(Controller::Ptr const& controller,
                             HttpProcessorConfig const& processorConfig,
                             HealthMonitorTask::Ptr const& healthMonitorTask)
    :   EventLogger(controller, taskName),
        _catalogsModule(HttpCatalogsModule::create(
            controller, taskName, processorConfig)),
        _replicationLevelsModule(HttpReplicationLevelsModule::create(
            controller, taskName, processorConfig, healthMonitorTask)),
        _workerStatusModule(HttpWorkerStatusModule::create(
            controller, taskName, processorConfig, healthMonitorTask)),
        _controllersModule(HttpControllersModule::create(
            controller, taskName, processorConfig)),
        _requestsModule(HttpRequestsModule::create(
            controller, taskName, processorConfig)),
        _jobsModule(HttpJobsModule::create(
            controller, taskName, processorConfig)),
        _configurationModule(HttpConfigurationModule::create(
            controller, taskName, processorConfig)),
        _qservMonitorModule(HttpQservMonitorModule::create(
            controller, taskName, processorConfig)),
        _qservSqlModule(HttpQservSqlModule::create(
            controller, taskName, processorConfig)),
        _ingestChunksModule(HttpIngestChunksModule::create(
            controller, taskName, processorConfig)),
        _ingestModule(HttpIngestModule::create(
            controller, taskName, processorConfig)),
        _exportModule(HttpExportModule::create(
            controller, taskName, processorConfig)) {
}


HttpProcessor::~HttpProcessor() {
    logOnStopEvent();
    controller()->serviceProvider()->httpServer()->stop();
}


void HttpProcessor::_initialize() {

    logOnStartEvent();

    auto const self = shared_from_this();

    controller()->serviceProvider()->httpServer()->addHandlers({
        {"GET", "/replication/v1/catalogs",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_catalogsModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/level",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_replicationLevelsModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_workerStatusModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/controller",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_controllersModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/controller/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_controllersModule->execute(req, resp, "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/v1/request",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_requestsModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/request/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_requestsModule->execute(req, resp, "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/v1/job",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_jobsModule->execute(req, resp);
            }
        },
        {"GET", "/replication/v1/job/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_jobsModule->execute(req, resp, "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/v1/config",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp);
            }
        },
        {"PUT", "/replication/v1/config/general",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "UPDATE-GENERAL", HttpModule::AUTH_REQUIRED);
            }
        },
        {"PUT", "/replication/v1/config/worker/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "UPDATE-WORKER", HttpModule::AUTH_REQUIRED);
            }
        },

        {"DELETE", "/replication/v1/config/worker/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "DELETE-WORKER", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/replication/v1/config/worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "ADD-WORKER", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/replication/v1/config/family/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "DELETE-DATABASE-FAMILY", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/replication/v1/config/family",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "ADD-DATABASE-FAMILY", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/replication/v1/config/database/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "DELETE-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/replication/v1/config/database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "ADD-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/replication/v1/config/table/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "DELETE-TABLE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/replication/v1/config/table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_configurationModule->execute(req, resp, "ADD-TABLE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"GET", "/replication/v1/qserv/worker/status",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_qservMonitorModule->execute(req, resp, "WORKERS");
            }
        },
        {"GET", "/replication/v1/qserv/worker/status/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_qservMonitorModule->execute(req, resp, "SELECT-WORKER-BY-NAME");
            }
        },
        {"GET", "/replication/v1/qserv/master/query",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_qservMonitorModule->execute(req, resp, "QUERIES");
            }
        },
        {"GET", "/replication/v1/qserv/master/query/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_qservMonitorModule->execute(req, resp, "SELECT-QUERY-BY-ID");
            }
        },
        {"POST", "/replication/v1/sql/query",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_qservSqlModule->execute(req, resp, string(), HttpModule::AUTH_REQUIRED);
            }
        },
        {"GET", "/ingest/v1/trans",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "TRANSACTIONS");
            }
        },
        {"GET", "/ingest/v1/trans/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "SELECT-TRANSACTION-BY-ID");
            }
        },
        {"POST", "/ingest/v1/trans",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "BEGIN-TRANSACTION", HttpModule::AUTH_REQUIRED);
            }
        },
        {"PUT", "/ingest/v1/trans/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "END-TRANSACTION", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/v1/database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "ADD-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"PUT", "/ingest/v1/database/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "PUBLISH-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/ingest/v1/database/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "DELETE-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/v1/table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "ADD-TABLE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/v1/chunk",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestChunksModule->execute(req, resp, "ADD-CHUNK", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/v1/chunks",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestChunksModule->execute(req, resp, "ADD-CHUNK-LIST", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/v1/chunk/empty",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "BUILD-CHUNK-LIST", HttpModule::AUTH_REQUIRED);
            }
        },
        {"GET", "/ingest/v1/regular/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_ingestModule->execute(req, resp, "REGULAR");
            }
        },
        {"GET", "/export/v1/tables/:database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                self->_exportModule->execute(req, resp, "TABLES");
            }
        }
    });
    controller()->serviceProvider()->httpServer()->start();
}

}}} // namespace lsst::qserv::replica
