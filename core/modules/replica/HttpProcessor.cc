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
#include "replica/HttpIngestIndexModule.h"
#include "replica/HttpJobsModule.h"
#include "replica/HttpMetaModule.h"
#include "replica/HttpQservMonitorModule.h"
#include "replica/HttpRequestsModule.h"
#include "replica/HttpReplicationLevelsModule.h"
#include "replica/HttpSqlIndexModule.h"
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
    auto ptr = Ptr(new HttpProcessor(controller, processorConfig, healthMonitorTask));
    ptr->_initialize();
    return ptr;
}


HttpProcessor::HttpProcessor(Controller::Ptr const& controller,
                             HttpProcessorConfig const& processorConfig,
                             HealthMonitorTask::Ptr const& healthMonitorTask)
    :   EventLogger(controller, taskName),
        _processorConfig(processorConfig),
        _healthMonitorTask(healthMonitorTask) {
}


HttpProcessor::~HttpProcessor() {
    logOnStopEvent();
    controller()->serviceProvider()->httpServer()->stop();
}


void HttpProcessor::_initialize() {

    logOnStartEvent();

    auto const self = shared_from_this();

    controller()->serviceProvider()->httpServer()->addHandlers({
        {"GET", "/meta/version",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpMetaModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "VERSION");
            }
        },
        {"GET", "/replication/catalogs",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpCatalogsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
        },
        {"GET", "/replication/level",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpReplicationLevelsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        self->_healthMonitorTask);
            }
        },
        {"GET", "/replication/worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpWorkerStatusModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        self->_healthMonitorTask);
            }
        },
        {"GET", "/replication/controller",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpControllersModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
        },
        {"GET", "/replication/controller/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpControllersModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/request",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpRequestsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
        },
        {"GET", "/replication/request/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpRequestsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/job",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpJobsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
        },
        {"GET", "/replication/job/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpJobsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-ONE-BY-ID");
            }
        },
        {"GET", "/replication/config",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
        },
        {"PUT", "/replication/config/general",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "UPDATE-GENERAL", HttpModule::AUTH_REQUIRED);
            }
        },
        {"PUT", "/replication/config/worker/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "UPDATE-WORKER", HttpModule::AUTH_REQUIRED);
            }
        },

        {"DELETE", "/replication/config/worker/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-WORKER", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/replication/config/worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-WORKER", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/replication/config/family/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-DATABASE-FAMILY", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/replication/config/family",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-DATABASE-FAMILY", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/replication/config/database/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/replication/config/database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/replication/config/table/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,"DELETE-TABLE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/replication/config/table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-TABLE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"GET", "/replication/qserv/worker/status",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpQservMonitorModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "WORKERS");
            }
        },
        {"GET", "/replication/qserv/worker/status/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpQservMonitorModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-WORKER-BY-NAME");
            }
        },
        {"GET", "/replication/qserv/master/query",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpQservMonitorModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "QUERIES");
            }
        },
        {"GET", "/replication/qserv/master/query/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpQservMonitorModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-QUERY-BY-ID");
            }
        },
        {"POST", "/replication/sql/query",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                string const defaultSubModule;
                HttpQservSqlModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        defaultSubModule, HttpModule::AUTH_REQUIRED);
            }
        },
        {"GET", "/replication/sql/index",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpSqlIndexModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
        },
        {"POST", "/replication/sql/index",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpSqlIndexModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "CREATE-INDEXES", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/replication/sql/index",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpSqlIndexModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DROP-INDEXES", HttpModule::AUTH_REQUIRED);
            }
        },
        {"GET", "/ingest/trans",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "TRANSACTIONS");
            }
        },
        {"GET", "/ingest/trans/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-TRANSACTION-BY-ID");
            }
        },
        {"POST", "/ingest/trans",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "BEGIN-TRANSACTION", HttpModule::AUTH_REQUIRED);
            }
        },
        {"PUT", "/ingest/trans/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "END-TRANSACTION", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"PUT", "/ingest/database/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "PUBLISH-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"DELETE", "/ingest/database/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-DATABASE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-TABLE", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/chunk",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestChunksModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-CHUNK", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/chunks",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestChunksModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-CHUNK-LIST", HttpModule::AUTH_REQUIRED);
            }
        },
        {"POST", "/ingest/chunk/empty",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "BUILD-CHUNK-LIST", HttpModule::AUTH_REQUIRED);
            }
        },
        {"GET", "/ingest/regular/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "REGULAR");
            }
        },
        {"POST", "/ingest/index/secondary",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestIndexModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "BUILD-SECONDARY-INDEX", HttpModule::AUTH_REQUIRED);
            }
        },
        {"GET", "/export/tables/:database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpExportModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "TABLES", HttpModule::AUTH_REQUIRED);
            }
        }
    });
    controller()->serviceProvider()->httpServer()->start();
}

}}} // namespace lsst::qserv::replica
