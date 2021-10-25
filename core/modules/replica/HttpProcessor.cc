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
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/HttpCatalogsModule.h"
#include "replica/HttpConfigurationModule.h"
#include "replica/HttpControllersModule.h"
#include "replica/HttpExportModule.h"
#include "replica/HttpIngestChunksModule.h"
#include "replica/HttpIngestConfigModule.h"
#include "replica/HttpIngestModule.h"
#include "replica/HttpIngestIndexModule.h"
#include "replica/HttpIngestTransModule.h"
#include "replica/HttpJobsModule.h"
#include "replica/HttpMetaModule.h"
#include "replica/HttpQservMonitorModule.h"
#include "replica/HttpRequestsModule.h"
#include "replica/HttpReplicationLevelsModule.h"
#include "replica/HttpSqlIndexModule.h"
#include "replica/HttpSqlSchemaModule.h"
#include "replica/HttpWorkerStatusModule.h"
#include "replica/HttpQservSqlModule.h"
#include "replica/ServiceProvider.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

using namespace std;
namespace fs = boost::filesystem;

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
    return Ptr(new HttpProcessor(controller, processorConfig, healthMonitorTask));
}


HttpProcessor::HttpProcessor(Controller::Ptr const& controller,
                             HttpProcessorConfig const& processorConfig,
                             HealthMonitorTask::Ptr const& healthMonitorTask)
    :   HttpSvc(controller->serviceProvider(),
                controller->serviceProvider()->config()->get<uint16_t>("controller", "http_server_port"),
                controller->serviceProvider()->config()->get<unsigned int>("controller", "http_max_listen_conn"),
                controller->serviceProvider()->config()->get<size_t>("controller", "http_server_threads"),
                processorConfig.authKey,
                processorConfig.adminAuthKey),
        EventLogger(controller, taskName),
        _processorConfig(processorConfig),
        _healthMonitorTask(healthMonitorTask) {
}


HttpProcessor::~HttpProcessor() {
    logOnStopEvent();
}


string const& HttpProcessor::context() const { return taskName; }


void HttpProcessor::registerServices() {
    logOnStartEvent();

    // IMPORTANT: qhttp matches requests to handlers in the order they are installed.
    // Therefore all REST services with specific path names should be done first.
    auto const self = shared_from_base<HttpProcessor>();

    // Register REST services first.
    httpServer()->addHandler(
            "GET", "/meta/version",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpMetaModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "VERSION");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/catalogs",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpCatalogsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/level",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpReplicationLevelsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        self->_healthMonitorTask);
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpWorkerStatusModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        self->_healthMonitorTask);
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/controller",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpControllersModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/controller/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpControllersModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-ONE-BY-ID");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/controller/:id/dict",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpControllersModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "LOG-DICT");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/request",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpRequestsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/request/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpRequestsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-ONE-BY-ID");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/job",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpJobsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/job/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpJobsModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-ONE-BY-ID");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/config",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
    );
    httpServer()->addHandler(
            "PUT", "/replication/config/general",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "UPDATE-GENERAL", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "PUT", "/replication/config/worker/:worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "UPDATE-WORKER", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "DELETE", "/replication/config/worker/:worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-WORKER", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "POST", "/replication/config/worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-WORKER", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "DELETE", "/replication/config/family/:family",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-DATABASE-FAMILY", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "POST", "/replication/config/family",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-DATABASE-FAMILY", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "DELETE", "/replication/config/database/:database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-DATABASE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "POST", "/replication/config/database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-DATABASE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "DELETE", "/replication/config/table/:database/:table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,"DELETE-TABLE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "POST", "/replication/config/table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpConfigurationModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-TABLE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/qserv/worker/status",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpQservMonitorModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "WORKERS");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/qserv/worker/status/:worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpQservMonitorModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-WORKER-BY-NAME");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/qserv/master/query",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpQservMonitorModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "QUERIES");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/qserv/master/query/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpQservMonitorModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-QUERY-BY-ID");
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/sql/table/schema/:database/:table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpSqlSchemaModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "GET-TABLE-SCHEMA");
            }
    );
    httpServer()->addHandler(
            "PUT", "/replication/sql/table/schema/:database/:table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpSqlSchemaModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ALTER-TABLE-SCHEMA", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "POST", "/replication/sql/query",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                string const defaultSubModule;
                HttpQservSqlModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        defaultSubModule, HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/replication/sql/index",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpSqlIndexModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp);
            }
    );
    httpServer()->addHandler(
            "POST", "/replication/sql/index",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpSqlIndexModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "CREATE-INDEXES", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "DELETE", "/replication/sql/index",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpSqlIndexModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DROP-INDEXES", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/ingest/config",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestConfigModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "GET");
            }
    );
    httpServer()->addHandler(
            "PUT", "/ingest/config",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestConfigModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "UPDATE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/ingest/trans",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestTransModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "TRANSACTIONS");
            }
    );
    httpServer()->addHandler(
            "GET", "/ingest/trans/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestTransModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "SELECT-TRANSACTION-BY-ID");
            }
    );
    httpServer()->addHandler(
            "POST", "/ingest/trans",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestTransModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "BEGIN-TRANSACTION", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "PUT", "/ingest/trans/:id",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestTransModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "END-TRANSACTION", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/ingest/database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DATABASES");
            }
    );
    httpServer()->addHandler(
            "POST", "/ingest/database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-DATABASE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "PUT", "/ingest/database/:database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "PUBLISH-DATABASE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "DELETE", "/ingest/database/:database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-DATABASE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/ingest/table/:database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "TABLES");
            }
    );
    httpServer()->addHandler(
            "POST", "/ingest/table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-TABLE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "DELETE", "/ingest/table/:database/:table",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "DELETE-TABLE", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "POST", "/ingest/chunk",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestChunksModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-CHUNK", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "POST", "/ingest/chunks",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestChunksModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "ADD-CHUNK-LIST", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/ingest/chunks",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestChunksModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "GET-CHUNK-LIST");
            }
    );
    httpServer()->addHandler(
            "POST", "/ingest/chunk/empty",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "BUILD-CHUNK-LIST", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/ingest/regular",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "REGULAR");
            }
    );
    httpServer()->addHandler(
            "POST", "/ingest/index/secondary",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpIngestIndexModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "BUILD-SECONDARY-INDEX", HttpModule::AUTH_REQUIRED);
            }
    );
    httpServer()->addHandler(
            "GET", "/export/tables/:database",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                HttpExportModule::process(
                        self->controller(), self->name(), self->_processorConfig,
                        req, resp,
                        "TABLES", HttpModule::AUTH_REQUIRED);
            }
    );

    // Pass-through for the static content
    if (!self->_processorConfig.httpRoot.empty()) {
        string const context_ = context() + " " + string(__func__) + " ";
        boost::system::error_code ec;
        fs::path const p(self->_processorConfig.httpRoot);
        bool const isDirectory = fs::is_directory(p, ec);
        if (ec.value() != 0) {
            throw runtime_error(
                    context_+ "failed to validate a value of the httpRoot parameter '"
                    + self->_processorConfig.httpRoot + "', error: " + ec.message());
        }
        if (!isDirectory) {
            throw runtime_error(
                    context_ + "a value of the httpRoot parameter '"
                    + self->_processorConfig.httpRoot + "' doesn't refer to a folder."); 
        }
        httpServer()->addStaticContent("/*", self->_processorConfig.httpRoot);
    }

}

}}} // namespace lsst::qserv::replica
