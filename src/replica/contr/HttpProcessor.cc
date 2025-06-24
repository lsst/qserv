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
#include "replica/contr/HttpProcessor.h"

// Qserv headers
#include "http/MetaModule.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/config/ConfigParserMySQL.h"
#include "replica/contr/HttpCatalogsModule.h"
#include "replica/contr/HttpConfigurationModule.h"
#include "replica/contr/HttpControllersModule.h"
#include "replica/contr/HttpExportModule.h"
#include "replica/contr/HttpIngestChunksModule.h"
#include "replica/contr/HttpIngestConfigModule.h"
#include "replica/contr/HttpIngestModule.h"
#include "replica/contr/HttpDirectorIndexModule.h"
#include "replica/contr/HttpIngestTransModule.h"
#include "replica/contr/HttpJobsModule.h"
#include "replica/contr/HttpQservMonitorModule.h"
#include "replica/contr/HttpRequestsModule.h"
#include "replica/contr/HttpReplicationLevelsModule.h"
#include "replica/contr/HttpSqlIndexModule.h"
#include "replica/contr/HttpSqlSchemaModule.h"
#include "replica/contr/HttpWorkerStatusModule.h"
#include "replica/contr/HttpQservSqlModule.h"
#include "replica/services/ServiceProvider.h"

// System headers
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"
#include "nlohmann/json.hpp"

using namespace nlohmann;
using namespace std;
namespace fs = boost::filesystem;

namespace {
string const taskName = "HTTP-PROCESSOR";
}

namespace lsst::qserv::replica {

HttpProcessor::Ptr HttpProcessor::create(Controller::Ptr const& controller,
                                         HttpProcessorConfig const& processorConfig,
                                         HealthMonitorTask::Ptr const& healthMonitorTask) {
    return Ptr(new HttpProcessor(controller, processorConfig, healthMonitorTask));
}

HttpProcessor::HttpProcessor(Controller::Ptr const& controller, HttpProcessorConfig const& processorConfig,
                             HealthMonitorTask::Ptr const& healthMonitorTask)
        : HttpSvc(controller->serviceProvider(),
                  controller->serviceProvider()->config()->get<uint16_t>("controller", "http-server-port"),
                  controller->serviceProvider()->config()->get<unsigned int>("controller",
                                                                             "http-max-listen-conn"),
                  controller->serviceProvider()->config()->get<size_t>("controller", "http-server-threads")),
          EventLogger(controller, taskName),
          _processorConfig(processorConfig),
          _healthMonitorTask(healthMonitorTask) {}

HttpProcessor::~HttpProcessor() { logOnStopEvent(); }

string const& HttpProcessor::context() const { return taskName; }

void HttpProcessor::registerServices() {
    logOnStartEvent();

    // IMPORTANT: qhttp matches requests to handlers in the order they are installed.
    // Therefore all REST services with specific path names should be done first.
    auto const self = shared_from_base<HttpProcessor>();

    // Register REST services first.
    httpServer()->addHandler(
            "GET", "/meta/version", [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                json const info =
                        json::object({{"kind", "replication-controller"},
                                      {"id", self->controller()->identity().id},
                                      {"database_schema_version", ConfigParserMySQL::expectedSchemaVersion},
                                      {"instance_id", self->serviceProvider()->instanceId()}});
                http::MetaModule::process(::taskName, info, req, resp, "VERSION");
            });
    httpServer()->addHandler("GET", "/replication/catalogs",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpCatalogsModule::process(self->controller(), self->name(),
                                                             self->_processorConfig, req, resp);
                             });
    httpServer()->addHandler("GET", "/replication/level",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpReplicationLevelsModule::process(self->controller(), self->name(),
                                                                      self->_processorConfig, req, resp,
                                                                      self->_healthMonitorTask, "GET");
                             });
    httpServer()->addHandler("PUT", "/replication/level",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpReplicationLevelsModule::process(
                                         self->controller(), self->name(), self->_processorConfig, req, resp,
                                         self->_healthMonitorTask, "SET", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/replication/worker",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpWorkerStatusModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 self->_healthMonitorTask);
                             });
    httpServer()->addHandler("GET", "/replication/controller",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpControllersModule::process(self->controller(), self->name(),
                                                                self->_processorConfig, req, resp);
                             });
    httpServer()->addHandler("GET", "/replication/controller/:id",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpControllersModule::process(self->controller(), self->name(),
                                                                self->_processorConfig, req, resp,
                                                                "SELECT-ONE-BY-ID");
                             });
    httpServer()->addHandler("GET", "/replication/controller/:id/dict",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpControllersModule::process(self->controller(), self->name(),
                                                                self->_processorConfig, req, resp,
                                                                "LOG-DICT");
                             });
    httpServer()->addHandler("GET", "/replication/request",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpRequestsModule::process(self->controller(), self->name(),
                                                             self->_processorConfig, req, resp);
                             });
    httpServer()->addHandler("GET", "/replication/request/:id",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpRequestsModule::process(self->controller(), self->name(),
                                                             self->_processorConfig, req, resp,
                                                             "SELECT-ONE-BY-ID");
                             });
    httpServer()->addHandler("GET", "/replication/job",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpJobsModule::process(self->controller(), self->name(),
                                                         self->_processorConfig, req, resp);
                             });
    httpServer()->addHandler("GET", "/replication/job/:id",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpJobsModule::process(self->controller(), self->name(),
                                                         self->_processorConfig, req, resp,
                                                         "SELECT-ONE-BY-ID");
                             });
    httpServer()->addHandler("GET", "/replication/config",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp);
                             });
    httpServer()->addHandler("PUT", "/replication/config/general",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp,
                                                                  "UPDATE-GENERAL", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("PUT", "/replication/config/worker/:worker",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp,
                                                                  "UPDATE-WORKER", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("DELETE", "/replication/config/worker/:worker",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp,
                                                                  "DELETE-WORKER", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("POST", "/replication/config/worker",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp,
                                                                  "ADD-WORKER", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("DELETE", "/replication/config/family/:family",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(
                                         self->controller(), self->name(), self->_processorConfig, req, resp,
                                         "DELETE-DATABASE-FAMILY", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("POST", "/replication/config/family",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(
                                         self->controller(), self->name(), self->_processorConfig, req, resp,
                                         "ADD-DATABASE-FAMILY", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("DELETE", "/replication/config/database/:database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(
                                         self->controller(), self->name(), self->_processorConfig, req, resp,
                                         "DELETE-DATABASE", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("POST", "/replication/config/database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp,
                                                                  "ADD-DATABASE", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("PUT", "/replication/config/database/:database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(
                                         self->controller(), self->name(), self->_processorConfig, req, resp,
                                         "[UN-]PUBLISH-DATABASE", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("DELETE", "/replication/config/table/:database/:table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp,
                                                                  "DELETE-TABLE", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("POST", "/replication/config/table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpConfigurationModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp,
                                                                  "ADD-TABLE", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/replication/qserv/worker/status",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "WORKERS");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/worker/status/:worker",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp, "WORKER");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/worker/config/:worker",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "WORKER-CONFIG");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/worker/db/:worker",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "WORKER-DB");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/worker/files/:worker",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "WORKER-FILES");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/master/status/:czar",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp, "CZAR");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/master/config/:czar",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "CZAR-CONFIG");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/master/db",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "CZAR-DB");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/master/queries/active",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "QUERIES-ACTIVE");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/master/queries/active/progress/:czar",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "QUERIES-ACTIVE-PROGRESS");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/master/queries/past",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "QUERIES-PAST");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/master/query/:id",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp, "QUERY");
                             });
    httpServer()->addHandler("GET", "/replication/qserv/css/shared-scan",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp, "CSS");
                             });
    httpServer()->addHandler("PUT", "/replication/qserv/css/shared-scan/:database/:table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpQservMonitorModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp,
                                                                 "CSS-UPDATE", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/replication/sql/table/schema/:database/:table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpSqlSchemaModule::process(self->controller(), self->name(),
                                                              self->_processorConfig, req, resp,
                                                              "GET-TABLE-SCHEMA");
                             });
    httpServer()->addHandler("PUT", "/replication/sql/table/schema/:database/:table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpSqlSchemaModule::process(self->controller(), self->name(),
                                                              self->_processorConfig, req, resp,
                                                              "ALTER-TABLE-SCHEMA", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("POST", "/replication/sql/query",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 string const defaultSubModule;
                                 HttpQservSqlModule::process(self->controller(), self->name(),
                                                             self->_processorConfig, req, resp,
                                                             defaultSubModule, http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/replication/sql/index/:database/:table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpSqlIndexModule::process(self->controller(), self->name(),
                                                             self->_processorConfig, req, resp);
                             });
    httpServer()->addHandler("POST", "/replication/sql/index",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpSqlIndexModule::process(self->controller(), self->name(),
                                                             self->_processorConfig, req, resp,
                                                             "CREATE-INDEXES", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("DELETE", "/replication/sql/index",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpSqlIndexModule::process(self->controller(), self->name(),
                                                             self->_processorConfig, req, resp,
                                                             "DROP-INDEXES", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/ingest/config",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestConfigModule::process(self->controller(), self->name(),
                                                                 self->_processorConfig, req, resp, "GET");
                             });
    httpServer()->addHandler(
            "PUT", "/ingest/config", [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                HttpIngestConfigModule::process(self->controller(), self->name(), self->_processorConfig, req,
                                                resp, "UPDATE", http::AuthType::REQUIRED);
            });
    httpServer()->addHandler(
            "GET", "/ingest/trans", [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                HttpIngestTransModule::process(self->controller(), self->_transactionMutexRegistry,
                                               self->name(), self->_processorConfig, req, resp,
                                               "TRANSACTIONS");
            });
    httpServer()->addHandler("GET", "/ingest/trans/:id",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestTransModule::process(
                                         self->controller(), self->_transactionMutexRegistry, self->name(),
                                         self->_processorConfig, req, resp, "SELECT-TRANSACTION-BY-ID");
                             });
    httpServer()->addHandler(
            "POST", "/ingest/trans", [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                HttpIngestTransModule::process(self->controller(), self->_transactionMutexRegistry,
                                               self->name(), self->_processorConfig, req, resp,
                                               "BEGIN-TRANSACTION", http::AuthType::REQUIRED);
            });
    httpServer()->addHandler("PUT", "/ingest/trans/:id",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestTransModule::process(self->controller(),
                                                                self->_transactionMutexRegistry, self->name(),
                                                                self->_processorConfig, req, resp,
                                                                "END-TRANSACTION", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/ingest/trans/contrib/:id",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestTransModule::process(
                                         self->controller(), self->_transactionMutexRegistry, self->name(),
                                         self->_processorConfig, req, resp, "GET-CONTRIBUTION-BY-ID");
                             });
    httpServer()->addHandler("GET", "/ingest/database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp, "DATABASES");
                             });
    httpServer()->addHandler("POST", "/ingest/database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp, "ADD-DATABASE",
                                                           http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("PUT", "/ingest/database/:database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp,
                                                           "PUBLISH-DATABASE", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("DELETE", "/ingest/database/:database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp,
                                                           "DELETE-DATABASE", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/ingest/table/:database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp, "TABLES");
                             });
    httpServer()->addHandler(
            "POST", "/ingest/table", [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                HttpIngestModule::process(self->controller(), self->name(), self->_processorConfig, req, resp,
                                          "ADD-TABLE", http::AuthType::REQUIRED);
            });
    httpServer()->addHandler("DELETE", "/ingest/table/:database/:table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp, "DELETE-TABLE",
                                                           http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("POST", "/ingest/table-stats",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp,
                                                           "SCAN-TABLE-STATS", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("DELETE", "/ingest/table-stats/:database/:table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp,
                                                           "DELETE-TABLE-STATS", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/ingest/table-stats/:database/:table",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp, "TABLE-STATS");
                             });
    httpServer()->addHandler(
            "POST", "/ingest/chunk", [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                HttpIngestChunksModule::process(self->controller(), self->name(), self->_processorConfig, req,
                                                resp, "ADD-CHUNK", http::AuthType::REQUIRED);
            });
    httpServer()->addHandler(
            "POST", "/ingest/chunks", [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                HttpIngestChunksModule::process(self->controller(), self->name(), self->_processorConfig, req,
                                                resp, "ADD-CHUNK-LIST", http::AuthType::REQUIRED);
            });
    httpServer()->addHandler(
            "GET", "/ingest/chunks", [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                HttpIngestChunksModule::process(self->controller(), self->name(), self->_processorConfig, req,
                                                resp, "GET-CHUNK-LIST");
            });
    httpServer()->addHandler("POST", "/ingest/chunk/empty",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp,
                                                           "BUILD-CHUNK-LIST", http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/ingest/regular",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpIngestModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp, "REGULAR");
                             });
    httpServer()->addHandler("POST", "/ingest/index/secondary",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpDirectorIndexModule::process(self->controller(), self->name(),
                                                                  self->_processorConfig, req, resp, "BUILD",
                                                                  http::AuthType::REQUIRED);
                             });
    httpServer()->addHandler("GET", "/export/tables/:database",
                             [self](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                 HttpExportModule::process(self->controller(), self->name(),
                                                           self->_processorConfig, req, resp, "TABLES",
                                                           http::AuthType::REQUIRED);
                             });

    // Pass-through for the static content
    if (!self->_processorConfig.httpRoot.empty()) {
        string const context_ = context() + " " + string(__func__) + " ";
        boost::system::error_code ec;
        fs::path const p(self->_processorConfig.httpRoot);
        bool const isDirectory = fs::is_directory(p, ec);
        if (ec.value() != 0) {
            throw runtime_error(context_ + "failed to validate a value of the httpRoot parameter '" +
                                self->_processorConfig.httpRoot + "', error: " + ec.message());
        }
        if (!isDirectory) {
            throw runtime_error(context_ + "a value of the httpRoot parameter '" +
                                self->_processorConfig.httpRoot + "' doesn't refer to a folder.");
        }
        httpServer()->addStaticContent("/*", self->_processorConfig.httpRoot);
    }
}

}  // namespace lsst::qserv::replica
