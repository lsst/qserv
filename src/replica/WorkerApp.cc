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
#include "replica/WorkerApp.h"

// System headers
#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ExportServer.h"
#include "replica/FileServer.h"
#include "replica/FileUtils.h"
#include "replica/IngestHttpSvc.h"
#include "replica/IngestSvc.h"
#include "replica/Registry.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerProcessor.h"
#include "replica/WorkerRequestFactory.h"
#include "replica/WorkerServer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

string const description = "This application represents the worker service of the Replication system.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerApp");

}  // namespace

namespace lsst { namespace qserv { namespace replica {

WorkerApp::Ptr WorkerApp::create(int argc, char* argv[]) { return Ptr(new WorkerApp(argc, argv)); }

WorkerApp::WorkerApp(int argc, char* argv[])
        : Application(argc, argv, description, injectDatabaseOptions, boostProtobufVersionCheck,
                      enableServiceProvider),
          _qservWorkerDbUrl(Configuration::qservWorkerDbUrl()) {
    // Configure the command line parser

    parser().option("qserv-worker-db", "A connection url for the MySQL service of the Qserv worker database.",
                    _qservWorkerDbUrl)
            .flag("do-not-create-folders",
                  "Do not attempt creating missing folders used by the worker services."
                  " Specify this flag in the production deployments of the Replication/Ingest system.",
                  _doNotCreateMissingFolders);
}

int WorkerApp::runImpl() {
    string const context = "WorkerApp::" + string(__func__) + "  ";

    if (!_qservWorkerDbUrl.empty()) {
        // IMPORTANT: set the connector, then clear it up to avoid
        // contaminating the log files when logging command line arguments
        // parsed by the application.
        Configuration::setQservWorkerDbUrl(_qservWorkerDbUrl);
        _qservWorkerDbUrl = "******";
    }

    // Read a unique identifier of the worker from Qserv's worker database.
    string worker;
    {
        // The RAII-style connection handler will rollback a transaction
        // and close the MySQL connection in case of exceptions.
        database::mysql::ConnectionHandler const handler(
                database::mysql::Connection::open(Configuration::qservWorkerDbParams("qservw_worker")));
        handler.conn->executeInOwnTransaction([&worker, &context](decltype(handler.conn) conn) {
            string const colname = "id";
            string const query = "SELECT " + conn->sqlId(colname) + " FROM " + conn->sqlId("Id");
            if (!conn->executeSingleValueSelect(query, colname, worker)) {
                throw invalid_argument(context + "worker identity is not set in the Qserv worker database.");
            }
        });
        LOGS(_log, LOG_LVL_INFO, context << "worker: " << worker);
    }

    _verifyCreateFolders();

    // Configure the factory with a pool of persistent connectors
    auto const config = serviceProvider()->config();
    auto const connectionPool = database::mysql::ConnectionPool::create(
            Configuration::qservWorkerDbParams(), config->get<size_t>("database", "services-pool-size"));
    WorkerRequestFactory requestFactory(serviceProvider(), connectionPool);

    auto const reqProcSvr = WorkerServer::create(serviceProvider(), requestFactory, worker);
    thread reqProcSvrThread([reqProcSvr]() { reqProcSvr->run(); });

    auto const fileSvr = FileServer::create(serviceProvider(), worker);
    thread fileSvrThread([fileSvr]() { fileSvr->run(); });

    auto const ingestSvr = IngestSvc::create(serviceProvider(), worker);
    thread ingestSvrThread([ingestSvr]() { ingestSvr->run(); });

    auto const ingestHttpSvr = IngestHttpSvc::create(serviceProvider(), worker);
    thread ingestHttpSvrThread([ingestHttpSvr]() { ingestHttpSvr->run(); });

    auto const exportSvr = ExportServer::create(serviceProvider(), worker);
    thread exportSvrThread([exportSvr]() { exportSvr->run(); });

    // Keep sending periodic 'heartbeats' to the Registry service to report
    // a configuration and a status of the current worker.
    while (true) {
        try {
            serviceProvider()->registry()->add(worker);
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN, context << "adding worker to the registry failed, ex: " << ex.what());
        }
        LOGS(_log, LOG_LVL_DEBUG,
             "HEARTBEAT"
                     << "  worker: " << reqProcSvr->worker()
                     << "  processor.state: " << reqProcSvr->processor()->state2string()
                     << "  new, in-progress, finished: " << reqProcSvr->processor()->numNewRequests() << ", "
                     << reqProcSvr->processor()->numInProgressRequests() << ", "
                     << reqProcSvr->processor()->numFinishedRequests());
        this_thread::sleep_for(
                chrono::seconds(max(1U, config->get<unsigned int>("registry", "heartbeat-ival-sec"))));
    }
    reqProcSvrThread.join();
    fileSvrThread.join();
    ingestSvrThread.join();
    ingestHttpSvrThread.join();
    exportSvrThread.join();

    return 0;
}

void WorkerApp::_verifyCreateFolders() const {
    auto const config = serviceProvider()->config();
    vector<string> const folders = {config->get<string>("worker", "data-dir"),
                                    config->get<string>("worker", "loader-tmp-dir"),
                                    config->get<string>("worker", "exporter-tmp-dir"),
                                    config->get<string>("worker", "http-loader-tmp-dir")};
    FileUtils::verifyFolders("WORKER", folders, !_doNotCreateMissingFolders);
}

}}}  // namespace lsst::qserv::replica
