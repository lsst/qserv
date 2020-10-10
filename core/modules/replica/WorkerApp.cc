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
#include <thread>

// Qserv headers
#include "replica/DatabaseMySQL.h"
#include "replica/ExportServer.h"
#include "replica/FileServer.h"
#include "replica/IngestHttpSvc.h"
#include "replica/IngestSvc.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerProcessor.h"
#include "replica/WorkerRequestFactory.h"
#include "replica/WorkerServer.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
    "This application represents the worker service of the Replication system.";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

WorkerApp::Ptr WorkerApp::create(int argc, char* argv[]) {
    return Ptr(
        new WorkerApp(argc, argv)
    );
}


WorkerApp::WorkerApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ),
        _log(LOG_GET("lsst.qserv.replica.WorkerApp")),
        _qservDbPassword(Configuration::qservWorkerDatabasePassword()) {

    // Configure the command line parser

    parser().required(
        "worker",
        "The name of a worker.",
        _worker);

    parser().option(
        "qserv-db-password",
        "A password for the MySQL account of the Qserv worker database. The account"
        " name is found in the Configuration.",
        _qservDbPassword
    ).option(
        "auth-key",
        "An authorization key for the catalog ingest operations.",
        _authKey
    );
}


int WorkerApp::runImpl() {

    string const context = "WorkerApp::" + string(__func__) + "  ";

    // Set the database password
    Configuration::setQservWorkerDatabasePassword(_qservDbPassword);

    // Configure the factory with a pool of persistent connectors
    auto const workerInfo = serviceProvider()->config()->workerInfo(_worker);
    auto const connectionPool = database::mysql::ConnectionPool::create(
        database::mysql::ConnectionParams(
            workerInfo.dbHost,
            workerInfo.dbPort,
            workerInfo.dbUser,
            serviceProvider()->config()->qservWorkerDatabasePassword(),
            ""
        ),
        serviceProvider()->config()->databaseServicesPoolSize()
    );
    WorkerRequestFactory requestFactory(serviceProvider(), connectionPool);

    auto const reqProcSvr = WorkerServer::create(serviceProvider(), requestFactory, _worker);
    thread reqProcSvrThread([reqProcSvr] () {
        reqProcSvr->run();
    });

    auto const fileSvr = FileServer::create(serviceProvider(), _worker);
    thread fileSvrThread([fileSvr]() {
        fileSvr->run();
    });

    auto const ingestSvr = IngestSvc::create(serviceProvider(), _worker, _authKey);
    thread ingestSvrThread([ingestSvr]() {
        ingestSvr->run();
    });

    auto const ingestHttpSvr = IngestHttpSvc::create(serviceProvider(), _worker, _authKey);
    thread ingestHttpSvrThread([ingestHttpSvr]() {
        ingestHttpSvr->run();
    });

    auto const exportSvr = ExportServer::create(serviceProvider(), _worker, _authKey);
    thread exportSvrThread([exportSvr]() {
        exportSvr->run();
    });

    // Print the 'heartbeat' report every 5 seconds

    util::BlockPost blockPost(5000, 5001);
    while (true) {
        blockPost.wait();
        LOGS(_log, LOG_LVL_INFO, "HEARTBEAT"
            << "  worker: " << reqProcSvr->worker()
            << "  processor.state: " << reqProcSvr->processor()->state2string()
            << "  new, in-progress, finished: "
            << reqProcSvr->processor()->numNewRequests() << ", "
            << reqProcSvr->processor()->numInProgressRequests() << ", "
            << reqProcSvr->processor()->numFinishedRequests());
    }
    reqProcSvrThread.join();
    fileSvrThread.join();
    ingestSvrThread.join();
    ingestHttpSvrThread.join();
    exportSvrThread.join();
    
    return 0;
}

}}} // namespace lsst::qserv::replica
