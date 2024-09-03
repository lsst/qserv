// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2016 AURA/LSST.
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
#include "wcontrol/Foreman.h"

// System headers
#include <cassert>
#include <thread>

// Third party headers
#include "boost/filesystem.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "qhttp/Server.h"
#include "qhttp/Status.h"
#include "wbase/WorkerCommand.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/ResourceMonitor.h"
#include "wcontrol/SqlConnMgr.h"
#include "wcontrol/WorkerStats.h"
#include "wdb/ChunkResource.h"
#include "wdb/SQLBackend.h"
#include "wpublish/QueriesAndChunks.h"

using namespace std;
namespace fs = boost::filesystem;
namespace qhttp = lsst::qserv::qhttp;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.Foreman");

/// Remove the result file specified in the parameter of the method.
/// @param fileName An absolute path name to a file to be removed.
/// @return The HTTP status code which depends on the status of the requested
///   file and the outcome of the operation.
qhttp::Status removeResultFile(std::string const& fileName) {
    string const context = "Foreman::" + string(__func__) + " ";
    fs::path const filePath(fileName);
    if (!fs::exists(filePath)) return qhttp::STATUS_NOT_FOUND;
    boost::system::error_code ec;
    fs::remove_all(filePath, ec);
    if (ec.value() != 0) {
        LOGS(_log, LOG_LVL_WARN,
             context << "failed to remove the result file: " << fileName << ", code: " << ec.value()
                     << ", error:" << ec.message());
        return qhttp::STATUS_INTERNAL_SERVER_ERR;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "result file removed: " << fileName);
    return qhttp::STATUS_OK;
}
}  // namespace

namespace lsst::qserv::wcontrol {

Foreman::Foreman(Scheduler::Ptr const& scheduler, unsigned int poolSize, unsigned int maxPoolThreads,
                 mysql::MySqlConfig const& mySqlConfig, wpublish::QueriesAndChunks::Ptr const& queries,
                 std::shared_ptr<wpublish::ChunkInventory> const& chunkInventory,
                 std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr)
        : _scheduler(scheduler),
          _mySqlConfig(mySqlConfig),
          _queries(queries),
          _chunkInventory(chunkInventory),
          _sqlConnMgr(sqlConnMgr),
          _resourceMonitor(make_shared<ResourceMonitor>()),
          _io_service(),
          _httpServer(qhttp::Server::create(_io_service, 0 /* grab the first available port */)) {
    // Make the chunk resource mgr
    // Creating backend makes a connection to the database for making temporary tables.
    // It will delete temporary tables that it can identify as being created by a worker.
    // Previous instances of the worker will terminate when they try to use or create temporary tables.
    // Previous instances of the worker should be terminated before a new worker is started.
    _chunkResourceMgr = wdb::ChunkResourceMgr::newMgr(make_shared<wdb::SQLBackend>(_mySqlConfig));

    assert(_scheduler);  // Cannot operate without scheduler.

    LOGS(_log, LOG_LVL_DEBUG, "poolSize=" << poolSize << " maxPoolThreads=" << maxPoolThreads);
    _pool = util::ThreadPool::newThreadPool(poolSize, maxPoolThreads, _scheduler);

    _workerCommandQueue = make_shared<util::CommandQueue>();
    _workerCommandPool = util::ThreadPool::newThreadPool(poolSize, _workerCommandQueue);

    WorkerStats::setup();  // FUTURE: maybe add links to scheduler, _backend, etc?

    _mark = make_shared<util::HoldTrack::Mark>(ERR_LOC, "Forman Test Msg");

    // Read-only access to the result files via the HTTP protocol's method "GET"
    auto const workerConfig = wconfig::WorkerConfig::instance();
    _httpServer->addStaticContent("/*", workerConfig->resultsDirname());
    _httpServer->addHandler("DELETE", "/:file",
                            [](qhttp::Request::Ptr const req, qhttp::Response::Ptr const resp) {
                                resp->sendStatus(::removeResultFile(req->path));
                            });

    // The HTTP server should be started before launching the threads to prevent
    // the thread from exiting prematurely due to a lack of work. The threads
    // will stop automatically when the server will be requested to stop in
    // the destructor of the current class.
    _httpServer->start();
    assert(workerConfig->resultsNumHttpThreads() > 0);
    for (size_t i = 0; i < workerConfig->resultsNumHttpThreads(); ++i) {
        std::thread t([this]() { _io_service.run(); });
        t.detach();
    }
    LOGS(_log, LOG_LVL_DEBUG, "qhttp started on port=" << _httpServer->getPort());
}

Foreman::~Foreman() {
    LOGS(_log, LOG_LVL_DEBUG, "Foreman::~Foreman()");
    // It will take significant effort to have xrootd shutdown cleanly and this will never get called
    // until that happens.
    _pool->shutdownPool();
    _httpServer->stop();
}

wpublish::QueryStatistics::Ptr Foreman::addQueryId(QueryId qId) { return _queries->addQueryId(qId); }

void Foreman::processTasks(vector<wbase::Task::Ptr> const& tasks) {
    std::vector<util::Command::Ptr> cmds;
    for (auto const& task : tasks) {
        _queries->addTask(task);
        cmds.push_back(task);
    }
    _scheduler->queCmd(cmds);
}

void Foreman::processCommand(shared_ptr<wbase::WorkerCommand> const& command) {
    _workerCommandQueue->queCmd(command);
}

uint16_t Foreman::httpPort() const { return _httpServer->getPort(); }

nlohmann::json Foreman::statusToJson(wbase::TaskSelector const& taskSelector) {
    nlohmann::json status;
    status["queries"] = _queries->statusToJson(taskSelector);
    status["sql_conn_mgr"] = _sqlConnMgr->statusToJson();
    return status;
}

}  // namespace lsst::qserv::wcontrol
