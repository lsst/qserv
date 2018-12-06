/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/HttpTask.h"

// System headers
#include <atomic>
#include <map>
#include <iomanip>
#include <stdexcept>
#include <sstream>

// Third party headers
#include "nlohmann/json.hpp"
using json = nlohmann::json;

// Qserv headers
#include "util/BlockPost.h"
#include "replica/Controller.h"
#include "replica/DatabaseServices.h"
#include "replica/Performance.h"

namespace lsst {
namespace qserv {
namespace replica {

HttpTask::Ptr HttpTask::create(Controller::Ptr const& controller,
                               Task::AbnormalTerminationCallbackType const& onTerminated,
                               HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                               HealthMonitorTask::Ptr const& healthMonitorTask,
                               ReplicationTask::Ptr const& replicationTask,
                               DeleteWorkerTask::Ptr const& deleteWorkerTask) {
    return Ptr(
        new HttpTask(
            controller,
            onTerminated,
            onWorkerEvict,
            healthMonitorTask,
            replicationTask,
            deleteWorkerTask
        )
    );
}

void HttpTask::run() {

    // Finish initializing the server

    if (not _isInitialized) {
        _isInitialized = true;

        using namespace std::placeholders;

        auto self = shared_from_base<HttpTask>();

        _httpServer->addHandlers({

            // Trivial tests of the API
            {"POST",   "/replication/test",     std::bind(&HttpTask::_testCreate, self, _1, _2)},
            {"GET",    "/replication/test",     std::bind(&HttpTask::_testList,   self, _1, _2)},
            {"GET",    "/replication/test/:id", std::bind(&HttpTask::_testGet,    self, _1, _2)},
            {"PUT",    "/replication/test/:id", std::bind(&HttpTask::_testUpdate, self, _1, _2)},
            {"DELETE", "/replication/test/:id", std::bind(&HttpTask::_testDelete, self, _1, _2)},

            // Replication level summary
            {"GET",    "/replication/v1/level", std::bind(&HttpTask::_getReplicationLevel, self, _1, _2)},

            // Status of all workers or a particular worker
            {"GET",    "/replication/v1/worker",       std::bind(&HttpTask::_listWorkerStatuses, self, _1, _2)},
            {"GET",    "/replication/v1/worker/:name", std::bind(&HttpTask::_getWorkerStatus,    self, _1, _2)},

        });
    }

    // Keep running before stopped

    _httpServer->start();

    util::BlockPost blockPost(1000, 2000);
    while (not stopRequested()) {
        blockPost.wait();
    }
    _httpServer->stop();
}

HttpTask::HttpTask(Controller::Ptr const& controller,
                   Task::AbnormalTerminationCallbackType const& onTerminated,
                   HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                   HealthMonitorTask::Ptr const& healthMonitorTask,
                   ReplicationTask::Ptr const& replicationTask,
                   DeleteWorkerTask::Ptr const& deleteWorkerTask)
    :   Task(controller,
             "HTTP-SERVER  ",
             onTerminated),
        _onWorkerEvict(onWorkerEvict),
        _healthMonitorTask(healthMonitorTask),
        _replicationTask(replicationTask),
        _deleteWorkerTask(deleteWorkerTask),
        _httpServer(
            qhttp::Server::create(
                controller->serviceProvider()->io_service(),
                controller->serviceProvider()->config()->controllerHttpPort()
            )
        ),
        _isInitialized(false) {
}


void HttpTask::_testCreate(qhttp::Request::Ptr req,
                             qhttp::Response::Ptr resp) {
    debug("_testCreate");
    resp->send("_testCreate", "application/json");
}


void HttpTask::_testList(qhttp::Request::Ptr req,
                           qhttp::Response::Ptr resp) {
    debug("_testList");
    resp->send("_testList", "application/json");
}


void HttpTask::_testGet(qhttp::Request::Ptr req,
                          qhttp::Response::Ptr resp) {
    debug("_testGet");
    resp->send("_testGet", "application/json");
}


void HttpTask::_testUpdate(qhttp::Request::Ptr req,
                             qhttp::Response::Ptr resp) {
    debug("_testUpdate");
    resp->send("_testUpdate", "application/json");
}


void HttpTask::_testDelete(qhttp::Request::Ptr req,
                             qhttp::Response::Ptr resp) {
    debug("_testDelete");
    resp->send("_testDelete", "application/json");
}


void HttpTask::_getReplicationLevel(qhttp::Request::Ptr req,
                                      qhttp::Response::Ptr resp) {
    debug("_getReplicationLevel");

    util::Lock lock(_replicationLevelMtx, "HttpTask::_getReplicationLevel");

    // Check if a cached report can be used
    //
    // TODO: add a cache control parameter to the class's constructor

    if (not _replicationLevelReport.empty()) {
        uint64_t lastReportAgeMs = PerformanceUtils::now() - _replicationLevelReportTimeMs;
        if (lastReportAgeMs < 240 * 1000) {
            resp->send(_replicationLevelReport, "application/json");
            return;
        }
    }

    auto const config = controller()->serviceProvider()->config();

    HealthMonitorTask::WorkerResponseDelay const delays =
        _healthMonitorTask->workerResponseDelay();

    std::vector<std::string> disabledQservWorkers;
    std::vector<std::string> disabledReplicationWorkers;
    for (auto&& entry: delays) {
        auto&& worker =  entry.first;

        unsigned int const qservProbeDelaySec = entry.second.at("qserv");
        if (qservProbeDelaySec > 0) {
            disabledQservWorkers.push_back(worker);
        }
        unsigned int const replicationSystemProbeDelaySec = entry.second.at("replication");
        if (replicationSystemProbeDelaySec > 0) {
            disabledReplicationWorkers.push_back(worker);
        }
    }
    
    json resultJson;
    for (auto&& family: config->databaseFamilies()) {

        size_t const replicationLevel = config->databaseFamilyInfo(family).replicationLevel;
        resultJson["families"][family]["level"] = replicationLevel;

        for (auto&& database: config->databases(family)) {
            debug("_getReplicationLevel  database=" + database);

            // Get observed replication levels for workers which are on-line
            // as well as for the whole cluster (if there in-active workers).

            auto const onlineQservLevels =
                controller()->serviceProvider()->databaseServices()->actualReplicationLevel(
                    database,
                    disabledQservWorkers);

            auto const allQservLevels = disabledQservWorkers.empty() ?
                onlineQservLevels : 
                controller()->serviceProvider()->databaseServices()->actualReplicationLevel(
                    database);

            auto const onLineReplicationSystemLevels =
                controller()->serviceProvider()->databaseServices()->actualReplicationLevel(
                    database,
                    disabledReplicationWorkers);

            auto const allReplicationSystemLevels = disabledReplicationWorkers.empty() ?
                onLineReplicationSystemLevels :
                controller()->serviceProvider()->databaseServices()->actualReplicationLevel(
                    database);

            // Get the numbers of 'orphan' chunks in each context. These chunks (if any)
            // will be associated with the replication level 0. Also note, that these
            // chunks will be contributing into the total number of chunks when computing
            // the percentage of each replication level.

            size_t const numOrphanQservChunks = disabledQservWorkers.empty() ?
                0 :
                controller()->serviceProvider()->databaseServices()->numOrphanChunks(
                    database,
                    disabledQservWorkers);

            size_t const numOrphanReplicationSystemChunks = disabledReplicationWorkers.empty() ?
                0 :
                controller()->serviceProvider()->databaseServices()->numOrphanChunks(
                    database,
                    disabledReplicationWorkers);

            // The maximum level is needed to initialize result with zeros for
            // a contiguous range of levels [0,maxObservedLevel]. The non-empty
            // cells will be filled from the above captured reports.
            //
            // Also, while doing so compute the total number of chunks in each context.

            unsigned int maxObservedLevel = 0;

            size_t numOnlineQservChunks = numOrphanQservChunks;
            for (auto&& entry: onlineQservLevels) {
                maxObservedLevel = std::max(maxObservedLevel, entry.first);
                numOnlineQservChunks += entry.second;
            }

            size_t numAllQservChunks = 0;
            for (auto&& entry: allQservLevels) {
                maxObservedLevel = std::max(maxObservedLevel, entry.first);
                numAllQservChunks += entry.second;
            }

            size_t numOnlineReplicationSystemChunks = numOrphanReplicationSystemChunks;
            for (auto&& entry: onLineReplicationSystemLevels) {
                maxObservedLevel = std::max(maxObservedLevel, entry.first);
                numOnlineReplicationSystemChunks += entry.second;
            }

            size_t numAllReplicationSystemChunks = 0;
            for (auto&& entry: allReplicationSystemLevels) {
                maxObservedLevel = std::max(maxObservedLevel, entry.first);
                numAllReplicationSystemChunks += entry.second;
            }

            // Pre-initialize the database-specific result with zeroes for all
            // levels in the range of [0,maxObservedLevel]

            json databaseJson;

            for (int level = maxObservedLevel; level >= 0; --level) {
                databaseJson["levels"][level]["qserv"      ]["online"]["num_chunks"] = 0;
                databaseJson["levels"][level]["qserv"      ]["online"]["percent"   ] = 0.;
                databaseJson["levels"][level]["qserv"      ]["all"   ]["num_chunks"] = 0;
                databaseJson["levels"][level]["qserv"      ]["all"   ]["percent"   ] = 0.;
                databaseJson["levels"][level]["replication"]["online"]["num_chunks"] = 0;
                databaseJson["levels"][level]["replication"]["online"]["percent"   ] = 0.;
                databaseJson["levels"][level]["replication"]["all"   ]["num_chunks"] = 0;
                databaseJson["levels"][level]["replication"]["all"   ]["percent"   ] = 0.;
            }

            // Fill-in non-blank areas

            for (auto&& entry: onlineQservLevels) {
                unsigned int const level = entry.first;
                size_t const numChunks   = entry.second;
                double const percent     = 100. * numChunks / numOnlineQservChunks;
                databaseJson["levels"][level]["qserv"      ]["online"]["num_chunks"] = numChunks;
                databaseJson["levels"][level]["qserv"      ]["online"]["percent"   ] = percent;
            }
            for (auto&& entry: allQservLevels) {
                unsigned int const level = entry.first;
                size_t const numChunks   = entry.second;
                double const percent     = 100. * numChunks / numAllQservChunks;
                databaseJson["levels"][level]["qserv"      ]["all"   ]["num_chunks"] = numChunks;
                databaseJson["levels"][level]["qserv"      ]["all"   ]["percent"   ] = percent;
            }
            for (auto&& entry: onLineReplicationSystemLevels) {
                unsigned int const level = entry.first;
                size_t const numChunks   = entry.second;
                double const percent     = 100. * numChunks / numOnlineReplicationSystemChunks;
                databaseJson["levels"][level]["replication"]["online"]["num_chunks"] = numChunks;
                databaseJson["levels"][level]["replication"]["online"]["percent"   ] = percent;
            }
            for (auto&& entry: allReplicationSystemLevels) {
                unsigned int const level = entry.first;
                size_t const numChunks   = entry.second;
                double const percent     = 100. * numChunks / numAllReplicationSystemChunks;
                databaseJson["levels"][level]["replication"]["all"   ]["num_chunks"] = numChunks;
                databaseJson["levels"][level]["replication"]["all"   ]["percent"   ] = percent;
            }
            {
                double const percent = 100. * numOrphanQservChunks / numAllQservChunks;
                databaseJson["levels"][0]["qserv"      ]["all"   ]["num_chunks"] = numOrphanQservChunks;
                databaseJson["levels"][0]["qserv"      ]["all"   ]["percent"   ] = percent;
            }
            {
                double const percent     = 100. * numOrphanReplicationSystemChunks / numAllReplicationSystemChunks;
                databaseJson["levels"][0]["replication"]["all"   ]["num_chunks"] = numOrphanReplicationSystemChunks;
                databaseJson["levels"][0]["replication"]["all"   ]["percent"   ] = percent;
            }
            resultJson["families"][family]["databases"][database] = databaseJson;
        }
    }
    
    // Update the cache
    _replicationLevelReport = resultJson.dump();
    _replicationLevelReportTimeMs = PerformanceUtils::now();

    resp->send(_replicationLevelReport, "application/json");
}


void HttpTask::_listWorkerStatuses(qhttp::Request::Ptr req,
                                     qhttp::Response::Ptr resp) {
    debug("_listWorkerStatuses");

    HealthMonitorTask::WorkerResponseDelay delays =
        _healthMonitorTask->workerResponseDelay();

    json resultJson = json::array();
    for (auto&& worker: controller()->serviceProvider()->config()->allWorkers()) {

        json workerJson;

        workerJson["worker"] = worker;

        WorkerInfo const info =
            controller()->serviceProvider()->config()->workerInfo(worker);

        uint64_t const numReplicas =
            controller()->serviceProvider()->databaseServices()->numWorkerReplicas(worker);

        workerJson["replication"]["num_replicas"] = numReplicas;
        workerJson["replication"]["isEnabled"]    = info.isEnabled  ? 1 : 0;
        workerJson["replication"]["isReadOnly"]   = info.isReadOnly ? 1 : 0;

        auto itr = delays.find(worker);
        if (delays.end() != itr) {
            workerJson["replication"]["probe_delay_s"] = itr->second["replication"];
            workerJson["qserv"      ]["probe_delay_s"] = itr->second["qserv"];
        } else {
            workerJson["replication"]["probe_delay_s"] = 0;
            workerJson["qserv"      ]["probe_delay_s"] = 0;
        }
        resultJson.push_back(workerJson);
    }
    resp->send(resultJson.dump(), "application/json");
}


void HttpTask::_getWorkerStatus(qhttp::Request::Ptr req,
                                  qhttp::Response::Ptr resp) {
    debug("_getWorkerStatus");
    resp->send(json::array(), "application/json");
}

}}} // namespace lsst::qserv::replica
