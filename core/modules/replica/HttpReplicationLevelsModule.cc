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
#include "replica/HttpReplicationLevelsModule.h"

// System headers
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/Performance.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

HttpReplicationLevelsModule::Ptr HttpReplicationLevelsModule::create(
                                    Controller::Ptr const& controller,
                                    string const& taskName,
                                    unsigned int workerResponseTimeoutSec,
                                    HealthMonitorTask::Ptr const& healthMonitorTask) {
    return Ptr(
        new HttpReplicationLevelsModule(
            controller,
            taskName,
            workerResponseTimeoutSec,
            healthMonitorTask
        )
    );
}


HttpReplicationLevelsModule::HttpReplicationLevelsModule(
                                Controller::Ptr const& controller,
                                string const& taskName,
                                unsigned int workerResponseTimeoutSec,
                                HealthMonitorTask::Ptr const& healthMonitorTask)
    :   HttpModule(controller,
                   taskName,
                   workerResponseTimeoutSec),
        _healthMonitorTask(healthMonitorTask) {
}


void HttpReplicationLevelsModule::executeImpl(qhttp::Request::Ptr const& req,
                                              qhttp::Response::Ptr const& resp,
                                              string const& subModuleName) {
    debug(__func__);

    util::Lock lock(_replicationLevelMtx, "HttpReplicationLevelsModule::" + string(__func__));

    // Check if a cached report can be used
    //
    // TODO: add a cache control parameter to the class's constructor

    if (not _replicationLevelReport.is_null()) {
        uint64_t lastReportAgeMs = PerformanceUtils::now() - _replicationLevelReportTimeMs;
        if (lastReportAgeMs < 240 * 1000) {
            sendData(resp, _replicationLevelReport);
            return;
        }
    }

    // Otherwise, get the fresh snapshot of the replica distributions

    auto const config = controller()->serviceProvider()->config();

    auto const healthMonitorTask = _healthMonitorTask.lock();
    if (nullptr == healthMonitorTask) {
        string const msg = "no access to the Health Monitor Task. The service may be shutting down.";
        error(__func__, msg);
        throw runtime_error("HttpReplicationLevelsModule::" + string(__func__) + "  " + msg);
    }
    auto const delays = healthMonitorTask->workerResponseDelay();

    vector<string> disabledQservWorkers;
    vector<string> disabledReplicationWorkers;
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

    json result;
    for (auto&& family: config->databaseFamilies()) {

        size_t const replicationLevel = config->databaseFamilyInfo(family).replicationLevel;
        result["families"][family]["level"] = replicationLevel;

        for (auto&& database: config->databases(family)) {
            debug(string(__func__) + "  database=" + database);

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
                maxObservedLevel = max(maxObservedLevel, entry.first);
                numOnlineQservChunks += entry.second;
            }

            size_t numAllQservChunks = 0;
            for (auto&& entry: allQservLevels) {
                maxObservedLevel = max(maxObservedLevel, entry.first);
                numAllQservChunks += entry.second;
            }

            size_t numOnlineReplicationSystemChunks = numOrphanReplicationSystemChunks;
            for (auto&& entry: onLineReplicationSystemLevels) {
                maxObservedLevel = max(maxObservedLevel, entry.first);
                numOnlineReplicationSystemChunks += entry.second;
            }

            size_t numAllReplicationSystemChunks = 0;
            for (auto&& entry: allReplicationSystemLevels) {
                maxObservedLevel = max(maxObservedLevel, entry.first);
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
                size_t const numChunks = entry.second;
                double const percent = numOnlineQservChunks == 0
                        ? 0. : 100. * numChunks / numOnlineQservChunks;
                databaseJson["levels"][level]["qserv"]["online"]["num_chunks"] = numChunks;
                databaseJson["levels"][level]["qserv"]["online"]["percent"   ] = percent;
            }
            for (auto&& entry: allQservLevels) {
                unsigned int const level = entry.first;
                size_t const numChunks = entry.second;
                double const percent = numAllQservChunks == 0
                        ? 0. : 100. * numChunks / numAllQservChunks;
                databaseJson["levels"][level]["qserv"]["all"]["num_chunks"] = numChunks;
                databaseJson["levels"][level]["qserv"]["all"]["percent"   ] = percent;
            }
            for (auto&& entry: onLineReplicationSystemLevels) {
                unsigned int const level = entry.first;
                size_t const numChunks = entry.second;
                double const percent = numOnlineReplicationSystemChunks == 0
                        ? 0. : 100. * numChunks / numOnlineReplicationSystemChunks;
                databaseJson["levels"][level]["replication"]["online"]["num_chunks"] = numChunks;
                databaseJson["levels"][level]["replication"]["online"]["percent"   ] = percent;
            }
            for (auto&& entry: allReplicationSystemLevels) {
                unsigned int const level = entry.first;
                size_t const numChunks = entry.second;
                double const percent = numAllReplicationSystemChunks == 0
                        ? 0. : 100. * numChunks / numAllReplicationSystemChunks;
                databaseJson["levels"][level]["replication"]["all"]["num_chunks"] = numChunks;
                databaseJson["levels"][level]["replication"]["all"]["percent"   ] = percent;
            }
            {
                double const percent = numAllQservChunks == 0
                        ? 0 : 100. * numOrphanQservChunks / numAllQservChunks;
                databaseJson["levels"][0]["qserv"]["online"]["num_chunks"] = numOrphanQservChunks;
                databaseJson["levels"][0]["qserv"]["online"]["percent"   ] = percent;
            }
            {
                double const percent = numAllReplicationSystemChunks == 0
                        ? 0 : 100. * numOrphanReplicationSystemChunks / numAllReplicationSystemChunks;
                databaseJson["levels"][0]["replication"]["online"]["num_chunks"] = numOrphanReplicationSystemChunks;
                databaseJson["levels"][0]["replication"]["online"]["percent"   ] = percent;
            }
            result["families"][family]["databases"][database] = databaseJson;
        }
    }

    // Update the cache before sending the response

    _replicationLevelReport = result;
    _replicationLevelReportTimeMs = PerformanceUtils::now();

    sendData(resp, _replicationLevelReport);
}

}}}  // namespace lsst::qserv::replica