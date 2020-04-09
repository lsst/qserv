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
#include "replica/HttpWorkerStatusModule.h"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

HttpWorkerStatusModule::Ptr HttpWorkerStatusModule::create(
                                Controller::Ptr const& controller,
                                string const& taskName,
                                HttpProcessorConfig const& processorConfig,
                                HealthMonitorTask::Ptr const& healthMonitorTask) {
    return Ptr(new HttpWorkerStatusModule(
        controller, taskName, processorConfig,
        healthMonitorTask
    ));
}


HttpWorkerStatusModule::HttpWorkerStatusModule(
                            Controller::Ptr const& controller,
                            string const& taskName,
                            HttpProcessorConfig const& processorConfig,
                            HealthMonitorTask::Ptr const& healthMonitorTask)
    :   HttpModule(controller, taskName, processorConfig),
        _healthMonitorTask(healthMonitorTask) {
}


void HttpWorkerStatusModule::executeImpl(string const& subModuleName) {
    debug(__func__);

    auto const healthMonitorTask = _healthMonitorTask.lock();
    if (nullptr == healthMonitorTask) {
        string const msg = "no access to the Health Monitor Task. The service may be shutting down.";
        error(__func__, msg);
        throw runtime_error("HttpWorkerStatusModule::" + string(__func__) + "  " + msg);
    }
    auto const delays = healthMonitorTask->workerResponseDelay();

    json workersJson = json::array();
    for (auto&& worker: controller()->serviceProvider()->config()->allWorkers()) {

        json workerJson;

        workerJson["worker"] = worker;

        WorkerInfo const info = controller()->serviceProvider()->config()->workerInfo(worker);
        uint64_t const numReplicas = controller()->serviceProvider()->databaseServices()->numWorkerReplicas(worker);

        workerJson["replication"]["num_replicas"] = numReplicas;
        workerJson["replication"]["isEnabled"]    = info.isEnabled  ? 1 : 0;
        workerJson["replication"]["isReadOnly"]   = info.isReadOnly ? 1 : 0;

        auto&& itr = delays.find(worker);
        if (delays.end() != itr) {
            workerJson["replication"]["probe_delay_s"] = itr->second.at("replication");
            workerJson["qserv"      ]["probe_delay_s"] = itr->second.at("qserv");
        } else {
            workerJson["replication"]["probe_delay_s"] = 0;
            workerJson["qserv"      ]["probe_delay_s"] = 0;
        }
        workersJson.push_back(workerJson);
    }
    json result;
    result["workers"] = workersJson;

    sendData(result);
}

}}}  // namespace lsst::qserv::replica

