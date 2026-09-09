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
#include "replica/apps/MasterControllerHttpApp.h"

// System headers
#include <chrono>
#include <functional>
#include <stdexcept>
#include <thread>

// Third-party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/Auth.h"
#include "http/Client.h"
#include "http/MetaModule.h"
#include "http/Method.h"
#include "replica/config/Configuration.h"
#include "replica/config/ConfigurationSchemaController.h"
#include "replica/contr/Controller.h"
#include "replica/contr/DeleteWorkerTask.h"
#include "replica/contr/HealthMonitorTask.h"
#include "replica/contr/HttpProcessor.h"
#include "replica/contr/ReplicationTask.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "util/common.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::chrono_literals;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.MasterControllerHttpApp");

string const description =
        "This application is the Master Replication Controller which has"
        " a built-in Cluster Health Monitor and a linear Replication loop."
        " The Monitor would track a status of both Qserv and Replication workers"
        " and trigger the worker exclusion sequence if both services were found"
        " non-responsive within a configured interval."
        " The interval is specified via the corresponding command-line option."
        " And it also has some built-in default value."
        " Also, note that only a single node failure can trigger the worker"
        " exclusion sequence."
        " The controller has the built-in REST API which accepts external commands"
        " or request for information.";

bool const enableServiceProvider = true;

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<MasterControllerHttpApp> MasterControllerHttpApp::create(int argc, char* argv[]) {
    return shared_ptr<MasterControllerHttpApp>(new MasterControllerHttpApp(argc, argv));
}

MasterControllerHttpApp::MasterControllerHttpApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::enableServiceProvider, ConfigurationSchemaController()) {}

string MasterControllerHttpApp::_controllerName4log() const {
    auto const config = _controller->serviceProvider()->config();
    return "CONTROLLER[" + config->get<std::string>("controller", "name") + "]";
}

int MasterControllerHttpApp::runImpl() {
    _controller = Controller::create(serviceProvider());

    // ATTENTION: Controller depends on a number of folders that are used for
    // storing intermediate files of various sizes. Locations (absolute path names)
    // of the folders are set in the corresponding configuration parameters.
    // Desired characteristics (including size, I/O latency, I/O bandwidth, etc.) of
    // the folders may vary depending on a type of the Controller's operation and
    // a scale of a particular Qserv deployment. Note that the overall performance
    // and scalability greatly depends on the quality of of the underlying filesystems.
    // Usually, in the large-scale deployments, the folders should be pre-created and be placed
    // at the large-capacity high-performance filesystems at the Qserv deployment time.
    auto const config = _controller->serviceProvider()->config();
    _controller->verifyFolders(config->get<unsigned int>("controller", "create-folders") != 0);

    _logControllerStartedEvent();

    // These tasks should be running in parallel

    auto self = shared_from_base<MasterControllerHttpApp>();

    _replicationTask =
            ReplicationTask::create(_controller, [self](Task::Ptr const& ptr) { self->_isFailed.fail(); });
    _replicationTask->start();

    _healthMonitorTask = HealthMonitorTask::create(
            _controller, [self](Task::Ptr const& ptr) { self->_isFailed.fail(); },
            [self](string const& worker2evict) { self->_evict(worker2evict); });
    _healthMonitorTask->start();

    // Running the REST server in its own thread
    auto const httpProcessor = HttpProcessor::create(_controller, _healthMonitorTask);
    thread ingestHttpSvrThread([httpProcessor]() { httpProcessor->run(); });

    // Keep sending periodic 'heartbeats' to the Registry service to report a configuration
    // and a status of the current Controller before a catastrophic failure is reported by
    // any activity.
    _registryUpdateLoop();

    // Stop all threads if any are still running
    _healthMonitorTask->stop();
    _replicationTask->stop();
    httpProcessor->stop();

    ingestHttpSvrThread.join();

    if ((_replicationTask != nullptr) && _replicationTask->isRunning()) _replicationTask->stop();
    _logControllerStoppedEvent();

    // Cancel all outstanding requests to workers (if any)
    _controller->stop();

    return 1;
}

void MasterControllerHttpApp::_evict(string const& worker) {
    _logWorkerEvictionStartedEvent(worker);

    // This thread needs to be stopped to avoid any interference with
    // the worker exclusion protocol.
    _replicationTask->stop();

    // This thread will be allowed to run for as long as it's permitted by
    // the corresponding timeouts set for Requests and Jobs in the Configuration,
    // or until a catastrophic failure occurs within any control thread (including
    // this one).
    auto self = shared_from_base<MasterControllerHttpApp>();

    _deleteWorkerTask = DeleteWorkerTask::create(
            _controller, [self](Task::Ptr const& ptr) { self->_isFailed.fail(); }, worker);
    _deleteWorkerTask->startAndWait([self](Task::Ptr const& ptr) -> bool { return self->_isFailed(); });
    _deleteWorkerTask->stop();  // it's safe to call this method even if the thread is
                                // no longer running.

    _deleteWorkerTask = nullptr;  // the object is no longer needed because it was
                                  // created for a specific worker.

    // Resume the normal replication sequence unless a catastrophic failure
    // in the system has been detected
    if (not _isFailed()) _replicationTask->start();

    _logWorkerEvictionFinishedEvent(worker);
}

void MasterControllerHttpApp::_logControllerStartedEvent() const {
    _assertIsStarted(__func__);
    auto const config = _controller->serviceProvider()->config();
    ControllerEvent event;
    event.status = "STARTED";
    event.kvInfo.emplace_back("name", config->get<string>("controller", "name"));
    event.kvInfo.emplace_back("host", _controller->identity().host);
    event.kvInfo.emplace_back("pid", to_string(_controller->identity().pid));
    event.kvInfo.emplace_back("health-probe-interval",
                              to_string(config->get<unsigned int>("controller", "health-probe-interval")));
    event.kvInfo.emplace_back("replication-interval",
                              to_string(config->get<unsigned int>("controller", "replication-interval")));
    event.kvInfo.emplace_back("czar-response-timeout",
                              to_string(config->get<unsigned int>("controller", "czar-response-timeout")));
    event.kvInfo.emplace_back("worker-response-timeout",
                              to_string(config->get<unsigned int>("controller", "worker-response-timeout")));
    event.kvInfo.emplace_back("worker-evict-timeout",
                              to_string(config->get<unsigned int>("controller", "worker-evict-timeout")));
    event.kvInfo.emplace_back("qserv-sync-timeout",
                              to_string(config->get<unsigned int>("controller", "qserv-sync-timeout")));
    event.kvInfo.emplace_back("qserv-sync-force",
                              to_string(config->get<unsigned int>("controller", "qserv-sync-force")));
    event.kvInfo.emplace_back("worker-config-timeout",
                              to_string(config->get<unsigned int>("controller", "worker-config-timeout")));
    event.kvInfo.emplace_back("purge-excess-replicas",
                              to_string(config->get<unsigned int>("controller", "purge-excess-replicas")));
    event.kvInfo.emplace_back("permanent-worker-delete",
                              to_string(config->get<unsigned int>("controller", "permanent-worker-delete")));
    _logEvent(event);
}

void MasterControllerHttpApp::_logControllerStoppedEvent() const {
    _assertIsStarted(__func__);
    ControllerEvent event;
    event.status = "STOPPED";
    _logEvent(event);
}

void MasterControllerHttpApp::_logWorkerEvictionStartedEvent(string const& worker) const {
    _assertIsStarted(__func__);
    ControllerEvent event;
    event.operation = "worker eviction";
    event.status = "STARTED";
    event.kvInfo.emplace_back("worker", worker);
    _logEvent(event);
}

void MasterControllerHttpApp::_logWorkerEvictionFinishedEvent(string const& worker) const {
    _assertIsStarted(__func__);
    ControllerEvent event;
    event.operation = "worker eviction";
    event.status = "FINISHED";
    event.kvInfo.emplace_back("worker", worker);
    _logEvent(event);
}

void MasterControllerHttpApp::_logEvent(ControllerEvent& event) const {
    event.controllerId = _controller->identity().id;
    event.timeStamp = util::TimeUtils::now();
    event.task = _controllerName4log();

    // For now ignore exceptions when logging events. Just report errors.
    try {
        serviceProvider()->databaseServices()->logControllerEvent(event);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, _controllerName4log() << "  failed to log event in " << __func__);
    }
}

void MasterControllerHttpApp::_assertIsStarted(string const& func) const {
    if (nullptr == _controller) {
        throw logic_error("MasterControllerHttpApp::" + func + "  Controller is not running");
    }
}

void MasterControllerHttpApp::_registryUpdateLoop() {
    auto const serviceProvider = _controller->serviceProvider();
    auto const config = serviceProvider->config();
    auto const method = http::Method::POST;
    string const url = "http://" + config->get<string>("registry", "host") + ":" +
                       to_string(config->get<uint16_t>("registry", "port")) + "/controller";
    vector<string> const headers = {"Content-Type: application/json"};
    json const request = json::object({{"version", http::MetaModule::version},
                                       {"instance_id", config->get<string>("security", "instance-id")},
                                       {"auth_key", config->httpAuthContext().authKey},
                                       {"controller",
                                        {{"name", config->get<string>("controller", "name")},
                                         {"id", _controller->identity().id},
                                         {"port", config->get<uint16_t>("controller", "http-server-port")},
                                         {"host-name", util::get_current_host_fqdn()}}}});
    string const requestContext =
            _controllerName4log() + ": '" + http::method2string(method) + "' request to '" + url + "'";
    http::Client client(method, url, request.dump(), headers);
    while (!_isFailed()) {
        try {
            json const response = client.readAsJson();
            if (0 == response.at("success").get<int>()) {
                string const error = response.at("error").get<string>();
                LOGS(_log, LOG_LVL_WARN, requestContext + " was denied, error: '" + error + "'.");
            }
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN, requestContext + " failed, ex: " + ex.what());
        }
        this_thread::sleep_for(
                chrono::seconds(max(1U, config->get<unsigned int>("registry", "heartbeat-ival-sec"))));
    }
}

}  // namespace lsst::qserv::replica
