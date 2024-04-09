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
#include "replica/contr/Controller.h"

// System headers
#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <sys/types.h>
#include <thread>
#include <vector>
#include <unistd.h>

// Qserv headers
#include "replica/config/ConfigCzar.h"
#include "replica/config/Configuration.h"
#include "replica/config/ConfigWorker.h"
#include "replica/registry/Registry.h"
#include "replica/requests/Request.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"
#include "replica/util/FileUtils.h"
#include "replica/util/Performance.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::chrono_literals;
using namespace lsst::qserv::replica;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Controller");

void tracker(weak_ptr<Controller> const& controller, string const& context) {
    LOGS(_log, LOG_LVL_INFO, context << "started tracking workers.");
    while (true) {
        Controller::Ptr const ptr = controller.lock();
        if (ptr == nullptr) break;

        auto const config = ptr->serviceProvider()->config();
        bool const autoRegisterWorkers =
                config->get<unsigned int>("controller", "auto-register-workers") != 0;
        vector<ConfigWorker> workers;
        try {
            workers = ptr->serviceProvider()->registry()->workers(); //&&& important?
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN,
                 context << "failed to pull worker info from the registry, ex: " << ex.what());
        }
        for (auto&& worker : workers) {
            try {
                if (config->isKnownWorker(worker.name)) {
                    auto const prevWorker = config->worker(worker.name);
                    if (prevWorker != worker) {
                        LOGS(_log, LOG_LVL_INFO,
                             context << "worker '" << worker.name << "' logged in from '" << worker.svcHost
                                     << "'. Updating worker's record in the configuration.");
                        config->updateWorker(worker);
                    }
                } else {
                    if (autoRegisterWorkers) {
                        LOGS(_log, LOG_LVL_INFO,
                             context << "new worker '" << worker.name << "' logged in from '"
                                     << worker.svcHost << "'. Registering new worker in the configuration.");
                        config->addWorker(worker);
                    }
                }
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_WARN,
                     context << "failed to process worker info, worker '" << worker.name
                             << "', ex: " << ex.what());
            }
        }
        bool const autoRegisterCzars = config->get<unsigned int>("controller", "auto-register-czars") != 0;
        vector<ConfigCzar> czars;
        try {
            czars = ptr->serviceProvider()->registry()->czars();
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN,
                 context << "failed to pull Czar info from the registry, ex: " << ex.what());
        }
        for (auto&& czar : czars) {
            try {
                if (config->isKnownCzar(czar.name)) {
                    auto const prevCzar = config->czar(czar.name);
                    if (prevCzar != czar) {
                        LOGS(_log, LOG_LVL_INFO,
                             context << "Czar '" << czar.name << "' logged in from '" << czar.host
                                     << "'. Updating Czar's record in the configuration.");
                        config->updateCzar(czar);
                    }
                } else {
                    if (autoRegisterCzars) {
                        LOGS(_log, LOG_LVL_INFO,
                             context << "new Czar '" << czar.name << "' logged in from '" << czar.host
                                     << "'. Registering new Czar in the configuration.");
                        config->addCzar(czar);
                    }
                }
            } catch (exception const& ex) {
                LOGS(_log, LOG_LVL_WARN,
                     context << "failed to process Czar info, Czar '" << czar.name << "', ex: " << ex.what());
            }
        }
        this_thread::sleep_for(
                chrono::seconds(max(1U, config->get<unsigned int>("registry", "heartbeat-ival-sec"))));
    }
    LOGS(_log, LOG_LVL_INFO, context << "finished tracking workers.");
}

}  // namespace

namespace lsst::qserv::replica {

ostream& operator<<(ostream& os, ControllerIdentity const& identity) {
    os << "ControllerIdentity(id=" << identity.id << ",host=" << identity.host << ",pid=" << identity.pid
       << ")";
    return os;
}

Controller::Ptr Controller::create(shared_ptr<ServiceProvider> const& serviceProvider) {
    auto const ptr = Controller::Ptr(new Controller(serviceProvider));

    // The code below is starting the worker status tracking algorithm that would
    // be running in the detached thread. The thread will cache the weak pointer to
    // the Controller and check its status to see if the Controller is still alive.
    // And if it's not then the thread would terminate. This technique is needed to
    // avoid having the live pointer to the Controller within the thread that would
    // prevent the normal completion of the process.
    //
    // IMPORTANT: updated states of the configuration parameters are obtained at each
    // iteration of the 'for' loop to allow external control over enabling/disabling
    // new workers to join the cluster. Also note that the automatic registration of
    // workers should be only allowed in the Master Replication Controller.
    string const context = ptr->_context(__func__) + "  ";
    weak_ptr<Controller> w = ptr;
    thread t([controller = move(w), context]() { ::tracker(controller, context); });
    t.detach();
    return ptr;
}

Controller::Controller(shared_ptr<ServiceProvider> const& serviceProvider)
        : _serviceProvider(serviceProvider),
          _identity({Generators::uniqueId(), boost::asio::ip::host_name(), getpid()}),
          _startTime(util::TimeUtils::now()) {
    serviceProvider->databaseServices()->saveState(_identity, _startTime);
}

string Controller::_context(string const& func) const {
    return "R-CONTR " + _identity.id + "  " + _identity.host + "[" + to_string(_identity.pid) + "]  " + func;
}

void Controller::verifyFolders(bool createMissingFolders) const {
    vector<string> const folders = {
            serviceProvider()->config()->get<string>("database", "qserv-master-tmp-dir")};
    FileUtils::verifyFolders("CONTROLLER", folders, createMissingFolders);
}

void Controller::_add(std::shared_ptr<Request> const& request) {
    replica::Lock lock(_mtx, _context(__func__));
    _registry[request->id()] = request;
}

void Controller::_remove(std::shared_ptr<Request> const& request) {
    replica::Lock lock(_mtx, _context(__func__));
    _registry.erase(request->id());
}

}  // namespace lsst::qserv::replica
