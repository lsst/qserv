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
#include "replica/HttpThread.h"

// System headers
#include <atomic>
#include <map>

// Third party headers
#include "nlohmann/json.hpp"
using json = nlohmann::json;

// Qserv headers
#include "util/BlockPost.h"
#include "Controller.h"

namespace lsst {
namespace qserv {
namespace replica {

HttpThread::Ptr HttpThread::create(Controller::Ptr const& controller,
                                   ControlThread::CallbackType const& onTerminated,
                                   HealthMonitorThread::WorkerEvictCallbackType const& onWorkerEvict,
                                   HealthMonitorThread::Ptr const& healthMonitorThread,
                                   ReplicationThread::Ptr const& replicationThread,
                                   DeleteWorkerThread::Ptr const& deleteWorkerThread) {
    return Ptr(
        new HttpThread(
            controller,
            onTerminated,
            onWorkerEvict,
            healthMonitorThread,
            replicationThread,
            deleteWorkerThread
        )
    );
}

void HttpThread::run() {

    // Finish initializing the server

    if (not _isInitialized) {
        _isInitialized = true;

        using namespace std::placeholders;

        auto self = shared_from_base<HttpThread>();

        _httpServer->addHandlers({

            // Trivial tests of the API
            {"POST",   "/replication/test",     std::bind(&HttpThread::_create, self, _1, _2)},
            {"GET",    "/replication/test",     std::bind(&HttpThread::_list,   self, _1, _2)},
            {"GET",    "/replication/test/:id", std::bind(&HttpThread::_get,    self, _1, _2)},
            {"PUT",    "/replication/test/:id", std::bind(&HttpThread::_update, self, _1, _2)},
            {"DELETE", "/replication/test/:id", std::bind(&HttpThread::_delete, self, _1, _2)},

            // Status of the 
            {"GET",    "/replication/v1/worker",       std::bind(&HttpThread::_listWorkerStatuses, self, _1, _2)},
            {"GET",    "/replication/v1/worker/:name", std::bind(&HttpThread::_getWorkerStatus,    self, _1, _2)},
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

HttpThread::HttpThread(Controller::Ptr const& controller,
                       ControlThread::CallbackType const& onTerminated,
                       HealthMonitorThread::WorkerEvictCallbackType const& onWorkerEvict,
                       HealthMonitorThread::Ptr const& healthMonitorThread,
                       ReplicationThread::Ptr const& replicationThread,
                       DeleteWorkerThread::Ptr const& deleteWorkerThread)
    :   ControlThread(controller,
                      "HTTP-SERVER  ",
                      onTerminated),
        _onWorkerEvict(onWorkerEvict),
        _healthMonitorThread(healthMonitorThread),
        _replicationThread(replicationThread),
        _deleteWorkerThread(deleteWorkerThread),
        _httpServer(
            qhttp::Server::create(
                controller->serviceProvider()->io_service(),
                controller->serviceProvider()->config()->controllerHttpPort()
            )
        ),
        _isInitialized(false) {
}


void HttpThread::_create(qhttp::Request::Ptr req,
                         qhttp::Response::Ptr resp) {
    debug("_create");
    resp->send("_create", "application/json");
}


void HttpThread::_list(qhttp::Request::Ptr req,
                       qhttp::Response::Ptr resp) {
    debug("_list");
    resp->send("_list", "application/json");
}


void HttpThread::_get(qhttp::Request::Ptr req,
                      qhttp::Response::Ptr resp) {
    debug("_get");
    resp->send("_get", "application/json");
}


void HttpThread::_update(qhttp::Request::Ptr req,
                         qhttp::Response::Ptr resp) {
    debug("_update");
    resp->send("_update", "application/json");
}


void HttpThread::_delete(qhttp::Request::Ptr req,
                         qhttp::Response::Ptr resp) {
    debug("_delete");
    resp->send("_delete", "application/json");
}

void HttpThread::_listWorkerStatuses(qhttp::Request::Ptr req,
                                     qhttp::Response::Ptr resp) {
    debug("_listWorkerStatuses");

    json workers = json::array();
    for (auto&& worker: controller()->serviceProvider()->config()->workers()) {
        workers.push_back({
            {"worker", worker},
            {"replica_probe_delay_s", 10},
            {"qserv_probe_delay_s", 10}
        });
    }
    resp->send(workers.dump(), "application/json");
}

void HttpThread::_getWorkerStatus(qhttp::Request::Ptr req,
                                  qhttp::Response::Ptr resp) {
    debug("_getWorkerStatus");
    resp->send(json::array(), "application/json");
}
        
}}} // namespace lsst::qserv::replica
