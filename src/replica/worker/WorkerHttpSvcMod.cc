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
#include "replica/worker/WorkerHttpSvcMod.h"

// System headers
#include <stdexcept>

// Third-party headers
#include <httplib.h>

// Qserv header
#include "http/Auth.h"
#include "http/Method.h"
#include "replica/proto/Protocol.h"
#include "replica/worker/WorkerHttpProcessor.h"
#include "replica/services/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void WorkerHttpSvcMod::process(shared_ptr<ServiceProvider> const& serviceProvider,
                               shared_ptr<WorkerHttpProcessor> const& processor, string const& workerName,
                               httplib::Request const& req, httplib::Response& resp,
                               string const& subModuleName, http::AuthType const authType) {
    WorkerHttpSvcMod module(serviceProvider, processor, workerName, req, resp);
    module.execute(subModuleName, authType);
}

WorkerHttpSvcMod::WorkerHttpSvcMod(shared_ptr<ServiceProvider> const& serviceProvider,
                                   shared_ptr<WorkerHttpProcessor> const& processor, string const& workerName,
                                   httplib::Request const& req, httplib::Response& resp)
        : http::ChttpModule(serviceProvider->httpAuthContext(), req, resp),
          _serviceProvider(serviceProvider),
          _processor(processor),
          _workerName(workerName) {}

string WorkerHttpSvcMod::context() const { return "WORKER-HTTP-SVC "; }

json WorkerHttpSvcMod::executeImpl(string const& subModuleName) {
    debug(__func__, "subModuleName: '" + subModuleName + "'");
    enforceInstanceId(__func__, _serviceProvider->instanceId());
    if (subModuleName == "ECHO")
        return _echo();
    else if (subModuleName == "REPLICA-CREATE")
        return _createReplica();
    else if (subModuleName == "REPLICA-DELETE")
        return _deleteReplica();
    else if (subModuleName == "REPLICA-FIND")
        return _findReplica();
    else if (subModuleName == "REPLICA-FIND-ALL")
        return _findAllReplicas();
    else if (subModuleName == "SQL")
        return _sql();
    else if (subModuleName == "INDEX")
        return _index();
    else if (subModuleName == "REQUEST-TRACK")
        return _trackRequest();
    else if (subModuleName == "REQUEST-STATUS")
        return _statusOfRequests();
    else if (subModuleName == "REQUEST-STOP")
        return _stopRequest();
    else if (subModuleName == "REQUEST-DISPOSE")
        return _disposeRequests();
    else if (subModuleName == "SERVICE-SUSPEND")
        return _suspendService();
    else if (subModuleName == "SERVICE-RESUME")
        return _resumeService();
    else if (subModuleName == "SERVICE-STATUS")
        return _getServiceStatus();
    else if (subModuleName == "SERVICE-REQUESTS")
        return _getRequests();
    else if (subModuleName == "SERVICE-DRAIN")
        return _drainService();
    else if (subModuleName == "SERVICE-RECONFIG")
        return _reconfigService();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

protocol::QueuedRequestHdr WorkerHttpSvcMod::_parseHdr(string const& func) const {
    protocol::QueuedRequestHdr const hdr(body().required<string>("id"), body().optional<int>("priority", 0),
                                         body().optional<unsigned int>("timeout", 0));
    debug(func, "id:       '" + hdr.id + "'");
    debug(func, "priority: " + to_string(hdr.priority));
    debug(func, "timeout:  " + to_string(hdr.timeout));
    return hdr;
}

protocol::RequestParams WorkerHttpSvcMod::_reqParams() const {
    return protocol::RequestParams(body().required<json>("req"));
}

json WorkerHttpSvcMod::_echo() const {
    debug(__func__);
    checkApiVersion(__func__, 56);
    return _processor->echo(_parseHdr(__func__), _reqParams());
}

json WorkerHttpSvcMod::_createReplica() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    return _processor->createReplica(_parseHdr(__func__), _reqParams());
}

json WorkerHttpSvcMod::_deleteReplica() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    return _processor->deleteReplica(_parseHdr(__func__), _reqParams());
}

json WorkerHttpSvcMod::_findReplica() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    return _processor->findReplica(_parseHdr(__func__), _reqParams());
}

json WorkerHttpSvcMod::_findAllReplicas() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    return _processor->findAllReplicas(_parseHdr(__func__), _reqParams());
}

json WorkerHttpSvcMod::_index() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    return _processor->index(_parseHdr(__func__), _reqParams());
}

json WorkerHttpSvcMod::_sql() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    return _processor->sql(_parseHdr(__func__), _reqParams());
}

json WorkerHttpSvcMod::_trackRequest() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    string const id = params().at("id");
    debug(__func__, "id: '" + id + "'");
    return _processor->trackRequest(id);
}

json WorkerHttpSvcMod::_statusOfRequests() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    string const id = params().at("id");
    debug(__func__, "id: '" + id + "'");
    return _processor->statusOfRequests(id);
}

json WorkerHttpSvcMod::_stopRequest() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    string const id = params().at("id");
    debug(__func__, "id: '" + id + "'");
    return _processor->stopRequest(id);
}

json WorkerHttpSvcMod::_disposeRequests() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    auto const idsJson = body().required<json>("ids");
    if (!idsJson.is_array())
        throw invalid_argument(context() + "::" + string(__func__) + "  'ids' is not an array");

    json idsDisposedJson = json::object();
    for (auto const& idJson : idsJson) {
        string const id = idJson.get<string>();
        idsDisposedJson[id] = _processor->disposeRequest(id) ? 1 : 0;
    }
    return json::object({{"req", json::object({{"id", ""},
                                               {"priority", 0},
                                               {"timeout", 0},
                                               {"params", json::object({{"ids", idsJson}})}})},
                         {"resp", json::object({{"type", ""},
                                                {"expiration_timeout_sec", 0},
                                                {"status", protocol::toString(protocol::Status::SUCCESS)},
                                                {"status_ext", protocol::toString(protocol::StatusExt::NONE)},
                                                {"error", ""},
                                                {"result", json::object({{"ids_disposed", idsDisposedJson}})},
                                                {"perf", WorkerPerformance::finishedToJson()}})}});
}

json WorkerHttpSvcMod::_suspendService() {
    debug(__func__);
    checkApiVersion(__func__, 56);

    // This operation is allowed to be asynchronous as it may take
    // extra time for the processor's threads to finish on-going processing
    _processor->stop();
    return _processor->toJson("SVC_SUSPEND",
                              _processor->state() == protocol::ServiceState::SUSPEND_IN_PROGRESS ||
                                              _processor->state() == protocol::ServiceState::SUSPENDED
                                      ? protocol::Status::SUCCESS
                                      : protocol::Status::FAILED);
}

json WorkerHttpSvcMod::_resumeService() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    _processor->run();
    return _processor->toJson("SVC_RESUME", _processor->state() == protocol::ServiceState::RUNNING
                                                    ? protocol::Status::SUCCESS
                                                    : protocol::Status::FAILED);
}

json WorkerHttpSvcMod::_getServiceStatus() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    return _processor->toJson("SVC_STATUS", protocol::Status::SUCCESS);
}

json WorkerHttpSvcMod::_getRequests() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    const bool includeRequests = true;
    return _processor->toJson("SVC_REQUESTS", protocol::Status::SUCCESS, includeRequests);
}

json WorkerHttpSvcMod::_drainService() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    _processor->drain();
    const bool includeRequests = true;
    return _processor->toJson("SVC_DRAIN", protocol::Status::SUCCESS, includeRequests);
}

json WorkerHttpSvcMod::_reconfigService() {
    debug(__func__);
    checkApiVersion(__func__, 56);
    _processor->reconfig();
    return _processor->toJson("SVC_RECONFIG", protocol::Status::SUCCESS);
}

}  // namespace lsst::qserv::replica
