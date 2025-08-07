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
#include "replica/qserv/QservMgtServices.h"

// System headers
#include <stdexcept>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservMgtServices");

}  // namespace

namespace lsst::qserv::replica {

QservMgtServices::Ptr QservMgtServices::create(shared_ptr<ServiceProvider> const& serviceProvider) {
    return QservMgtServices::Ptr(new QservMgtServices(serviceProvider));
}

QservMgtServices::QservMgtServices(shared_ptr<ServiceProvider> const& serviceProvider)
        : _serviceProvider(serviceProvider) {}

AddReplicaQservMgtRequest::Ptr QservMgtServices::addReplica(
        unsigned int chunk, vector<string> const& databases, string const& worker,
        AddReplicaQservMgtRequest::CallbackType const& onFinish, string const& jobId,
        unsigned int requestExpirationIvalSec) {
    auto const request = AddReplicaQservMgtRequest::create(
            serviceProvider(), worker, chunk, databases,
            [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

RemoveReplicaQservMgtRequest::Ptr QservMgtServices::removeReplica(
        unsigned int chunk, vector<string> const& databases, string const& worker, bool force,
        RemoveReplicaQservMgtRequest::CallbackType const& onFinish, string const& jobId,
        unsigned int requestExpirationIvalSec) {
    auto const request = RemoveReplicaQservMgtRequest::create(
            serviceProvider(), worker, chunk, databases, force,
            [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

GetReplicasQservMgtRequest::Ptr QservMgtServices::getReplicas(
        string const& databaseFamily, string const& worker, bool inUseOnly, string const& jobId,
        GetReplicasQservMgtRequest::CallbackType const& onFinish, unsigned int requestExpirationIvalSec) {
    auto const request = GetReplicasQservMgtRequest::create(
            serviceProvider(), worker, databaseFamily, inUseOnly,
            [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

SetReplicasQservMgtRequest::Ptr QservMgtServices::setReplicas(
        string const& worker, QservReplicaCollection const& newReplicas, vector<string> const& databases,
        bool force, string const& jobId, SetReplicasQservMgtRequest::CallbackType const& onFinish,
        unsigned int requestExpirationIvalSec) {
    auto const request = SetReplicasQservMgtRequest::create(
            serviceProvider(), worker, newReplicas, databases, force,
            [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

TestEchoQservMgtRequest::Ptr QservMgtServices::echo(string const& worker, string const& data,
                                                    string const& jobId,
                                                    TestEchoQservMgtRequest::CallbackType const& onFinish,
                                                    unsigned int requestExpirationIvalSec) {
    auto const request =
            TestEchoQservMgtRequest::create(serviceProvider(), worker, data,
                                            [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                                                self->_finish(request->id());
                                            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

GetStatusQservMgtRequest::Ptr QservMgtServices::status(string const& worker, string const& jobId,
                                                       wbase::TaskSelector const& taskSelector,
                                                       GetStatusQservMgtRequest::CallbackType const& onFinish,
                                                       unsigned int requestExpirationIvalSec) {
    auto const request = GetStatusQservMgtRequest::create(
            serviceProvider(), worker, taskSelector,
            [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

GetDbStatusQservMgtRequest::Ptr QservMgtServices::databaseStatus(
        string const& worker, string const& jobId, GetDbStatusQservMgtRequest::CallbackType const& onFinish,
        unsigned int requestExpirationIvalSec) {
    auto const request = GetDbStatusQservMgtRequest::create(
            serviceProvider(), worker, [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

GetConfigQservMgtRequest::Ptr QservMgtServices::config(string const& worker, string const& jobId,
                                                       GetConfigQservMgtRequest::CallbackType const& onFinish,
                                                       unsigned int requestExpirationIvalSec) {
    auto const request = GetConfigQservMgtRequest::create(
            serviceProvider(), worker, [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

GetResultFilesQservMgtRequest::Ptr QservMgtServices::resultFiles(
        string const& worker, string const& jobId, vector<QueryId> const& queryIds, unsigned int maxFiles,
        GetResultFilesQservMgtRequest::CallbackType const& onFinish, unsigned int requestExpirationIvalSec) {
    auto const request = GetResultFilesQservMgtRequest::create(
            serviceProvider(), worker, queryIds, maxFiles,
            [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

GetStatusQservCzarMgtRequest::Ptr QservMgtServices::czarStatus(
        string const& czarName, string const& jobId,
        GetStatusQservCzarMgtRequest::CallbackType const& onFinish, unsigned int requestExpirationIvalSec) {
    auto const request = GetStatusQservCzarMgtRequest::create(
            serviceProvider(), czarName, [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

GetQueryProgressQservCzarMgtRequest::Ptr QservMgtServices::czarQueryProgress(
        string const& czarName, string const& jobId, vector<QueryId> const& queryIds,
        unsigned int lastSeconds, string const& queryStatus,
        GetQueryProgressQservCzarMgtRequest::CallbackType const& onFinish,
        unsigned int requestExpirationIvalSec) {
    auto const request = GetQueryProgressQservCzarMgtRequest::create(
            serviceProvider(), czarName, queryIds, lastSeconds, queryStatus,
            [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

GetConfigQservCzarMgtRequest::Ptr QservMgtServices::czarConfig(
        string const& czarName, string const& jobId,
        GetConfigQservCzarMgtRequest::CallbackType const& onFinish, unsigned int requestExpirationIvalSec) {
    auto const request = GetConfigQservCzarMgtRequest::create(
            serviceProvider(), czarName, [self = shared_from_this()](QservMgtRequest::Ptr const& request) {
                self->_finish(request->id());
            });
    _register(__func__, request, onFinish);
    request->start(jobId, requestExpirationIvalSec);
    return request;
}

void QservMgtServices::_finish(string const& id) {
    string const context = "QservMgtServices::" + string(__func__) + "[" + id + "] ";
    LOGS(_log, LOG_LVL_TRACE, context);

    // IMPORTANT: Make sure the notification is complete before removing the request
    // from the registry. This has two reasons:
    //   - it will avoid a possibility of deadlocking when the callback function
    //     to be notified will be doing any API calls of the service manager.
    //   - it will reduce the controller API dead-time due to a prolonged execution
    //     time of the callback function.
    detail::QservMgtRequestWrapper::Ptr requestWrapper;
    {
        replica::Lock const lock(_mtx, context);
        auto&& itr = _registry.find(id);
        if (itr == _registry.end()) throw logic_error(context + "unklnown request.");
        requestWrapper = itr->second;
        _registry.erase(id);
    }
    requestWrapper->notify();
}

}  // namespace lsst::qserv::replica
