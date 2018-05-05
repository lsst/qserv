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
#include "replica/QservMgtServices.h"

// System headers
#include <stdexcept>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.QservMgtServices");

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

//////////////////////////////////////////////////////////////////////////////////
//////////////////////////  QservMgtRequestWrapperImpl  //////////////////////////
//////////////////////////////////////////////////////////////////////////////////

/**
 * Request-type specific wrappers
 */
template <class  T>
struct QservMgtRequestWrapperImpl
    :   QservMgtRequestWrapper {

    /// The implementation of the vurtual method defined in the base class
    virtual void notify() {
        if (_onFinish == nullptr) { return; }
        _onFinish(_request);
    }

    QservMgtRequestWrapperImpl(typename T::Ptr const& request,
                               typename T::CallbackType onFinish)
        :   QservMgtRequestWrapper(),
            _request(request),
            _onFinish(onFinish) {
    }

    /// Destructor
    ~QservMgtRequestWrapperImpl() override = default;

    /// Implement a virtual method of the base class
    std::shared_ptr<QservMgtRequest> request() const override {
        return _request;
    }

private:

    // The context of the operation

    typename T::Ptr       _request;
    typename T::CallbackType _onFinish;
};

////////////////////////////////////////////////////////////////////////
//////////////////////////  QservMgtServices  //////////////////////////
////////////////////////////////////////////////////////////////////////

QservMgtServices::Ptr QservMgtServices::create(ServiceProvider::Ptr const& serviceProvider) {
    return QservMgtServices::Ptr(
        new QservMgtServices(serviceProvider));
}

QservMgtServices::QservMgtServices(ServiceProvider::Ptr const& serviceProvider)
    :   _serviceProvider(serviceProvider),
        _io_service(),
        _work(nullptr),
        _registry() {
}

AddReplicaQservMgtRequest::Ptr QservMgtServices::addReplica(
                                        unsigned int chunk,
                                        std::vector<std::string> const& databases,
                                        std::string const& worker,
                                        AddReplicaQservMgtRequest::CallbackType onFinish,
                                        std::string const& jobId,
                                        unsigned int requestExpirationIvalSec) {
    LOCK_GUARD;

    // Ensure we have the XROOTD/SSI service object before attempting any
    // operations on requests
    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return AddReplicaQservMgtRequest::Ptr();
    }

    auto const manager = shared_from_this();

    AddReplicaQservMgtRequest::Ptr const request =
        AddReplicaQservMgtRequest::create(
            _serviceProvider,
            _io_service,
            worker,
            chunk,
            databases,
            [manager] (QservMgtRequest::Ptr const& request) {
                manager->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.
    _registry[request->id()] =
        std::make_shared<QservMgtRequestWrapperImpl<AddReplicaQservMgtRequest>>(
            request, onFinish);

    // Initiate the request
    request->start(service,
                   jobId,
                   requestExpirationIvalSec);

    return request;
}



RemoveReplicaQservMgtRequest::Ptr QservMgtServices::removeReplica(
                                        unsigned int chunk,
                                        std::vector<std::string> const& databases,
                                        std::string const& worker,
                                        bool force,
                                        RemoveReplicaQservMgtRequest::CallbackType onFinish,
                                        std::string const& jobId,
                                        unsigned int requestExpirationIvalSec) {
    LOCK_GUARD;

    // Ensure we have the XROOTD/SSI service object before attempting any
    // operations on requests
    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return RemoveReplicaQservMgtRequest::Ptr();
    }

    auto const manager = shared_from_this();

    RemoveReplicaQservMgtRequest::Ptr const request =
        RemoveReplicaQservMgtRequest::create(
            _serviceProvider,
            _io_service,
            worker,
            chunk,
            databases,
            force,
            [manager] (QservMgtRequest::Ptr const& request) {
                manager->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.
    _registry[request->id()] =
        std::make_shared<QservMgtRequestWrapperImpl<RemoveReplicaQservMgtRequest>>(
            request, onFinish);

    // Initiate the request
    request->start(service,
                   jobId,
                   requestExpirationIvalSec);

    return request;
}

GetReplicasQservMgtRequest::Ptr QservMgtServices::getReplicas(
                                        std::string const& databaseFamily,
                                        std::string const& worker,
                                        bool inUseOnly,
                                        std::string const& jobId,
                                        GetReplicasQservMgtRequest::CallbackType onFinish,
                                        unsigned int requestExpirationIvalSec) {
    LOCK_GUARD;

    // Ensure we have the XROOTD/SSI service object before attempting any
    // operations on requests
    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return GetReplicasQservMgtRequest::Ptr();
    }

    auto const manager = shared_from_this();

    GetReplicasQservMgtRequest::Ptr const request =
        GetReplicasQservMgtRequest::create(
            _serviceProvider,
            _io_service,
            worker,
            databaseFamily,
            inUseOnly,
            [manager] (QservMgtRequest::Ptr const& request) {
                manager->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.
    _registry[request->id()] =
        std::make_shared<QservMgtRequestWrapperImpl<GetReplicasQservMgtRequest>>(
            request, onFinish);

    // Initiate the request
    request->start(service,
                   jobId,
                   requestExpirationIvalSec);

    return request;
}


SetReplicasQservMgtRequest::Ptr QservMgtServices::setReplicas(
                                        std::string const& worker,
                                        QservReplicaCollection const& newReplicas,
                                        bool force,
                                        std::string const& jobId,
                                        SetReplicasQservMgtRequest::CallbackType onFinish,
                                        unsigned int requestExpirationIvalSec) {
    LOCK_GUARD;

    // Ensure we have the XROOTD/SSI service object before attempting any
    // operations on requests
    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return SetReplicasQservMgtRequest::Ptr();
    }

    auto const manager = shared_from_this();

    SetReplicasQservMgtRequest::Ptr const request =
        SetReplicasQservMgtRequest::create(
            _serviceProvider,
            _io_service,
            worker,
            newReplicas,
            force,
            [manager] (QservMgtRequest::Ptr const& request) {
                manager->finish(request->id());
            }
        );

    // Register the request (along with its callback) by its unique
    // identifier in the local registry. Once it's complete it'll
    // be automatically removed from the Registry.
    _registry[request->id()] =
        std::make_shared<QservMgtRequestWrapperImpl<SetReplicasQservMgtRequest>>(
            request, onFinish);

    // Initiate the request
    request->start(service,
                   jobId,
                   requestExpirationIvalSec);

    return request;
}

void QservMgtServices::finish(std::string const& id) {

    // IMPORTANT:
    //
    //   Make sure the notification is complete before removing
    //   the request from the registry. This has two reasons:
    //
    //   - it will avoid a possibility of deadlocking in case if
    //     the callback function to be notified will be doing
    //     any API calls of the service manager.
    //
    //   - it will reduce the controller API dead-time due to a prolonged
    //     execution time of of the callback function.

    QservMgtRequestWrapper::Ptr request;
    {
        LOCK_GUARD;
        request = _registry[id];
        _registry.erase(id);
    }
    request->notify();
}

XrdSsiService* QservMgtServices::xrdSsiService() {

    // Lazy construction of the locator string to allow dynamic
    // reconfiguration.
    std::string const serviceProviderLocation =
        _serviceProvider->config()->xrootdHost() + ":" +
        std::to_string(_serviceProvider->config()->xrootdPort());

    // Connect to a service provider
    XrdSsiErrInfo errInfo;
    XrdSsiService* service =
        XrdSsiProviderClient->GetService(errInfo,
                                         serviceProviderLocation);
    if (not service) {
        LOGS(_log, LOG_LVL_ERROR, "QservMgtServices::xrdSsiService()  "
             << "failed to contact service provider at: " << serviceProviderLocation
             << ", error: " << errInfo.Get());
    }
    return service;
}

}}} // namespace lsst::qserv::replica
