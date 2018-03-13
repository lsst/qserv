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

    QservMgtRequestWrapperImpl(typename T::pointer const& request,
                               typename T::callback_type onFinish)
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

    typename T::pointer       _request;
    typename T::callback_type _onFinish;
};

////////////////////////////////////////////////////////////////////////
//////////////////////////  QservMgtServices  //////////////////////////
////////////////////////////////////////////////////////////////////////

QservMgtServices::pointer QservMgtServices::create(ServiceProvider::pointer const& serviceProvider) {
    return QservMgtServices::pointer(
        new QservMgtServices(serviceProvider));
}

QservMgtServices::QservMgtServices(ServiceProvider::pointer const& serviceProvider)
    :   _serviceProvider(serviceProvider),
        _io_service(),
        _work(nullptr),
        _registry() {
}

AddReplicaQservMgtRequest::pointer QservMgtServices::addReplica(
                                        unsigned int chunk,
                                        std::string const& databaseFamily,
                                        std::string const& worker,
                                        AddReplicaQservMgtRequest::callback_type onFinish,
                                        std::string const& jobId,
                                        unsigned int requestExpirationIvalSec) {
    LOCK_GUARD;

    // Ensure we have the XROOTD/SSI service object before attempting any
    // operations on requests
    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return AddReplicaQservMgtRequest::pointer();
    }

    QservMgtServices::pointer manager = shared_from_this();

    AddReplicaQservMgtRequest::pointer const request =
        AddReplicaQservMgtRequest::create(
            _serviceProvider,
            _io_service,
            worker,
            chunk,
            databaseFamily,
            [manager] (QservMgtRequest::pointer const& request) {
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



RemoveReplicaQservMgtRequest::pointer QservMgtServices::removeReplica(
                                        unsigned int chunk,
                                        std::string const& databaseFamily,
                                        std::string const& worker,
                                        bool force,
                                        RemoveReplicaQservMgtRequest::callback_type onFinish,
                                        std::string const& jobId,
                                        unsigned int requestExpirationIvalSec) {
    LOCK_GUARD;

    // Ensure we have the XROOTD/SSI service object before attempting any
    // operations on requests
    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return RemoveReplicaQservMgtRequest::pointer();
    }

    QservMgtServices::pointer manager = shared_from_this();

    RemoveReplicaQservMgtRequest::pointer const request =
        RemoveReplicaQservMgtRequest::create(
            _serviceProvider,
            _io_service,
            worker,
            chunk,
            databaseFamily,
            force,
            [manager] (QservMgtRequest::pointer const& request) {
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

    QservMgtRequestWrapper::pointer request;
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
