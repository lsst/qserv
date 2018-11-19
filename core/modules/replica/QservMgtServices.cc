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

    /// The implementation of the virtual method defined in the base class
    void notify() const final {

        if (nullptr != _onFinish) {

            // Clearing the stored callback after finishing the up-stream notification
            // has two purposes:
            //
            // 1. it guaranties (exactly) one time notification
            // 2. it breaks the up-stream dependency on a caller object if a shared
            //    pointer to the object was mentioned as the lambda-function's closure

            auto onFinish = std::move(_onFinish);
            _onFinish = nullptr;
            onFinish(_request);
        }
    }

    QservMgtRequestWrapperImpl(typename T::Ptr const& request,
                               typename T::CallbackType const& onFinish)
        :   QservMgtRequestWrapper(),
            _request(request),
            _onFinish(onFinish) {
    }

    /// Destructor
    ~QservMgtRequestWrapperImpl() = default;

    /// Implement a virtual method of the base class
    std::shared_ptr<QservMgtRequest> request() const override {
        return _request;
    }

private:

    // The context of the operation

    typename T::Ptr _request;
    mutable typename T::CallbackType _onFinish;
};

////////////////////////////////////////////////////////////////////////
//////////////////////////  QservMgtServices  //////////////////////////
////////////////////////////////////////////////////////////////////////

QservMgtServices::Ptr QservMgtServices::create(ServiceProvider::Ptr const& serviceProvider) {
    return QservMgtServices::Ptr(new QservMgtServices(serviceProvider));
}

QservMgtServices::QservMgtServices(ServiceProvider::Ptr const& serviceProvider)
    :   _serviceProvider(serviceProvider) {
}

AddReplicaQservMgtRequest::Ptr QservMgtServices::addReplica(
                                        unsigned int chunk,
                                        std::vector<std::string> const& databases,
                                        std::string const& worker,
                                        AddReplicaQservMgtRequest::CallbackType const& onFinish,
                                        std::string const& jobId,
                                        unsigned int requestExpirationIvalSec) {

    AddReplicaQservMgtRequest::Ptr request;

    // Make sure the XROOTD/SSI service is available before attempting
    // any operations on requests

    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return request;
    } else {

        util::Lock lock(_mtx, "QservMgtServices::addReplica");
    
        auto const manager = shared_from_this();
    
        request = AddReplicaQservMgtRequest::create(
            serviceProvider(),
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
    }

    // Initiate the request in the lock-free zone to avoid blocking the service
    // from initiating other requests which this one is starting.
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
                                        RemoveReplicaQservMgtRequest::CallbackType const& onFinish,
                                        std::string const& jobId,
                                        unsigned int requestExpirationIvalSec) {

    RemoveReplicaQservMgtRequest::Ptr request;

    // Make sure the XROOTD/SSI service is available before attempting
    // any operations on requests

    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return request;
    } else {

        util::Lock lock(_mtx, "QservMgtServices::removeReplica");
    
        auto const manager = shared_from_this();
    
        request = RemoveReplicaQservMgtRequest::create(
            serviceProvider(),
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
    }

    // Initiate the request in the lock-free zone to avoid blocking the service
    // from initiating other requests which this one is starting.
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
                                        GetReplicasQservMgtRequest::CallbackType const& onFinish,
                                        unsigned int requestExpirationIvalSec) {

    GetReplicasQservMgtRequest::Ptr request;

    // Make sure the XROOTD/SSI service is available before attempting
    // any operations on requests

    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return request;
    } else {

        util::Lock lock(_mtx, "QservMgtServices::getReplicas");
    
        auto const manager = shared_from_this();
    
        request = GetReplicasQservMgtRequest::create(
            serviceProvider(),
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
    }

    // Initiate the request in the lock-free zone to avoid blocking the service
    // from initiating other requests which this one is starting.
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
                                        SetReplicasQservMgtRequest::CallbackType const& onFinish,
                                        unsigned int requestExpirationIvalSec) {

    SetReplicasQservMgtRequest::Ptr request;

    // Make sure the XROOTD/SSI service is available before attempting
    // any operations on requests

    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return request;
    } else {

        util::Lock lock(_mtx, "QservMgtServices::setReplicas");
    
        auto const manager = shared_from_this();
    
        request = SetReplicasQservMgtRequest::create(
            serviceProvider(),
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
    }

    // Initiate the request in the lock-free zone to avoid blocking the service
    // from initiating other requests which this one is starting.
    request->start(service,
                   jobId,
                   requestExpirationIvalSec);

    return request;
}

TestEchoQservMgtRequest::Ptr QservMgtServices::echo(
                                    std::string const& worker,
                                    std::string const& data,
                                    std::string const& jobId,
                                    TestEchoQservMgtRequest::CallbackType const& onFinish,
                                    unsigned int requestExpirationIvalSec) {

    TestEchoQservMgtRequest::Ptr request;

    // Make sure the XROOTD/SSI service is available before attempting
    // any operations on requests

    XrdSsiService* service = xrdSsiService();
    if (not service) {
        return request;
    } else {

        util::Lock lock(_mtx, "QservMgtServices::echo");
    
        auto const manager = shared_from_this();
    
        request = TestEchoQservMgtRequest::create(
            serviceProvider(),
            worker,
            data,
            [manager] (QservMgtRequest::Ptr const& request) {
                manager->finish(request->id());
            }
        );
    
        // Register the request (along with its callback) by its unique
        // identifier in the local registry. Once it's complete it'll
        // be automatically removed from the Registry.
        _registry[request->id()] =
            std::make_shared<QservMgtRequestWrapperImpl<TestEchoQservMgtRequest>>(
                request, onFinish);
    }

    // Initiate the request in the lock-free zone to avoid blocking the service
    // from initiating other requests which this one is starting.
    request->start(service,
                   jobId,
                   requestExpirationIvalSec);

    return request;
}

void QservMgtServices::finish(std::string const& id) {

    std::string const context = id + "  QservMgtServices::finish  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

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

    QservMgtRequestWrapper::Ptr requestWrapper;
    {
        util::Lock lock(_mtx, context);
        auto&& itr = _registry.find(id);
        if (itr == _registry.end()) {
            throw std::logic_error(
                        "QservMgtServices::finish: request identifier " + id +
                        " is no longer valid. Check the logic of the application.");
        }
        requestWrapper = itr->second;
        _registry.erase(id);
    }
    requestWrapper->notify();
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
