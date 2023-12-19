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
#include "replica/services/ServiceProvider.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/qserv/QservMgtServices.h"
#include "replica/registry/Registry.h"
#include "replica/requests/Messenger.h"
#include "replica/services/DatabaseServicesPool.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ServiceProvider");

}  // namespace

namespace lsst::qserv::replica {

ServiceProvider::Ptr ServiceProvider::create(string const& configUrl, string const& instanceId,
                                             string const& authKey, string const& adminAuthKey) {
    return ServiceProvider::Ptr(new ServiceProvider(configUrl, instanceId, authKey, adminAuthKey));
}

ServiceProvider::ServiceProvider(string const& configUrl, string const& instanceId, string const& authKey,
                                 string const& adminAuthKey)
        : _configuration(Configuration::load(configUrl)),
          _instanceId(instanceId),
          _authKey(authKey),
          _adminAuthKey(adminAuthKey) {}

DatabaseServices::Ptr const& ServiceProvider::databaseServices() {
    replica::Lock lock(_mtx, _context() + __func__);
    if (_databaseServices == nullptr) {
        _databaseServices = DatabaseServicesPool::create(_configuration);
    }
    return _databaseServices;
}

QservMgtServices::Ptr const& ServiceProvider::qservMgtServices() {
    replica::Lock lock(_mtx, _context() + __func__);
    if (_qservMgtServices == nullptr) {
        _qservMgtServices = QservMgtServices::create(shared_from_this());
    }
    return _qservMgtServices;
}

Messenger::Ptr const& ServiceProvider::messenger() {
    replica::Lock lock(_mtx, _context() + __func__);
    if (_messenger == nullptr) {
        _messenger = Messenger::create(_configuration, _io_service);
    }
    return _messenger;
}

Registry::Ptr const& ServiceProvider::registry() {
    replica::Lock lock(_mtx, _context() + __func__);
    if (_registry == nullptr) {
        _registry = Registry::create(shared_from_this());
    }
    return _registry;
}

shared_ptr<replica::Mutex> ServiceProvider::getNamedMutex(string const& name) {
    return _namedMutexRegistry.get(name);
}

void ServiceProvider::run() {
    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__);

    replica::Lock lock(_mtx, _context() + __func__);

    // Check if the service is still not running

    if (not _threads.empty()) return;

    // Initialize BOOST ASIO services

    _work.reset(new boost::asio::io_service::work(_io_service));

    auto self = shared_from_this();

    _threads.clear();
    for (size_t i = 0; i < config()->get<size_t>("controller", "num-threads"); ++i) {
        _threads.push_back(make_unique<thread>([self]() {
            // This will prevent the I/O service from exiting the .run()
            // method event when it will run out of any requests to process.
            // Unless the service will be explicitly stopped.
            self->_io_service.run();
        }));
    }
}

bool ServiceProvider::isRunning() const {
    replica::Lock lock(_mtx, _context() + __func__);

    return not _threads.empty();
}

void ServiceProvider::stop() {
    LOGS(_log, LOG_LVL_DEBUG, _context() << __func__);

    replica::Lock lock(_mtx, _context() + __func__);

    // Check if the service is already stopped

    if (_threads.empty()) return;

    // These steps will cancel all outstanding requests to workers (if any)

    if (_messenger != nullptr) _messenger->stop();

    // Destroying this object will let the I/O service to (eventually) finish
    // all on-going work and shut down all service threads. In that case there
    // is no need to stop the service explicitly (which is not a good idea anyway
    // because there may be outstanding synchronous requests, in which case the service
    // would get into an unpredictable state.)

    _work.reset();

    // At this point all outstanding requests should finish and all threads
    // should stop as well.

    for (auto&& t : _threads) {
        t->join();
    }

    // Always do so in order to put service into a clean state. This will prepare
    // it for further usage.

    _io_service.reset();

    _threads.clear();
}

string ServiceProvider::_context() const { return "SERVICE-PROVIDER  "; }

}  // namespace lsst::qserv::replica
