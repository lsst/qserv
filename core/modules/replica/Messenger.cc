/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/Messenger.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Messenger");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

Messenger::pointer Messenger::create(ServiceProvider::pointer const& serviceProvider,
                                     boost::asio::io_service& io_service) {
    return Messenger::pointer(
        new Messenger(serviceProvider,
                      io_service));
}

Messenger::Messenger(ServiceProvider::pointer const& serviceProvider,
                     boost::asio::io_service& io_service) {

    for (auto const& worker: serviceProvider->config()->workers()){
        _connector[worker] = MessengerConnector::create(serviceProvider,
                                                        io_service,
                                                        worker);
    }
}

void Messenger::stop() {
    for (auto const& entry: _connector) {
        entry.second->stop();
    }
}

void Messenger::cancel(std::string const& worker,
                       std::string const& id) {

    // Forward the request to the corresponidng worker
    connector(worker)->cancel(id);
}

bool Messenger::exists(std::string const& worker,
                       std::string const& id) const {

    // Forward the request to the corresponidng worker
    return connector(worker)->exists(id);
}
    
MessengerConnector::pointer const& Messenger::connector(std::string const& worker)  const {

    if (!_connector.count(worker))
        throw std::invalid_argument(
            "Messenger::connector(): unknown worker: " + worker);
    return _connector.at(worker);
}

}}} // namespace lsst::qserv::replica