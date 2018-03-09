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
#include "replica/RequestMessenger.h"

// System headers

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Messenger.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.RequestMessenger");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

RequestMessenger::RequestMessenger(ServiceProvider::pointer const& serviceProvider,
                                   boost::asio::io_service& io_service,
                                   std::string const& type,
                                   std::string const& worker,
                                   int  priority,
                                   bool keepTracking,
                                   bool allowDuplicate,
                                   std::shared_ptr<Messenger> const& messenger)
    :   Request(serviceProvider,
                io_service,
                type,
                worker,
                priority,
                keepTracking,
                allowDuplicate),
        _messenger(messenger) {
}

void RequestMessenger::finishImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finishImpl");

    // Make sure the request (if any) has been eliminated from the messenger
    if (_messenger->exists(worker(), id())) {
        _messenger->cancel(worker(), id());
    }
}

}}} // namespace lsst::qserv::replica