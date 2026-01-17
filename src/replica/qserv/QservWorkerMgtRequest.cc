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
#include "replica/qserv/QservWorkerMgtRequest.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"

using namespace std;

namespace lsst::qserv::replica {

QservWorkerMgtRequest::QservWorkerMgtRequest(ServiceProvider::Ptr const& serviceProvider, string const& type,
                                             string const& workerName)
        : QservMgtRequest(serviceProvider, type, "worker", workerName), _workerName(workerName) {}

http::AsyncReq::GetHostPort QservWorkerMgtRequest::getHostPortTracker() const {
    return [config = serviceProvider()->config(),
            workerName = _workerName](http::AsyncReq::HostPort const&) -> http::AsyncReq::HostPort {
        auto const worker = config->worker(workerName);
        return http::AsyncReq::HostPort{worker.qservWorker.host.addr, worker.qservWorker.managementPort};
    };
}

void QservWorkerMgtRequest::updatePersistentState(Performance const& performance,
                                                  std::string const& serverError) const {
    serviceProvider()->databaseServices()->saveState(*this, performance, serverError);
}

}  // namespace lsst::qserv::replica
