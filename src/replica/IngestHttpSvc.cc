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
#include "replica/IngestHttpSvc.h"

// System headers
#include <functional>

// Qserv headers
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/Configuration.h"
#include "replica/IngestHttpSvcMod.h"

using namespace std;

namespace {
string const context_ = "INGEST-HTTP-SVC  ";
}

namespace lsst {
namespace qserv {
namespace replica {

IngestHttpSvc::Ptr IngestHttpSvc::create(ServiceProvider::Ptr const& serviceProvider,
                                         string const& workerName,
                                         string const& authKey,
                                         string const& adminAuthKey) {
    return IngestHttpSvc::Ptr(new IngestHttpSvc(serviceProvider, workerName, authKey, adminAuthKey));
}


IngestHttpSvc::IngestHttpSvc(ServiceProvider::Ptr const& serviceProvider,
                             string const& workerName,
                             string const& authKey,
                             string const& adminAuthKey)
    :   HttpSvc(serviceProvider,
                serviceProvider->config()->workerInfo(workerName).httpLoaderPort,
                serviceProvider->config()->get<size_t>("worker", "num_http_loader_processing_threads"),
                authKey,
                adminAuthKey),
        _workerName(workerName) {
}


string const& IngestHttpSvc::context() const { return context_; }


void IngestHttpSvc::registerServices() {
    auto const self = shared_from_base<IngestHttpSvc>();
    httpServer()->addHandlers({
        {"POST", "/ingest/file",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                IngestHttpSvcMod::process(
                        self->serviceProvider(), self->_workerName, self->authKey(), self->adminAuthKey(),
                        req, resp);
            }
        }
    });
}

}}} // namespace lsst::qserv::replica
