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
#include "replica/RedirectorHttpSvc.h"

// Qserv headers
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/Configuration.h"
#include "replica/RedirectorHttpSvcMod.h"
#include "replica/RedirectorWorkers.h"

using namespace std;

namespace {
string const context_ = "REDIRECTOR-HTTP-SVC ";
}

namespace lsst {
namespace qserv {
namespace replica {

RedirectorHttpSvc::Ptr RedirectorHttpSvc::create(
        ServiceProvider::Ptr const& serviceProvider) {
    return RedirectorHttpSvc::Ptr(new RedirectorHttpSvc(serviceProvider));
}


RedirectorHttpSvc::RedirectorHttpSvc(
        ServiceProvider::Ptr const& serviceProvider)
    :   HttpSvc(serviceProvider,
                serviceProvider->config()->get<uint16_t>("redirector", "port"),
                serviceProvider->config()->get<unsigned int>("redirector", "max-listen-conn"),
                serviceProvider->config()->get<size_t>("redirector", "threads")),
        _workers(new RedirectorWorkers()) {
}


string const& RedirectorHttpSvc::context() const { return ::context_; }


void RedirectorHttpSvc::registerServices() {
    auto const self = shared_from_base<RedirectorHttpSvc>();
    httpServer()->addHandlers({
        {"GET", "/workers",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                RedirectorHttpSvcMod::process(
                        self->serviceProvider(), *(self->_workers),
                        req, resp,
                        "WORKERS",
                        HttpModuleBase::AUTH_NONE);
            }
        },
        {"POST", "/worker",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                RedirectorHttpSvcMod::process(
                        self->serviceProvider(), *(self->_workers),
                        req, resp,
                        "ADD-WORKER");
            }
        },
        {"DELETE", "/worker/:name",
            [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                RedirectorHttpSvcMod::process(
                        self->serviceProvider(), *(self->_workers),
                        req, resp,
                        "DELETE-WORKER");
            }
        }
    });
}

}}} // namespace lsst::qserv::replica
