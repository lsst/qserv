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
#include "replica/registry/RegistryHttpSvc.h"

// Qserv headers
#include "http/MetaModule.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/config/Configuration.h"
#include "replica/registry/RegistryHttpSvcMod.h"
#include "replica/registry/RegistryServices.h"

// Third party headers
#include "nlohmann/json.hpp"

using namespace nlohmann;
using namespace std;

namespace {
string const context_ = "REGISTRY-HTTP-SVC ";
}

namespace lsst::qserv::replica {

RegistryHttpSvc::Ptr RegistryHttpSvc::create(ServiceProvider::Ptr const& serviceProvider) {
    return RegistryHttpSvc::Ptr(new RegistryHttpSvc(serviceProvider));
}

RegistryHttpSvc::RegistryHttpSvc(ServiceProvider::Ptr const& serviceProvider)
        : HttpSvc(serviceProvider, serviceProvider->config()->get<uint16_t>("registry", "port"),
                  serviceProvider->config()->get<unsigned int>("registry", "max-listen-conn"),
                  serviceProvider->config()->get<size_t>("registry", "threads")),
          _services(new RegistryServices()) {}

string const& RegistryHttpSvc::context() const { return ::context_; }

void RegistryHttpSvc::registerServices() {
    auto const self = shared_from_base<RegistryHttpSvc>();
    httpServer()->addHandlers({{"GET", "/meta/version",
                                [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                    json const info = json::object(
                                            {{"kind", "replication-registry"},
                                             {"id", ""},
                                             {"instance_id", self->serviceProvider()->instanceId()}});
                                    http::MetaModule::process(context_, info, req, resp, "VERSION");
                                }},
                               {"GET", "/services",
                                [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                    RegistryHttpSvcMod::process(self->serviceProvider(), *(self->_services),
                                                                req, resp, "SERVICES", http::AuthType::NONE);
                                }},
                               {"POST", "/worker",
                                [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                    RegistryHttpSvcMod::process(self->serviceProvider(), *(self->_services),
                                                                req, resp, "ADD-WORKER");
                                }},
                               {"POST", "/qserv-worker",
                                [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                    RegistryHttpSvcMod::process(self->serviceProvider(), *(self->_services),
                                                                req, resp, "ADD-QSERV-WORKER");
                                }},
                               {"DELETE", "/worker/:name",
                                [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                    RegistryHttpSvcMod::process(self->serviceProvider(), *(self->_services),
                                                                req, resp, "DELETE-WORKER");
                                }},
                               {"POST", "/czar",
                                [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                    RegistryHttpSvcMod::process(self->serviceProvider(), *(self->_services),
                                                                req, resp, "ADD-CZAR");
                                }},
                               {"DELETE", "/czar/:name",
                                [self](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                    RegistryHttpSvcMod::process(self->serviceProvider(), *(self->_services),
                                                                req, resp, "DELETE-CZAR");
                                }}});
}

}  // namespace lsst::qserv::replica
