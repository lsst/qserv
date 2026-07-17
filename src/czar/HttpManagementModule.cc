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
#include "czar/HttpManagementModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "cconfig/EventService.h"
#include "cconfig/DataManagementEvent.h"
#include "http/Exceptions.h"
#include "http/RequestBodyJSON.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::czar {

void HttpManagementModule::process(string const& context, shared_ptr<qhttp::Request> const& req,
                                   shared_ptr<qhttp::Response> const& resp,
                                   shared_ptr<cconfig::EventService> const& eventService,
                                   string const& subModuleName, http::AuthType const authType) {
    HttpManagementModule module(context, req, resp, eventService);
    module.execute(subModuleName, authType);
}

HttpManagementModule::HttpManagementModule(string const& context, shared_ptr<qhttp::Request> const& req,
                                           shared_ptr<qhttp::Response> const& resp,
                                           shared_ptr<cconfig::EventService> const& eventService)
        : QhttpModule(context, req, resp), _eventService(eventService) {}

json HttpManagementModule::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func);
    enforceInstanceId(func, cconfig::CzarConfig::instance()->replicationInstanceId());
    enforceCzarName(func);
    if (subModuleName == "EVENT") return _event();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpManagementModule::_event() {
    debug(__func__);
    checkApiVersion(__func__, 57);
    json const eventJson = body().required<json>("event");
    cconfig::DataManagementEvent const event = cconfig::DataManagementEvent::fromJson(eventJson);
    debug(__func__, "event: " + event.toJson().dump());
    _eventService->postEvent(event);
    return {};
}

}  // namespace lsst::qserv::czar
