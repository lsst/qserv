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
#include "replica/HttpControllersModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

void HttpControllersModule::process(Controller::Ptr const& controller,
                                    string const& taskName,
                                    HttpProcessorConfig const& processorConfig,
                                    qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp,
                                    string const& subModuleName,
                                    HttpModule::AuthType const authType) {
    HttpControllersModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpControllersModule::HttpControllersModule(Controller::Ptr const& controller,
                                             string const& taskName,
                                             HttpProcessorConfig const& processorConfig,
                                             qhttp::Request::Ptr const& req,
                                             qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpControllersModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty()) return _controllers();
    else if (subModuleName == "SELECT-ONE-BY-ID") return _oneController();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpControllersModule::_controllers() {
    debug(__func__);

    uint64_t const fromTimeStamp = query().optionalUInt64("from");
    uint64_t const toTimeStamp   = query().optionalUInt64("to", numeric_limits<uint64_t>::max());
    size_t   const maxEntries    = query().optionalUInt64("max_entries");

    debug(__func__, "from="        + to_string(fromTimeStamp));
    debug(__func__, "to="          + to_string(toTimeStamp));
    debug(__func__, "max_entries=" + to_string(maxEntries));

    // Just descriptions of the Controllers. No persistent logs in this
    // report.

    auto const controllers =
        controller()->serviceProvider()->databaseServices()->controllers(
            fromTimeStamp,
            toTimeStamp,
            maxEntries);

    json controllersJson;
    for (auto&& info: controllers) {
        bool const isCurrent = info.id == controller()->identity().id;
        controllersJson.push_back(info.toJson(isCurrent));
    }
    json result;
    result["controllers"] = controllersJson;
    return result;
}


json HttpControllersModule::_oneController() {
    debug(__func__);

    auto const id = params().at("id");

    bool     const log           = query().optionalBool(  "log");
    uint64_t const fromTimeStamp = query().optionalUInt64("log_from");
    uint64_t const toTimeStamp   = query().optionalUInt64("log_to", numeric_limits<uint64_t>::max());
    size_t   const maxEvents     = query().optionalUInt64("log_max_events");

    debug(string(__func__) + " log="            +  bool2str(log));
    debug(string(__func__) + " log_from="       + to_string(fromTimeStamp));
    debug(string(__func__) + " log_to="         + to_string(toTimeStamp));
    debug(string(__func__) + " log_max_events=" + to_string(maxEvents));

    json result;
    try {
        // General description of the Controller

        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const controllerInfo = databaseServices->controller(id);

        bool const isCurrent = controllerInfo.id == controller()->identity().id;
        result["controller"] = controllerInfo.toJson(isCurrent);

        // Pull the Controller log data if requested
        json jsonLog = json::array();
        if (log) {
            auto const events = databaseServices->readControllerEvents(
                id,
                fromTimeStamp,
                toTimeStamp,
                maxEvents
            );
            for (auto&& event: events) {
                jsonLog.push_back(event.toJson());
            }
        }
        result["log"] = jsonLog;

    } catch (DatabaseServicesNotFound const& ex) {
        throw HttpError(__func__, "no such controller found");
    }
    return result;
}

}}}  // namespace lsst::qserv::replica
