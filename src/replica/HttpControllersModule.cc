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

namespace lsst { namespace qserv { namespace replica {

void HttpControllersModule::process(Controller::Ptr const& controller, string const& taskName,
                                    HttpProcessorConfig const& processorConfig,
                                    qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                    string const& subModuleName, HttpAuthType const authType) {
    HttpControllersModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpControllersModule::HttpControllersModule(Controller::Ptr const& controller, string const& taskName,
                                             HttpProcessorConfig const& processorConfig,
                                             qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpControllersModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty())
        return _controllers();
    else if (subModuleName == "SELECT-ONE-BY-ID")
        return _oneController();
    else if (subModuleName == "LOG-DICT")
        return _eventLogDict();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpControllersModule::_controllers() {
    debug(__func__);

    uint64_t const fromTimeStamp = query().optionalUInt64("from");
    uint64_t const toTimeStamp = query().optionalUInt64("to", numeric_limits<uint64_t>::max());
    size_t const maxEntries = query().optionalUInt64("max_entries");
    bool const currentOnly = query().optionalBool("current_only", false);

    debug(__func__, "from=" + to_string(fromTimeStamp));
    debug(__func__, "to=" + to_string(toTimeStamp));
    debug(__func__, "max_entries=" + to_string(maxEntries));
    debug(__func__, "current_only=" + bool2str(currentOnly));

    // Just descriptions of the Controllers. No persistent logs in this
    // report.

    auto const controllers = controller()->serviceProvider()->databaseServices()->controllers(
            fromTimeStamp, toTimeStamp, maxEntries);

    json controllersJson;
    for (auto&& info : controllers) {
        bool const isCurrent = info.id == controller()->identity().id;
        if (currentOnly && !isCurrent) continue;
        controllersJson.push_back(info.toJson(isCurrent));
    }
    json result;
    result["controllers"] = controllersJson;
    return result;
}

json HttpControllersModule::_oneController() {
    debug(__func__);

    string const id = params().at("id");
    bool const log = query().optionalBool("log");
    bool const logCurrentController = query().optionalBool("log_current_controller");
    string const logTask = query().optionalString("log_task");
    string const logOperation = query().optionalString("log_operation");
    string const logOperationStatus = query().optionalString("log_operation_status");
    uint64_t const fromTimeStamp = query().optionalUInt64("log_from");
    uint64_t const toTimeStamp = query().optionalUInt64("log_to", numeric_limits<uint64_t>::max());
    size_t const maxEvents = query().optionalUInt64("log_max_events");

    debug(string(__func__) + " id=" + id);
    debug(string(__func__) + " log=" + bool2str(log));
    debug(string(__func__) + " log_current_controller=" + bool2str(logCurrentController));
    debug(string(__func__) + " log_task=" + logTask);
    debug(string(__func__) + " log_operation=" + logOperation);
    debug(string(__func__) + " log_operation_status=" + logOperationStatus);
    debug(string(__func__) + " log_from=" + to_string(fromTimeStamp));
    debug(string(__func__) + " log_to=" + to_string(toTimeStamp));
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
                    logCurrentController ? id : string(), fromTimeStamp, toTimeStamp, maxEvents, logTask,
                    logOperation, logOperationStatus);
            for (auto&& event : events) {
                jsonLog.push_back(event.toJson());
            }
        }
        result["log"] = jsonLog;

    } catch (DatabaseServicesNotFound const& ex) {
        throw HttpError(__func__, "no such controller found");
    }
    return result;
}

json HttpControllersModule::_eventLogDict() {
    debug(__func__);
    string const id = params().at("id");
    bool const logCurrentController = query().optionalBool("log_current_controller");
    debug(string(__func__) + " id=" + id);
    debug(string(__func__) + " log_current_controller=" + bool2str(logCurrentController));
    json result;
    try {
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        auto const controllerInfo = databaseServices->controller(id);
        bool const isCurrent = controllerInfo.id == controller()->identity().id;
        result["controller"] = controllerInfo.toJson(isCurrent);
        result["log_dict"] = databaseServices->readControllerEventDict(logCurrentController ? id : string());
    } catch (DatabaseServicesNotFound const& ex) {
        throw HttpError(__func__, "no such controller found");
    }
    return result;
}

}}}  // namespace lsst::qserv::replica
