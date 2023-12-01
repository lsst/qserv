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
#include "xrdsvc/HttpMonitorModule.h"

// System headers
#include <set>
#include <stdexcept>

// Qserv headers
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "mysql/MySqlUtils.h"
#include "util/String.h"
#include "wbase/FileChannelShared.h"
#include "wbase/TaskState.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/Foreman.h"
#include "wcontrol/ResourceMonitor.h"
#include "wpublish/QueriesAndChunks.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::xrdsvc {

void HttpMonitorModule::process(string const& context, shared_ptr<wcontrol::Foreman> const& foreman,
                                shared_ptr<qhttp::Request> const& req,
                                shared_ptr<qhttp::Response> const& resp, string const& subModuleName,
                                http::AuthType const authType) {
    HttpMonitorModule module(context, foreman, req, resp);
    module.execute(subModuleName, authType);
}

HttpMonitorModule::HttpMonitorModule(string const& context, shared_ptr<wcontrol::Foreman> const& foreman,
                                     shared_ptr<qhttp::Request> const& req,
                                     shared_ptr<qhttp::Response> const& resp)
        : HttpModule(context, foreman, req, resp) {}

json HttpMonitorModule::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    debug(func);
    enforceInstanceId(func, wconfig::WorkerConfig::instance()->replicationInstanceId());
    enforceWorkerId(func);
    if (subModuleName == "CONFIG")
        return _config();
    else if (subModuleName == "MYSQL")
        return _mysql();
    else if (subModuleName == "STATUS")
        return _status();
    else if (subModuleName == "FILES")
        return _files();
    else if (subModuleName == "ECHO")
        return _echo();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpMonitorModule::_config() {
    debug(__func__);
    checkApiVersion(__func__, 27);
    return wconfig::WorkerConfig::instance()->toJson();
}

json HttpMonitorModule::_mysql() {
    debug(__func__);
    checkApiVersion(__func__, 27);
    json result;
    try {
        bool const full = true;
        result = mysql::MySqlUtils::processList(wconfig::WorkerConfig::instance()->getMySqlConfig(), full);
    } catch (mysql::MySqlQueryError const& ex) {
        error(__func__, ex.what());
        throw http::Error(__func__, ex.what());
    }

    // Amend the result with a map linking MySQL thread identifiers to the corresponding
    // tasks that are being (or have been) processed by the worker. Note that only a subset
    // of tasks is selected for the known MySQL threads. This prevents the monitoring
    // system from pulling old tasks that may still keep records of the closed threads.
    set<unsigned long> activeMySqlThreadIds;
    for (auto const& row : result["queries"]["rows"]) {
        // The thread identifier is stored as a string at the very first element
        // of the array. See mysql::MySqlUtils::processList for details.
        activeMySqlThreadIds.insert(stoul(row[0].get<string>()));
    }
    result["mysql_thread_to_task"] = foreman()->queriesAndChunks()->mySqlThread2task(activeMySqlThreadIds);
    return result;
}

json HttpMonitorModule::_status() {
    debug(__func__);
    checkApiVersion(__func__, 27);
    wbase::TaskSelector const taskSelector = translateTaskSelector(__func__);
    json result;
    result["processor"] = foreman()->statusToJson(taskSelector);
    result["resources"] = foreman()->resourceMonitor()->statusToJson();
    result["filesystem"] = wbase::FileChannelShared::statusToJson();
    return result;
}

json HttpMonitorModule::_files() {
    debug(__func__);
    checkApiVersion(__func__, 28);
    auto const queryIds = query().optionalVectorUInt64("query_ids");
    auto const maxFiles = query().optionalUInt("max_files", 0);
    debug(__func__, "query_ids=" + util::String::toString(queryIds));
    debug(__func__, "max_files=" + to_string(maxFiles));
    return wbase::FileChannelShared::filesToJson(queryIds, maxFiles);
}

json HttpMonitorModule::_echo() {
    debug(__func__);
    checkApiVersion(__func__, 27);
    return json::object({{"data", body().required<string>("data")}});
}

}  // namespace lsst::qserv::xrdsvc
