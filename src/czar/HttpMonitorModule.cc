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
#include "czar/HttpMonitorModule.h"

// System headers
#include <set>
#include <stdexcept>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "global/intTypes.h"
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "qdisp/CzarStats.h"
#include "qmeta/QProgressHistory.h"
#include "util/String.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::czar {

void HttpMonitorModule::process(string const& context, shared_ptr<qhttp::Request> const& req,
                                shared_ptr<qhttp::Response> const& resp, string const& subModuleName,
                                http::AuthType const authType) {
    HttpMonitorModule module(context, req, resp);
    module.execute(subModuleName, authType);
}

HttpMonitorModule::HttpMonitorModule(string const& context, shared_ptr<qhttp::Request> const& req,
                                     shared_ptr<qhttp::Response> const& resp)
        : QhttpModule(context, req, resp) {}

json HttpMonitorModule::executeImpl(string const& subModuleName) {
    string const func = string(__func__) + "[sub-module='" + subModuleName + "']";
    trace(func);
    enforceInstanceId(func, cconfig::CzarConfig::instance()->replicationInstanceId());
    enforceCzarName(func);
    if (subModuleName == "CONFIG")
        return _config();
    else if (subModuleName == "STATUS")
        return _status();
    else if (subModuleName == "QUERY-PROGRESS")
        return _queryProgress();
    throw invalid_argument(context() + func + " unsupported sub-module");
}

json HttpMonitorModule::_config() {
    debug(__func__);
    checkApiVersion(__func__, 29);
    return cconfig::CzarConfig::instance()->toJson();
}

json HttpMonitorModule::_status() {
    debug(__func__);
    checkApiVersion(__func__, 29);
    auto const stats = qdisp::CzarStats::get();
    return json::object(
            {{"qdisp_stats", stats->getQdispStatsJson()}, {"transmit_stats", stats->getTransmitStatsJson()}});
}

json HttpMonitorModule::_queryProgress() {
    debug(__func__);
    checkApiVersion(__func__, 48);

    auto const queryIds = query().optionalVectorUInt64("query_ids");
    unsigned int const lastSeconds = query().optionalUInt("last_seconds", 0);
    string const queryStatus = query().optionalString("query_status");

    debug(__func__, "query_ids=" + util::String::toString(queryIds));
    debug(__func__, "last_seconds=" + to_string(lastSeconds));
    debug(__func__, "query_status=" + queryStatus);

    auto const queryProgressHistory = qmeta::QProgressHistory::get();
    if (queryProgressHistory == nullptr) {
        throw http::Error(context() + __func__, " QProgressHistory is not initialized");
    }
    json result = json::object();
    if (queryIds.empty()) {
        result["queries"] = queryProgressHistory->findMany(lastSeconds, queryStatus);
    } else {
        result["queries"] = json::array();
        for (auto const queryId : queryIds) {
            result["queries"].push_back(queryProgressHistory->findOne(queryId));
        }
    }
    return result;
}

}  // namespace lsst::qserv::czar
