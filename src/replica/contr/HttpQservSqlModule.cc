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
#include "replica/contr/HttpQservSqlModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "http/Exceptions.h"
#include "replica/requests/SqlQueryRequest.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

void HttpQservSqlModule::process(Controller::Ptr const& controller, string const& taskName,
                                 HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp, string const& subModuleName,
                                 http::AuthType const authType) {
    HttpQservSqlModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpQservSqlModule::HttpQservSqlModule(Controller::Ptr const& controller, string const& taskName,
                                       HttpProcessorConfig const& processorConfig,
                                       qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpQservSqlModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty()) return _execute();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpQservSqlModule::_execute() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const worker = body().required<string>("worker");
    auto const query = body().required<string>("query");
    auto const user = body().required<string>("user");
    auto const password = body().required<string>("password");
    auto const maxRows = body().optional<uint64_t>("max_rows", 0);

    debug(__func__, "worker=" + worker);
    debug(__func__, "query=" + query);
    debug(__func__, "user=" + user);
    debug(__func__, "maxRows=" + to_string(maxRows));

    auto const request = controller()->sqlQuery(worker, query, user, password, maxRows);
    request->wait();

    json result;
    result["result_set"] = request->responseData().toJson();

    if (request->extendedState() != Request::SUCCESS) {
        throw http::Error(__func__, "Query failed. See details in the result set", result);
    }
    return result;
}

}  // namespace lsst::qserv::replica
