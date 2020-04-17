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
#include "replica/HttpQservSqlModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/SqlQueryRequest.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

HttpQservSqlModule::Ptr HttpQservSqlModule::create(Controller::Ptr const& controller,
                                                   string const& taskName,
                                                   HttpProcessorConfig const& processorConfig) {
    return Ptr(new HttpQservSqlModule(
        controller, taskName, processorConfig
    ));
}


HttpQservSqlModule::HttpQservSqlModule(Controller::Ptr const& controller,
                                       string const& taskName,
                                       HttpProcessorConfig const& processorConfig)
    :   HttpModule(controller, taskName, processorConfig) {
}


void HttpQservSqlModule::executeImpl(string const& subModuleName) {

    if (subModuleName.empty()) {
        _execute();
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpQservSqlModule::_execute() {
    debug(__func__);

    auto const worker   = body().required<string>("worker");
    auto const query    = body().required<string>("query");
    auto const user     = body().required<string>("user");
    auto const password = body().required<string>("password");
    auto const maxRows  = body().optional<uint64_t>("max_rows", 0);

    debug(__func__, "worker="   + worker);
    debug(__func__, "query="    + query);
    debug(__func__, "user="     + user);
    debug(__func__, "maxRows="  + to_string(maxRows));

    auto const request = controller()->sqlQuery(
        worker,
        query,
        user,
        password,
        maxRows
    );
    request->wait();

    json result;
    result["result_set"] = request->responseData().toJson();

    bool const success = request->extendedState() == Request::SUCCESS ? 1 : 0;
    sendData(result, success);
}

}}}  // namespace lsst::qserv::replica
