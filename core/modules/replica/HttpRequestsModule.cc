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
#include "replica/HttpRequestsModule.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/DatabaseServices.h"
#include "replica/HttpRequestQuery.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

HttpRequestsModule::Ptr HttpRequestsModule::create(Controller::Ptr const& controller,
                                                   string const& taskName,
                                                   unsigned int workerResponseTimeoutSec) {
    return Ptr(new HttpRequestsModule(
        controller,
        taskName,
        workerResponseTimeoutSec
    ));
}


HttpRequestsModule::HttpRequestsModule(Controller::Ptr const& controller,
                                       string const& taskName,
                                       unsigned int workerResponseTimeoutSec)
    :   HttpModule(controller,
                   taskName,
                   workerResponseTimeoutSec) {
}


void HttpRequestsModule::executeImpl(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp,
                                     string const& subModuleName) {

    if (subModuleName.empty()) {
        _requests(req, resp);
    } else if (subModuleName == "SELECT-ONE-BY-ID") {
        _oneRequest(req, resp);
    } else {
        throw invalid_argument(
                context() + "::" + string(__func__) +
                "  unsupported sub-module: '" + subModuleName + "'");
    }
}


void HttpRequestsModule::_requests(qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp) {
    debug(__func__);

    HttpRequestQuery const query(req->query);
    string   const jobId         = query.optionalString("job_id");
    uint64_t const fromTimeStamp = query.optionalUInt64("from");
    uint64_t const toTimeStamp   = query.optionalUInt64("to", numeric_limits<uint64_t>::max());
    size_t   const maxEntries    = query.optionalUInt64("max_entries");

    debug(__func__, "job_id="      +           jobId);
    debug(__func__, "from="        + to_string(fromTimeStamp));
    debug(__func__, "to="          + to_string(toTimeStamp));
    debug(__func__, "max_entries=" + to_string(maxEntries));

    // Pull descriptions of the Requests

    auto const requests =
        controller()->serviceProvider()->databaseServices()->requests(
            jobId,
            fromTimeStamp,
            toTimeStamp,
            maxEntries);

    json requestsJson;
    for (auto&& info: requests) {
        requestsJson.push_back(info.toJson());
    }
    json result;
    result["requests"] = requestsJson;

    sendData(resp, result);
}


void HttpRequestsModule::_oneRequest(qhttp::Request::Ptr const& req,
                                     qhttp::Response::Ptr const& resp) {
    debug(__func__);

    auto const id = req->params.at("id");

    try {
        json result;
        result["request"] = controller()->serviceProvider()->databaseServices()->request(id).toJson();
        sendData(resp, result);

    } catch (DatabaseServicesNotFound const& ex) {
        sendError(resp, __func__, "no such request found");
    }
}

}}}  // namespace lsst::qserv::replica