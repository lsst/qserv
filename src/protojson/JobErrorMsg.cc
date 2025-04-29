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
#include "protojson/JobErrorMsg.h"

#include <stdexcept>

// Qserv headers
#include "http/Client.h"
#include "http/MetaModule.h"
#include "http/RequestBodyJSON.h"
#include "util/common.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.JobErrorMsg");
}  // namespace

namespace lsst::qserv::protojson {

JobErrorMsg::Ptr JobErrorMsg::create(std::string const& replicationInstanceId,
                                     std::string const& replicationAuthKey, std::string const& workerIdStr,
                                     std::string const& czarName, CzarIdType czarId, QueryId queryId,
                                     UberJobId uberJobId, int errorCode, std::string const& errorMsg) {
    auto jrMsg = Ptr(new JobErrorMsg(replicationInstanceId, replicationAuthKey));
    jrMsg->_workerId = workerIdStr;
    jrMsg->_czarName = czarName;
    jrMsg->_czarId = czarId;
    jrMsg->_queryId = queryId;
    jrMsg->_uberJobId = uberJobId;
    jrMsg->_errorCode = errorCode;
    jrMsg->_errorMsg = errorMsg;
    return jrMsg;
}

JobErrorMsg::Ptr JobErrorMsg::createFromJson(nlohmann::json const& jsWReq,
                                             std::string const& replicationInstanceId,
                                             std::string const& replicationAuthKey) {
    string const fName("JobErrorMsg::createFromJson");
    LOGS(_log, LOG_LVL_DEBUG, fName);
    try {
        if (jsWReq["version"] != http::MetaModule::version) {
            LOGS(_log, LOG_LVL_ERROR, fName << " bad version");
            return nullptr;
        }

        // Presumably, if these were wrong, it wouldn't have gotten this far.
        auto repliInstId = http::RequestBodyJSON::required<string>(jsWReq, "instance_id");
        auto repliAuthKey = http::RequestBodyJSON::required<string>(jsWReq, "auth_key");

        auto jrMsg = create(repliInstId, repliAuthKey);
        jrMsg->_workerId = http::RequestBodyJSON::required<string>(jsWReq, "workerid");
        jrMsg->_czarName = http::RequestBodyJSON::required<string>(jsWReq, "czar");
        jrMsg->_czarId = http::RequestBodyJSON::required<CzarIdType>(jsWReq, "czarid");
        jrMsg->_queryId = http::RequestBodyJSON::required<QueryId>(jsWReq, "queryid");
        jrMsg->_uberJobId = http::RequestBodyJSON::required<UberJobId>(jsWReq, "uberjobid");
        jrMsg->_errorMsg = http::RequestBodyJSON::required<string>(jsWReq, "errorMsg");
        jrMsg->_errorCode = http::RequestBodyJSON::required<int>(jsWReq, "errorCode");
        return jrMsg;
    } catch (invalid_argument const& exc) {
        LOGS(_log, LOG_LVL_ERROR, string("JobErrorMsg::createJson invalid ") << exc.what());
    }
    return nullptr;
}

json JobErrorMsg::serializeJson() {
    shared_ptr<json> jsJrReqPtr = make_shared<json>();
    json& jsJr = *jsJrReqPtr;

    // These need to match what http::BaseModule::enforceInstanceId()
    // and http::BaseModule::enforceAuthorization() are looking for.
    jsJr["instance_id"] = _replicationInstanceId;
    jsJr["auth_key"] = _replicationAuthKey;

    jsJr["version"] = http::MetaModule::version;
    jsJr["workerid"] = _workerId;
    jsJr["czar"] = _czarName;
    jsJr["czarid"] = _czarId;
    jsJr["queryid"] = _queryId;
    jsJr["uberjobid"] = _uberJobId;
    jsJr["errorCode"] = _errorCode;
    jsJr["errorMsg"] = _errorMsg;
    return jsJr;
}

}  // namespace lsst::qserv::protojson
