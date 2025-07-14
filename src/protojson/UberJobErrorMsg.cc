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
#include "protojson/UberJobErrorMsg.h"

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
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.UberJobErrorMsg");
}  // namespace

namespace lsst::qserv::protojson {

string UberJobErrorMsg::_cName(const char* fName) const {
    return string("UberJobErrorMsg::") + fName + " qId=" + to_string(_queryId) +
           " ujId=" + to_string(_uberJobId);
}

UberJobErrorMsg::Ptr UberJobErrorMsg::create(string const& replicationInstanceId,
                                             string const& replicationAuthKey, unsigned int version,
                                             string const& workerIdStr, string const& czarName, CzarId czarId,
                                             QueryId queryId, UberJobId uberJobId, int errorCode,
                                             string const& errorMsg) {
    Ptr jrMsg = Ptr(new UberJobErrorMsg(replicationInstanceId, replicationAuthKey, version, workerIdStr,
                                        czarName, czarId, queryId, uberJobId, errorCode, errorMsg));
    return jrMsg;
}

UberJobErrorMsg::Ptr UberJobErrorMsg::createFromJson(nlohmann::json const& jsWReq,
                                                     string const& replicationInstanceId,
                                                     string const& replicationAuthKey) {
    string const fName("UberJobErrorMsg::createFromJson");
    LOGS(_log, LOG_LVL_DEBUG, fName);
    try {
        Ptr jrMsg = Ptr(new UberJobErrorMsg(http::RequestBodyJSON::required<string>(jsWReq, "instance_id"),
                                            http::RequestBodyJSON::required<string>(jsWReq, "auth_key"),
                                            http::RequestBodyJSON::required<unsigned int>(jsWReq, "version"),
                                            http::RequestBodyJSON::required<string>(jsWReq, "workerid"),
                                            http::RequestBodyJSON::required<string>(jsWReq, "czar"),
                                            http::RequestBodyJSON::required<CzarId>(jsWReq, "czarid"),
                                            http::RequestBodyJSON::required<QueryId>(jsWReq, "queryid"),
                                            http::RequestBodyJSON::required<UberJobId>(jsWReq, "uberjobid"),
                                            http::RequestBodyJSON::required<int>(jsWReq, "errorCode"),
                                            http::RequestBodyJSON::required<string>(jsWReq, "errorMsg")));
        return jrMsg;
    } catch (invalid_argument const& exc) {
        LOGS(_log, LOG_LVL_ERROR, string("UberJobErrorMsg::createJson invalid ") << exc.what());
    }
    return nullptr;
}

UberJobErrorMsg::UberJobErrorMsg(string const& replicationInstanceId, string const& replicationAuthKey,
                                 unsigned int version, string const& workerId, string const& czarName,
                                 CzarId czarId, QueryId queryId, UberJobId uberJobId, int errorCode,
                                 string const& errorMsg)
        : _replicationInstanceId(replicationInstanceId),
          _replicationAuthKey(replicationAuthKey),
          _version(version),
          _workerId(workerId),
          _czarName(czarName),
          _czarId(czarId),
          _queryId(queryId),
          _uberJobId(uberJobId),
          _errorCode(errorCode),
          _errorMsg(errorMsg) {
    if (_version != http::MetaModule::version) {
        string eMsg = _cName(__func__) + " bad version " + to_string(_version);
        LOGS(_log, LOG_LVL_ERROR, eMsg);
        throw invalid_argument(eMsg);
    }
}

json UberJobErrorMsg::toJson() const {
    json jsJr;

    // These need to match what http::BaseModule::enforceInstanceId()
    // and http::BaseModule::enforceAuthorization() are looking for.
    jsJr["instance_id"] = _replicationInstanceId;
    jsJr["auth_key"] = _replicationAuthKey;
    jsJr["version"] = _version;

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
