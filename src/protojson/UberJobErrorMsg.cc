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

string UberJobErrorMsg::cName(const char* fName) const {
    return string("UberJobErrorMsg::") + fName + " qId=" + to_string(queryId) +
           " ujId=" + to_string(uberJobId);
}

UberJobErrorMsg::Ptr UberJobErrorMsg::create(AuthContext const& authContext_, unsigned int version_,
                                             string const& workerIdStr_, string const& czarName_,
                                             CzarId czarId_, QueryId queryId_, UberJobId uberJobId_,
                                             int errorCode_, string const& errorMsg_) {
    Ptr jrMsg = Ptr(new UberJobErrorMsg(authContext_, version_, workerIdStr_, czarName_, czarId_, queryId_,
                                        uberJobId_, errorCode_, errorMsg_));
    return jrMsg;
}

UberJobErrorMsg::Ptr UberJobErrorMsg::createFromJson(nlohmann::json const& jsWReq) {
    string const fName("UberJobErrorMsg::createFromJson");
    LOGS(_log, LOG_LVL_DEBUG, fName);
    try {
        AuthContext const authContext_(http::RequestBodyJSON::required<string>(jsWReq, "instance_id"),
                                       http::RequestBodyJSON::required<string>(jsWReq, "auth_key"));
        Ptr jrMsg = Ptr(new UberJobErrorMsg(authContext_,
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

bool UberJobErrorMsg::equals(UberJobStatusMsg const& other) const {
    try {
        UberJobErrorMsg const& otherError = dynamic_cast<UberJobErrorMsg const&>(other);
        if ((errorCode == otherError.errorCode) && (errorMsg == otherError.errorMsg)) {
            return equalsBase(other);
        }
    } catch (std::bad_cast& ex) {
    }
    // different type
    return false;
}

std::ostream& UberJobErrorMsg::dump(std::ostream& os) const {
    os << "{UberJobErrorMsg:";
    UberJobStatusMsg::dump(os);
    os << " errorCode=" << errorCode << " errorMsg=" << errorMsg << "}";
    return os;
}

UberJobErrorMsg::UberJobErrorMsg(AuthContext const& authContext_, unsigned int version_,
                                 string const& workerId_, string const& czarName_, CzarId czarId_,
                                 QueryId queryId_, UberJobId uberJobId_, int errorCode_,
                                 string const& errorMsg_)
        : UberJobStatusMsg(authContext_, version_, workerId_, czarName_, czarId_, queryId_, uberJobId_),
          errorCode(errorCode_),
          errorMsg(errorMsg_) {}

json UberJobErrorMsg::toJson() const {
    json jsJr;

    // These need to match what http::BaseModule::enforceInstanceId()
    // and http::BaseModule::enforceAuthorization() are looking for.
    jsJr["instance_id"] = authContext.replicationInstanceId;
    jsJr["auth_key"] = authContext.replicationAuthKey;
    jsJr["version"] = version;

    jsJr["workerid"] = workerId;
    jsJr["czar"] = czarName;
    jsJr["czarid"] = czarId;
    jsJr["queryid"] = queryId;
    jsJr["uberjobid"] = uberJobId;
    jsJr["errorCode"] = errorCode;
    jsJr["errorMsg"] = errorMsg;
    return jsJr;
}

}  // namespace lsst::qserv::protojson
