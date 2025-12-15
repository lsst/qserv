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
#include "protojson/UberJobReadyMsg.h"

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
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.UberJobReadyMsg");
}  // namespace

namespace lsst::qserv::protojson {
UberJobStatusMsg::UberJobStatusMsg(AuthContext const& authContext_, unsigned int version_,
                                   string const& workerId_, string const& czarName_, CzarId czarId_,
                                   QueryId queryId_, UberJobId uberJobId_)
        : authContext(authContext_),
          version(version_),
          workerId(workerId_),
          czarName(czarName_),
          czarId(czarId_),
          queryId(queryId_),
          uberJobId(uberJobId_) {
    if (version != http::MetaModule::version) {
        string eMsg = cName(__func__) + " UberJobStatusMsg constructor bad version " + to_string(version);
        LOGS(_log, LOG_LVL_ERROR, eMsg);
        throw invalid_argument(eMsg);
    }
}

bool UberJobStatusMsg::equalsBase(UberJobStatusMsg const& other) const {
    return ((authContext == other.authContext) && (queryId == other.queryId) &&
            (uberJobId == other.uberJobId) && (version == other.version) && (workerId == other.workerId) &&
            (czarName == other.czarName));
}

std::ostream& UberJobStatusMsg::dump(std::ostream& os) const {
    os << "{UberJobStatusMsg:" << " QID=" << queryId << "_ujId=" << uberJobId << " czId=" << czarId
       << " czName=" << czarName << " workerId=" << workerId << " version=" << version << "}";
    return os;
}

std::string UberJobStatusMsg::dump() const {
    std::ostringstream os;
    dump(os);
    return os.str();
}

std::ostream& operator<<(std::ostream& os, UberJobStatusMsg const& ujMsg) { return ujMsg.dump(os); }

string UberJobReadyMsg::cName(const char* fName) const {
    return string("UberJobReadyMsg::") + fName + " QID=" + to_string(queryId) +
           "_ujId=" + to_string(uberJobId);
}

UberJobReadyMsg::Ptr UberJobReadyMsg::create(AuthContext const& authContext_, unsigned int version_,
                                             string const& workerIdStr_, string const& czarName_,
                                             CzarId czarId_, QueryId queryId_, UberJobId uberJobId_,
                                             FileUrlInfo const& fileUrlInfo_) {
    Ptr jrMsg = Ptr(new UberJobReadyMsg(authContext_, version_, workerIdStr_, czarName_, czarId_, queryId_,
                                        uberJobId_, fileUrlInfo_));
    return jrMsg;
}

bool UberJobReadyMsg::equals(UberJobStatusMsg const& other) const {
    try {
        UberJobReadyMsg const& otherReady = dynamic_cast<UberJobReadyMsg const&>(other);
        if (fileUrlInfo == otherReady.fileUrlInfo) {
            return equalsBase(other);
        }
    } catch (std::bad_cast& ex) {
    }
    // different type
    return false;
}

std::ostream& UberJobReadyMsg::dump(std::ostream& os) const {
    os << "{UberJobReadyMsg:";
    UberJobStatusMsg::dump(os);
    os << fileUrlInfo.dump() << "}";
    return os;
}

UberJobReadyMsg::Ptr UberJobReadyMsg::createFromJson(json const& jsWReq) {
    string const fName("UberJobReadyMsg::createFromJson");
    LOGS(_log, LOG_LVL_DEBUG, fName);
    try {
        // If replication identifiers were wrong, it wouldn't have gotten this far.
        AuthContext authContext_(http::RequestBodyJSON::required<string>(jsWReq, "instance_id"),
                                 http::RequestBodyJSON::required<string>(jsWReq, "auth_key"));
        FileUrlInfo fileUrlInfo_(http::RequestBodyJSON::required<string>(jsWReq, "fileUrl"),
                                 http::RequestBodyJSON::required<uint64_t>(jsWReq, "rowCount"),
                                 http::RequestBodyJSON::required<uint64_t>(jsWReq, "fileSize"));
        Ptr jrMsg = Ptr(new UberJobReadyMsg(
                authContext_, http::RequestBodyJSON::required<unsigned int>(jsWReq, "version"),
                http::RequestBodyJSON::required<string>(jsWReq, "workerid"),
                http::RequestBodyJSON::required<string>(jsWReq, "czar"),
                http::RequestBodyJSON::required<CzarId>(jsWReq, "czarid"),
                http::RequestBodyJSON::required<QueryId>(jsWReq, "queryid"),
                http::RequestBodyJSON::required<UberJobId>(jsWReq, "uberjobid"), fileUrlInfo_));
        return jrMsg;
    } catch (invalid_argument const& exc) {
        LOGS(_log, LOG_LVL_ERROR, string("UberJobReadyMsg::createJson invalid ") << exc.what());
    }
    return nullptr;
}

UberJobReadyMsg::UberJobReadyMsg(AuthContext const& authContext_, unsigned int version_,
                                 string const& workerId_, string const& czarName_, CzarId czarId_,
                                 QueryId queryId_, UberJobId uberJobId_, FileUrlInfo const& fileUrlInfo_)
        : UberJobStatusMsg(authContext_, version_, workerId_, czarName_, czarId_, queryId_, uberJobId_),
          fileUrlInfo(fileUrlInfo_) {}

json UberJobReadyMsg::toJson() const {
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
    jsJr["fileUrl"] = fileUrlInfo.fileUrl;
    jsJr["rowCount"] = fileUrlInfo.rowCount;
    jsJr["fileSize"] = fileUrlInfo.fileSize;
    return jsJr;
}

std::string FileUrlInfo::dump() const {
    return std::string("{fileUrl=") + fileUrl + " rowCount=" + std::to_string(rowCount) +
           " fileSize=" + std::to_string(fileSize) + "}";
}

}  // namespace lsst::qserv::protojson
