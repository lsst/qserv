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

UberJobStatusMsg::UberJobStatusMsg(string const& replicationInstanceId, string const& replicationAuthKey,
                                   unsigned int version, string const& workerId, string const& czarName,
                                   CzarId czarId, QueryId queryId, UberJobId uberJobId)
        : _replicationInstanceId(replicationInstanceId),
          _replicationAuthKey(replicationAuthKey),
          _version(version),
          _workerId(workerId),
          _czarName(czarName),
          _czarId(czarId),
          _queryId(queryId),
          _uberJobId(uberJobId) {
    if (_version != http::MetaModule::version) {
        string eMsg = cName(__func__) + " UberJobStatusMsg constructor bad version " + to_string(_version);
        LOGS(_log, LOG_LVL_ERROR, eMsg);
        throw invalid_argument(eMsg);
    }
}

bool UberJobStatusMsg::equalsBase(UberJobStatusMsg const& other) const {
    return ((_replicationInstanceId == other._replicationInstanceId) &&
            (_replicationAuthKey == other._replicationAuthKey) && (_czarId == other._czarId) &&
            (_queryId == other._queryId) && (_uberJobId == other._uberJobId) &&
            (_version == other._version) && (_workerId == other._workerId) && (_czarName == other._czarName));
}

std::ostream& UberJobStatusMsg::dumpOS(std::ostream& os) const {
    os << "{UberJobStatusMsg:" << " QID=" << _queryId << "_ujId=" << _uberJobId << " czId=" << _czarId
       << " czName=" << _czarName << " workerId=" << _workerId << " version=" << _version << "}";
    return os;
}

std::string UberJobStatusMsg::dump() const {
    std::ostringstream os;
    dumpOS(os);
    return os.str();
}

std::ostream& operator<<(std::ostream& os, UberJobStatusMsg const& ujMsg) { return ujMsg.dumpOS(os); }

string UberJobReadyMsg::cName(const char* fName) const {
    return string("UberJobReadyMsg::") + fName + " QID=" + to_string(_queryId) +
           "_ujId=" + to_string(_uberJobId);
}

UberJobReadyMsg::Ptr UberJobReadyMsg::create(string const& replicationInstanceId,
                                             string const& replicationAuthKey, unsigned int version,
                                             string const& workerIdStr, string const& czarName, CzarId czarId,
                                             QueryId queryId, UberJobId uberJobId, string const& fileUrl,
                                             uint64_t rowCount, uint64_t fileSize) {
    Ptr jrMsg = Ptr(new UberJobReadyMsg(replicationInstanceId, replicationAuthKey, version, workerIdStr,
                                        czarName, czarId, queryId, uberJobId, fileUrl, rowCount, fileSize));
    return jrMsg;
}

bool UberJobReadyMsg::equals(UberJobStatusMsg const& other) const {
    try {
        UberJobReadyMsg const& otherReady = dynamic_cast<UberJobReadyMsg const&>(other);
        if ((_fileUrl == otherReady._fileUrl) && (_rowCount == otherReady._rowCount) &&
            (_fileSize == otherReady._fileSize)) {
            return equalsBase(other);
        }
    } catch (std::bad_cast& ex) {
    }
    // different type
    return false;
}

std::ostream& UberJobReadyMsg::dumpOS(std::ostream& os) const {
    os << "{UberJobReadyMsg:";
    UberJobStatusMsg::dumpOS(os);
    os << " fileUrl=" << _fileUrl << " rowCount=" << _rowCount << " fileSize=" << _fileSize << "}";
    return os;
}

UberJobReadyMsg::Ptr UberJobReadyMsg::createFromJson(json const& jsWReq) {
    string const fName("UberJobReadyMsg::createFromJson");
    LOGS(_log, LOG_LVL_DEBUG, fName);
    try {
        // If replication identifiers were wrong, it wouldn't have gotten this far.
        Ptr jrMsg = Ptr(new UberJobReadyMsg(http::RequestBodyJSON::required<string>(jsWReq, "instance_id"),
                                            http::RequestBodyJSON::required<string>(jsWReq, "auth_key"),
                                            http::RequestBodyJSON::required<unsigned int>(jsWReq, "version"),
                                            http::RequestBodyJSON::required<string>(jsWReq, "workerid"),
                                            http::RequestBodyJSON::required<string>(jsWReq, "czar"),
                                            http::RequestBodyJSON::required<CzarId>(jsWReq, "czarid"),
                                            http::RequestBodyJSON::required<QueryId>(jsWReq, "queryid"),
                                            http::RequestBodyJSON::required<UberJobId>(jsWReq, "uberjobid"),
                                            http::RequestBodyJSON::required<string>(jsWReq, "fileUrl"),
                                            http::RequestBodyJSON::required<uint64_t>(jsWReq, "rowCount"),
                                            http::RequestBodyJSON::required<uint64_t>(jsWReq, "fileSize")));
        return jrMsg;
    } catch (invalid_argument const& exc) {
        LOGS(_log, LOG_LVL_ERROR, string("UberJobReadyMsg::createJson invalid ") << exc.what());
    }
    return nullptr;
}

UberJobReadyMsg::UberJobReadyMsg(string const& replicationInstanceId, string const& replicationAuthKey,
                                 unsigned int version, string const& workerId, string const& czarName,
                                 CzarId czarId, QueryId queryId, UberJobId uberJobId, string const& fileUrl,
                                 uint64_t rowCount, uint64_t fileSize)
        : UberJobStatusMsg(replicationInstanceId, replicationAuthKey, version, workerId, czarName, czarId,
                           queryId, uberJobId),
          _fileUrl(fileUrl),
          _rowCount(rowCount),
          _fileSize(fileSize) {}

shared_ptr<json> UberJobReadyMsg::toJsonPtr() const {
    shared_ptr<json> jsJrReqPtr = make_shared<json>();
    json& jsJr = *jsJrReqPtr;

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
    jsJr["fileUrl"] = _fileUrl;
    jsJr["rowCount"] = _rowCount;
    jsJr["fileSize"] = _fileSize;
    return jsJrReqPtr;
}

}  // namespace lsst::qserv::protojson
