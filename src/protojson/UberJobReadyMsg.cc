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

string UberJobReadyMsg::_cName(const char* fName) const {
    return string("UberJobReadyMsg::") + fName + " qId=" + to_string(_queryId) +
           " ujId=" + to_string(_uberJobId);
}

UberJobReadyMsg::Ptr UberJobReadyMsg::create(string const& replicationInstanceId,
                                             string const& replicationAuthKey, unsigned int version,
                                             string const& workerIdStr, string const& czarName,
                                             CzarIdType czarId, QueryId queryId, UberJobId uberJobId,
                                             string const& fileUrl, uint64_t rowCount, uint64_t fileSize) {
    Ptr jrMsg = Ptr(new UberJobReadyMsg(replicationInstanceId, replicationAuthKey, version, workerIdStr,
                                        czarName, czarId, queryId, uberJobId, fileUrl, rowCount, fileSize));
    return jrMsg;
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
                                            http::RequestBodyJSON::required<CzarIdType>(jsWReq, "czarid"),
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
                                 CzarIdType czarId, QueryId queryId, UberJobId uberJobId,
                                 string const& fileUrl, uint64_t rowCount, uint64_t fileSize)
        : _replicationInstanceId(replicationInstanceId),
          _replicationAuthKey(replicationAuthKey),
          _version(version),
          _workerId(workerId),
          _czarName(czarName),
          _czarId(czarId),
          _queryId(queryId),
          _uberJobId(uberJobId),
          _fileUrl(fileUrl),
          _rowCount(rowCount),
          _fileSize(fileSize) {
    if (_version != http::MetaModule::version) {
        string eMsg = _cName(__func__) + " bad version " + to_string(_version);
        LOGS(_log, LOG_LVL_ERROR, eMsg);
        throw invalid_argument(eMsg);
    }
}

json UberJobReadyMsg::toJson() const {
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
    return jsJr;
}

}  // namespace lsst::qserv::protojson
