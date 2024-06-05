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
#include "wbase/UberJobData.h"

// System headers

// Third party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "http/Client.h"
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/Method.h"
#include "http/RequestBody.h"
#include "http/RequestQuery.h"
#include "util/Bug.h"
#include "wcontrol/Foreman.h"
#include "wpublish/ChunkInventory.h"
#include "wpublish/QueriesAndChunks.h"

using namespace std;
using namespace nlohmann;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.UberJobData");

}  // namespace

namespace lsst::qserv::wbase {

UberJobData::UberJobData(UberJobId uberJobId, std::string const& czarName, qmeta::CzarId czarId, std::string czarHost, int czarPort,
                         uint64_t queryId, std::string const& workerId,
                         std::shared_ptr<wcontrol::Foreman> const& foreman, std::string const& authKey)
        : _uberJobId(uberJobId),
          _czarName(czarName),
          _czarId(czarId),
          _czarHost(czarHost),
          _czarPort(czarPort),
          _queryId(queryId),
          _workerId(workerId),
          _authKey(authKey),
          _foreman(foreman) {}

void UberJobData::setFileChannelShared(std::shared_ptr<FileChannelShared> const& fileChannelShared) {
    if (_fileChannelShared != nullptr && _fileChannelShared != fileChannelShared) {
        throw util::Bug(ERR_LOC, string(__func__) + " Trying to change _fileChannelShared");
    }
    _fileChannelShared = fileChannelShared;
}

void UberJobData::fileReadyResponse(string const& httpFileUrl, uint64_t rowCount, uint64_t fileSize) {
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::fileReadyResponse a httpFileUrl=" << httpFileUrl);

    json request = {{"version", http::MetaModule::version},
                    {"workerid", _foreman->chunkInventory()->id()},
                    {"auth_key", _authKey},
                    {"czar", _czarName},
                    {"czarid", _czarId},
                    {"queryid", _queryId},
                    {"uberjobid", _uberJobId},
                    {"fileUrl", httpFileUrl},
                    {"rowCount", rowCount},
                    {"fileSize", fileSize}};

    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::fileReadyResponse b");

    auto const method = http::Method::POST;
    vector<string> const headers = {"Content-Type: application/json"};
    //&&&string const url = "http://" + _czarName + ":" + to_string(_czarPort) + "/queryjob-ready";
    string const url = "http://" + _czarHost + ":" + to_string(_czarPort) + "/queryjob-ready";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::fileReadyResponse c");
    LOGS(_log, LOG_LVL_WARN, "&&&uj UberJobData::fileReadyResponse url=" << url << " request=" << request.dump());
    http::Client client(method, url, request.dump(), headers);

    int maxTries = 2; // &&& set from config
    bool transmitSuccess = false;
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::fileReadyResponse d");
    for (int j=0; j<maxTries; ++j) {
        LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::fileReadyResponse d1");
        try {
            LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::fileReadyResponse d2");
            json const response = client.readAsJson();
            LOGS(_log, LOG_LVL_WARN, __func__ << "&&&uj response=" << response);
            if (0 != response.at("success").get<int>()) {
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&uj success");
                transmitSuccess = true;
            } else {
                LOGS(_log, LOG_LVL_WARN, __func__ << "&&&uj NEED CODE success=0");
            }
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN, requestContext + " &&&uj failed, ex: " + ex.what());
        }
    }
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::fileReadyResponse e");

    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_ERROR,
             __func__ << "&&&uj NEED CODE Let czar find out through polling worker status???");
    } else {
        LOGS(_log, LOG_LVL_WARN, __func__ << "&&&uj NEED CODE do nothing, czar should collect file");
    }
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::fileReadyResponse end");
}



}  // namespace lsst::qserv::wbase
