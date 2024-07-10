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
#include "util/MultiError.h"
#include "wcontrol/Foreman.h"
#include "wpublish/ChunkInventory.h"
#include "wpublish/QueriesAndChunks.h"

using namespace std;
using namespace nlohmann;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.UberJobData");

}  // namespace

namespace lsst::qserv::wbase {

UberJobData::UberJobData(UberJobId uberJobId, std::string const& czarName, qmeta::CzarId czarId,
                         std::string czarHost, int czarPort, uint64_t queryId, std::string const& workerId,
                         std::shared_ptr<wcontrol::Foreman> const& foreman, std::string const& authKey)
        : _uberJobId(uberJobId),
          _czarName(czarName),
          _czarId(czarId),
          _czarHost(czarHost),
          _czarPort(czarPort),
          _queryId(queryId),
          _workerId(workerId),
          _authKey(authKey),
          _foreman(foreman),
          _idStr(string("QID=") + to_string(_queryId) + ":ujId=" + to_string(_uberJobId)) {}

void UberJobData::setFileChannelShared(std::shared_ptr<FileChannelShared> const& fileChannelShared) {
    if (_fileChannelShared != nullptr && _fileChannelShared != fileChannelShared) {
        throw util::Bug(ERR_LOC, string(__func__) + " Trying to change _fileChannelShared");
    }
    _fileChannelShared = fileChannelShared;
}

void UberJobData::responseFileReady(string const& httpFileUrl, uint64_t rowCount, uint64_t fileSize,
                                    uint64_t headerCount) {
    string const funcN = cName(__func__);
    LOGS(_log, LOG_LVL_WARN,
         funcN << "&&& UberJobData::responseFileReady a httpFileUrl=" << httpFileUrl << " rows=" << rowCount
               << " fSize=" << fileSize << " headerCount=" << headerCount);

    json request = {{"version", http::MetaModule::version},
                    {"workerid", _foreman->chunkInventory()->id()},
                    {"auth_key", _authKey},
                    {"czar", _czarName},
                    {"czarid", _czarId},
                    {"queryid", _queryId},
                    {"uberjobid", _uberJobId},
                    {"fileUrl", httpFileUrl},
                    {"rowCount", rowCount},
                    {"fileSize", fileSize},
                    {"headerCount", headerCount}};

    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::responseFileReady b");

    auto const method = http::Method::POST;
    vector<string> const headers = {"Content-Type: application/json"};
    //&&&string const url = "http://" + _czarName + ":" + to_string(_czarPort) + "/queryjob-ready";
    string const url = "http://" + _czarHost + ":" + to_string(_czarPort) + "/queryjob-ready";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    LOGS(_log, LOG_LVL_WARN,
         funcN + "&&&uj UberJobData::responseFileReady url=" << url << " request=" << request.dump());
    http::Client client(method, url, request.dump(), headers);

    int maxTries = 2;  // &&& set from config
    bool transmitSuccess = false;
    for (int j = 0; (!transmitSuccess && j < maxTries); ++j) {
        LOGS(_log, LOG_LVL_WARN,
             funcN + "&&&uj UberJobData::responseFileReady j=" << j << " url=" << url
                                                               << " request=" << request.dump());
        try {
            json const response = client.readAsJson();
            LOGS(_log, LOG_LVL_WARN, funcN << "&&&uj response=" << response);
            if (0 != response.at("success").get<int>()) {
                LOGS(_log, LOG_LVL_WARN, funcN << "&&&uj success");
                transmitSuccess = true;
            } else {
                LOGS(_log, LOG_LVL_WARN,
                     funcN << "&&&uj NEED CODE success=0, result file should probably be deleted.");
                j = maxTries;  /// There's no point in resending as the czar got the message and didn't like
                               /// it.
            }
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN, funcN + " " + requestContext + " &&&uj failed, ex: " + ex.what());
        }
    }

    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_ERROR,
             funcN << "&&&uj NEED CODE Let czar find out through polling worker status??? Just throw the "
                      "result away???");
    }
    LOGS(_log, LOG_LVL_WARN, funcN << "&&& UberJobData::responseFileReady end");
}

bool UberJobData::responseError(util::MultiError& multiErr, std::shared_ptr<Task> const& task,
                                bool cancelled) {
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::responseError a");
    string const funcN = cName(__func__);

    string errorMsg;
    int errorCode = 0;
    if (!multiErr.empty()) {
        errorMsg = multiErr.toOneLineString();
        errorCode = multiErr.firstErrorCode();
    } else if (cancelled) {
        errorMsg = "cancelled";
        errorCode = -1;
    }
    if (!errorMsg.empty() or (errorCode != 0)) {
        errorMsg =
                funcN + " error(s) in result for chunk #" + to_string(task->getChunkId()) + ": " + errorMsg;
        LOGS(_log, LOG_LVL_ERROR, errorMsg);
    }

    json request = {{"version", http::MetaModule::version},
                    {"workerid", _foreman->chunkInventory()->id()},
                    {"auth_key", _authKey},
                    {"czar", _czarName},
                    {"czarid", _czarId},
                    {"queryid", _queryId},
                    {"uberjobid", _uberJobId},
                    {"errorCode", errorCode},
                    {"errorMsg", errorMsg}};

    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::responseError b");

    auto const method = http::Method::POST;
    vector<string> const headers = {"Content-Type: application/json"};
    //&&&string const url = "http://" + _czarName + ":" + to_string(_czarPort) + "/queryjob-ready";
    string const url = "http://" + _czarHost + ":" + to_string(_czarPort) + "/queryjob-error";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    LOGS(_log, LOG_LVL_WARN, "&&& UberJobData::responseError c");
    LOGS(_log, LOG_LVL_WARN,
         funcN << "&&&uj UberJobData::responseError url=" << url << " request=" << request.dump());
    http::Client client(method, url, request.dump(), headers);

    int maxTries = 2;  // &&& set from config
    bool transmitSuccess = false;
    for (int j = 0; !transmitSuccess && j < maxTries; ++j) {
        try {
            json const response = client.readAsJson();
            LOGS(_log, LOG_LVL_WARN, funcN << "&&&uj response=" << response);
            if (0 != response.at("success").get<int>()) {
                LOGS(_log, LOG_LVL_WARN, funcN << "&&&uj success");
                transmitSuccess = true;
            } else {
                LOGS(_log, LOG_LVL_WARN, funcN << "&&&uj NEED CODE success=0");
                j = maxTries;  /// There's no point in resending as the czar got the message and didn't like
                               /// it.
            }
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_WARN, funcN + " " + requestContext + " &&&uj failed, ex: " + ex.what());
        }
    }
    return transmitSuccess;
}

}  // namespace lsst::qserv::wbase
