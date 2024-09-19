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

#include "../wcontrol/WCzarInfoMap.h"
// System headers

// Third party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "http/Client.h"
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/Method.h"
#include "http/RequestBodyJSON.h"
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
    //&&&string const funcN = cName(__func__);
    LOGS(_log, LOG_LVL_TRACE,
            cName(__func__) << " httpFileUrl=" << httpFileUrl << " rows=" << rowCount << " fSize=" << fileSize
               << " headerCount=" << headerCount);

    string workerIdStr;
    if (_foreman != nullptr) {
        workerIdStr = _foreman->chunkInventory()->id();
    } else {
        workerIdStr = "dummyWorkerIdStr";
        LOGS(_log, LOG_LVL_INFO, cName(__func__) << " _foreman was null, which should only happen in unit tests");
    }

    json request = {{"version", http::MetaModule::version},
                    {"workerid", workerIdStr},
                    {"auth_key", _authKey},
                    {"czar", _czarName},
                    {"czarid", _czarId},
                    {"queryid", _queryId},
                    {"uberjobid", _uberJobId},
                    {"fileUrl", httpFileUrl},
                    {"rowCount", rowCount},
                    {"fileSize", fileSize},
                    {"headerCount", headerCount}};

    auto const method = http::Method::POST;
    vector<string> const headers = {"Content-Type: application/json"};
    string const url = "http://" + _czarHost + ":" + to_string(_czarPort) + "/queryjob-ready";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    string const requestStr = request.dump();
    _queueUJResponse(method, headers, url, requestContext, requestStr);
    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& end");
}


bool UberJobData::responseError(util::MultiError& multiErr, std::shared_ptr<Task> const& task,
                                bool cancelled) {
    LOGS(_log, LOG_LVL_INFO, cName(__func__));
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
                cName(__func__) + " error(s) in result for chunk #" + to_string(task->getChunkId()) + ": " + errorMsg;
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

    auto const method = http::Method::POST;
    vector<string> const headers = {"Content-Type: application/json"};
    string const url = "http://" + _czarHost + ":" + to_string(_czarPort) + "/queryjob-error";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    string const requestStr = request.dump();
    _queueUJResponse(method, headers, url, requestContext, requestStr);
    return true;
}

void UberJobData::_queueUJResponse(http::Method method_, std::vector<std::string> const& headers_, std::string const& url_, std::string const& requestContext_, std::string const& requestStr_) {
    util::QdispPool::Ptr wPool;
    if (_foreman != nullptr) {
        wPool = _foreman->getWPool();
    }

    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& creating UJTransmitCmd wPool=" << wPool);
    auto cmdTransmit = UJTransmitCmd::create(_foreman, shared_from_this(), method_, headers_, url_, requestContext_, requestStr_);
    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& created UJTransmitCmd wPool=" << wPool);
    if (wPool == nullptr) {
        // No thread pool. Run the command now. This should only happen in unit tests.
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& creating UJTransmitCmd direct run action");
        cmdTransmit->action(nullptr);
    } else {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& creating UJTransmitCmd queue transmit");
        if (_scanInteractive) {
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& creating UJTransmitCmd queue transmit_0");
            wPool->queCmd(cmdTransmit, 0);
        }else {
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& creating UJTransmitCmd queue transmit_1");
            wPool->queCmd(cmdTransmit, 1);
        }
    }
}

void UberJobData::cancelAllTasks() {
    LOGS(_log, LOG_LVL_INFO, cName(__func__));
    if (_cancelled.exchange(true) == false) {
        lock_guard<mutex> lg(_ujTasksMtx);
        for (auto const& task : _ujTasks) {
            task->cancel();
        }
    }
}

string UJTransmitCmd::cName(const char* funcN) const {
    stringstream os;
    os << "UJTransmitCmd::" << funcN << " czId=" << _czarId << " qId=" << _queryId << " ujId=" << _uberJobId;
    return os.str();
}

void UJTransmitCmd::action(util::CmdData* data) {
    _attemptCount++;
    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& start attempt=" << _attemptCount);
    auto ujPtr = _ujData.lock();
    if (ujPtr == nullptr || ujPtr->getCancelled()) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " UberJob was cancelled " << _attemptCount);
    }
    http::Client client(_method, _url, _requestStr, _headers);
    bool transmitSuccess = false;
    try {
        json const response = client.readAsJson();
        if (0 != response.at("success").get<int>()) {
            transmitSuccess = true;
            _selfPtr.reset(); // clear so this can be deleted.
        } else {
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << " Transmit success == 0");
            // There's no point in re-sending as the czar got the message and didn't like
            // it.
            // &&& maybe add this czId+ujId to a list of failed uberjobs that can be put
            // &&& status return??? Probably overkill.
            _selfPtr.reset(); // clear so this can be deleted.
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& start d except");
        LOGS(_log, LOG_LVL_WARN, cName(__func__) + " " + _requestContext + " failed, ex: " + ex.what());
    }

    if (!transmitSuccess) {
        auto sPtr = _selfPtr;
        if (_foreman != nullptr && sPtr != nullptr) {
            // Do not reset _selfPtr as re-queuing may be needed several times.
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << " no response for transmit, putting on failed transmit queue.");
            auto wCzInfo = _foreman->getWCzarInfoMap()->getWCzarInfo(_czarId);
            // This will check if the czar is believed to be alive and try the queue the query to be tried again
            // at a lower priority. It it thinks the czar is dead, it will throw it away.
            // TODO:UJ &&& I have my doubts about this as a reconnected czar may go down in flames
            //         &&& as it is hit with thousands of these.
            //         &&& Alternate plan, set a flag in the status message response (WorkerQueryStatusData)
            //         &&& indicates some messages failed. When the czar sees the flag, it'll request a
            //         &&& message from the worker that contains all of the failed transmit data and handle
            //         &&& that. All of these failed transmits should fit in a single message.
            if (wCzInfo->checkAlive(CLOCK::now())) {
                auto wPool = _foreman->getWPool();
                if (wPool != nullptr) {
                    Ptr replacement = duplicate();
                    _selfPtr.reset();
                    if (replacement != nullptr) {
                        wPool->queCmd(replacement, 2);
                    } else {
                        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " replacement was null");
                    }
                } else{
                    // No thread pool, should only be possible in unit tests.
                    LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " no wPool");
                    _selfPtr.reset();
                    return;
                }
            }
        } else {
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " _selfPtr was null, assuming job killed.");
            _selfPtr.reset(); // In case _foreman is null.
        }
    }
    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " &&& start end");
}

void UJTransmitCmd::kill() {
    string const funcN("UJTransmitCmd::kill");
    LOGS(_log, LOG_LVL_WARN, funcN);
    auto sPtr = _selfPtr;
    _selfPtr.reset();
    if (sPtr == nullptr) { return; }
    // &&& TODO:UJ Is there anything that should be done here???
}

UJTransmitCmd::Ptr UJTransmitCmd::duplicate() {
    auto ujD = _ujData.lock();
    if (ujD == nullptr) {
        return nullptr;
    }
    Ptr newPtr = create(_foreman, ujD, _method, _headers, _url, _requestContext, _requestStr);
    newPtr->_attemptCount = _attemptCount;
    return newPtr;

}

}  // namespace lsst::qserv::wbase
