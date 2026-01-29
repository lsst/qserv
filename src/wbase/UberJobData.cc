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
#include "boost/filesystem.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "http/Client.h"
#include "http/Exceptions.h"
#include "http/MetaModule.h"
#include "http/Method.h"
#include "http/RequestBodyJSON.h"
#include "http/RequestQuery.h"
#include "protojson/UberJobErrorMsg.h"
#include "protojson/UberJobReadyMsg.h"
#include "protojson/WorkerCzarComIssue.h"
#include "util/Bug.h"
#include "util/MultiError.h"
#include "util/ResultFileName.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/Foreman.h"
#include "wcontrol/WCzarInfoMap.h"
#include "wpublish/ChunkInventory.h"
#include "wpublish/QueriesAndChunks.h"

using namespace std;
using namespace nlohmann;

namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.UberJobData");

}  // namespace

namespace lsst::qserv::wbase {

UberJobData::UberJobData(UberJobId uberJobId, std::string const& czarName, CzarId czarId,
                         std::string czarHost, int czarPort, uint64_t queryId, int rowLimit,
                         uint64_t maxTableSizeBytes, protojson::ScanInfo::Ptr const& scanInfo,
                         bool scanInteractive, std::string const& workerId,
                         std::shared_ptr<wcontrol::Foreman> const& foreman, std::string const& authKey,
                         uint16_t resultsHttpPort)
        : UberJobBase(queryId, uberJobId, czarId),
          _czarName(czarName),
          _czarHost(czarHost),
          _czarPort(czarPort),
          _rowLimit(rowLimit),
          _maxTableSizeBytes(maxTableSizeBytes),
          _workerId(workerId),
          _authKey(authKey),
          _resultsHttpPort(resultsHttpPort),
          _foreman(foreman),
          _scanInteractive(scanInteractive),
          _scanInfo(scanInfo),
          _idStr(string("QID=") + to_string(_queryId) + "_ujId=" + to_string(_uberJobId)) {}

void UberJobData::setFileChannelShared(std::shared_ptr<FileChannelShared> const& fileChannelShared) {
    if (_fileChannelShared != nullptr && _fileChannelShared != fileChannelShared) {
        throw util::Bug(ERR_LOC, string(__func__) + " Trying to change _fileChannelShared");
    }
    _fileChannelShared = fileChannelShared;
}

void UberJobData::responseFileReady(protojson::FileUrlInfo const& fileUrlInfo_) {
    LOGS(_log, LOG_LVL_INFO, cName(__func__) << fileUrlInfo_.dump());

    // Latch to prevent errors from being transmitted.
    // NOTE: Calls to responseError() and responseFileReady() are protected by the
    //       mutex in FileChannelShared (_tMtx).
    if (_responseState.exchange(SENDING_FILEURL) != NOTHING) {
        LOGS(_log, LOG_LVL_ERROR,
             cName(__func__) << " _responseState was " << _responseState << " instead of NOTHING");
    }

    protojson::AuthContext authContext_(wconfig::WorkerConfig::instance()->replicationInstanceId(),
                                        wconfig::WorkerConfig::instance()->replicationAuthKey());
    auto ujMsg = responseFileReadyBuild(fileUrlInfo_, authContext_);

    auto const method = http::Method::POST;
    vector<string> const headers = {"Content-Type: application/json"};
    string const url = "http://" + _czarHost + ":" + to_string(_czarPort) + "/queryjob-ready";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    _queueUJResponse(method, headers, url, requestContext, ujMsg);
}

shared_ptr<protojson::UberJobReadyMsg> UberJobData::responseFileReadyBuild(
        protojson::FileUrlInfo const& fileUrlInfo_, protojson::AuthContext const& authContext_) {
    string workerIdStr;
    if (_foreman != nullptr) {
        workerIdStr = _foreman->chunkInventory()->id();
    } else {
        workerIdStr = "dummyWorkerIdStr";
        LOGS(_log, LOG_LVL_INFO,
             cName(__func__) << " _foreman was null, which should only happen in unit tests");
    }

    unsigned int const version = http::MetaModule::version;
    auto ujMsg = protojson::UberJobReadyMsg::create(authContext_, version, workerIdStr, _czarName, _czarId,
                                                    _queryId, _uberJobId, fileUrlInfo_);

    return ujMsg;
}

void UberJobData::responseError(util::MultiError& multiErr, int chunkId, bool cancelled, int logLvl) {
    LOGS(_log, logLvl, cName(__func__));
    // NOTE: Calls to responseError() and responseFileReady() are protected by the
    //       mutex in FileChannelShared (_tMtx).
    if (_responseState == NOTHING) {
        _responseState = SENDING_ERROR;
    } else {
        LOGS(_log, LOG_LVL_WARN,
             cName(__func__) << " Already sending a different message. NOT sending [" << multiErr << "]");
        return;
    }

    protojson::AuthContext authContext_(wconfig::WorkerConfig::instance()->replicationInstanceId(),
                                        wconfig::WorkerConfig::instance()->replicationAuthKey());

    auto jrMsg = responseErrorBuild(multiErr, chunkId, cancelled, logLvl, authContext_);

    auto const method = http::Method::POST;
    vector<string> const headers = {"Content-Type: application/json"};
    string const url = "http://" + _czarHost + ":" + to_string(_czarPort) + "/queryjob-error";
    string const requestContext = "Worker: '" + http::method2string(method) + "' request to '" + url + "'";
    _queueUJResponse(method, headers, url, requestContext, jrMsg);
}

shared_ptr<protojson::UberJobErrorMsg> UberJobData::responseErrorBuild(
        util::MultiError& multiErr, int chunkId, bool cancelled, int logLvl,
        protojson::AuthContext const& authContext_) {
    string workerIdStr;
    if (_foreman != nullptr) {
        workerIdStr = _foreman->chunkInventory()->id();
    } else {
        workerIdStr = "dummyWorkerIdStr";
        LOGS(_log, LOG_LVL_INFO,
             cName(__func__) << " _foreman was null, which should only happen in unit tests");
    }

    if (cancelled) {
        util::Error err(util::Error::CANCEL, util::Error::NONE, "cancelled");
        multiErr.insert(err);
    }
    LOGS(_log, logLvl,
         cName(__func__) + " error(s) in result for chunk #" + to_string(chunkId) + ":" +
                 multiErr.toOneLineString());
    unsigned int const version = http::MetaModule::version;
    auto jrMsg = protojson::UberJobErrorMsg::create(authContext_, version, workerIdStr, _czarName, _czarId,
                                                    _queryId, _uberJobId, multiErr);

    return jrMsg;
}

void UberJobData::_queueUJResponse(http::Method method_, vector<string> const& headers_, string const& url_,
                                   string const& requestContext_,
                                   shared_ptr<protojson::UberJobStatusMsg> const& ujMsg_) {
    util::QdispPool::Ptr wPool;
    if (_foreman != nullptr) {
        wPool = _foreman->getWPool();
    }

    auto thisPtr = static_pointer_cast<UberJobData>(shared_from_this());
    if (thisPtr == nullptr) {
        throw util::Bug(ERR_LOC, "Bad thisPtr in UberJobData::_queueUJResponse");
    }
    auto cmdTransmit =
            UJTransmitCmd::create(_foreman, thisPtr, method_, headers_, url_, requestContext_, ujMsg_);
    if (wPool == nullptr) {
        // No thread pool. Run the command now. This should only happen in unit tests.
        cmdTransmit->action(nullptr);
    } else {
        if (_scanInteractive) {
            wPool->queCmd(cmdTransmit, 0);
        } else {
            wPool->queCmd(cmdTransmit, 1);
        }
    }
}

string UberJobData::_resultFileName() const {
    return util::ResultFileName(_czarId, _queryId, _uberJobId).fileName();
}

string UberJobData::resultFilePath() const {
    string const resultsDirname = wconfig::WorkerConfig::instance()->resultsDirname();
    if (resultsDirname.empty()) return resultsDirname;
    return (fs::path(resultsDirname) / _resultFileName()).string();
}

std::string UberJobData::resultFileHttpUrl() const {
    return "http://" + _foreman->getFqdn() + ":" + to_string(_resultsHttpPort) + "/" + _resultFileName();
}

void UberJobData::cancelAllTasks() {
    LOGS(_log, LOG_LVL_INFO, cName(__func__));
    int count = 0;
    if (_cancelled.exchange(true) == false) {
        lock_guard<mutex> lg(_ujTasksMtx);
        for (auto const& task : _ujTasks) {
            auto tsk = task.lock();
            if (tsk != nullptr) {
                tsk->cancel(false);
                ++count;
            }
        }
        LOGS(_log, LOG_LVL_INFO, cName(__func__) << " cancelled " << count << " Tasks");
    }
}

string UJTransmitCmd::cName(const char* funcN) const {
    stringstream os;
    os << "UJTransmitCmd::" << funcN << " czId=" << _czarId << " QID=" << _queryId << "_ujId=" << _uberJobId;
    return os.str();
}

void UJTransmitCmd::action(util::CmdData* data) {
    LOGS(_log, LOG_LVL_TRACE, cName(__func__));
    // Make certain _selfPtr is reset before leaving this function.
    // If a retry is needed, duplicate() is called.
    class ResetSelf {
    public:
        ResetSelf(UJTransmitCmd* ujtCmd) : _ujtCmd(ujtCmd) {}
        ~ResetSelf() { _ujtCmd->_selfPtr.reset(); }
        UJTransmitCmd* const _ujtCmd;
    };
    ResetSelf resetSelf(this);

    _attemptCount++;
    auto ujPtr = _ujData.lock();
    if (ujPtr == nullptr || ujPtr->getCancelled()) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " UberJob was cancelled " << _attemptCount);
        return;
    }
    auto request = _ujMsg->toJson();
    string const requestStr = request.dump();
    http::Client client(_method, _url, requestStr, _headers);
    bool transmitSuccess = false;
    try {
        json const response = client.readAsJson();
        auto respMsg = protojson::ResponseMsg::createFromJson(response);
        if (respMsg->success) {
            transmitSuccess = true;
            string note = response.at("note");
            if (note.empty()) {
                LOGS(_log, LOG_LVL_INFO, response);
            }
        } else {
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << " Transmit success=0 " << response);
            respMsg->failedUpdateUberJobData(ujPtr);
            // There's no point in re-sending as the czar got the message and didn't like
            // it.
            return;
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " " << _requestContext << " failed, ex: " << ex.what());
    }
    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " resending!");
        auto sPtr = _selfPtr;
        if (_foreman != nullptr && sPtr != nullptr) {
            // Do not reset _selfPtr as re-queuing may be needed several times.
            LOGS(_log, LOG_LVL_WARN,
                 cName(__func__) << " no response for transmit, putting on failed transmit queue.");
            auto wCzInfo = _foreman->getWCzarInfoMap()->getWCzarInfo(_czarId);
            // This will check if the czar is believed to be alive and try the queue the query to be tried
            // again at a lower priority. It it thinks the czar is dead, it will throw it away.
            if (wCzInfo->checkAlive(CLOCK::now())) {
                auto wcComIssue = wCzInfo->getWorkerCzarComIssue();
                // nullptr should be impossible
                // Add this failed transmit to the list so the czar will try to
                // handle it when it gets the WorkerCzarComIssue message.
                wcComIssue->addFailedTransmit(_queryId, _uberJobId, _ujMsg);
            }
        } else {
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " _selfPtr was null, assuming job killed.");
        }
    }
}

void UJTransmitCmd::kill() {
    LOGS(_log, LOG_LVL_WARN, cName(__func__));
    auto sPtr = _selfPtr;
    _selfPtr.reset();
    if (sPtr == nullptr) {
        return;
    }
}

}  // namespace lsst::qserv::wbase
