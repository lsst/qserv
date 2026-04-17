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
#include "wcontrol/WCzarInfoMap.h"

#include <cstdio>
#include <float.h>
#include <set>

// Third party headers
#include "nlohmann/json.hpp"

// qserv headers
#include "http/Client.h"
#include "protojson/PwHideJson.h"
#include "protojson/ResponseMsg.h"
#include "protojson/WorkerCzarComIssue.h"
#include "protojson/WorkerQueryStatusData.h"
#include "util/Bug.h"
#include "util/Histogram.h"
#include "wbase/FileChannelShared.h"
#include "wbase/UberJobData.h"
#include "wconfig/WorkerConfig.h"
#include "wcontrol/Foreman.h"
#include "wpublish/QueriesAndChunks.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

using namespace std::chrono_literals;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.WCzarInfoMap");
}

namespace lsst::qserv::wcontrol {

WCzarInfo::WCzarInfo(CzarId czarId_)
        : czarId(czarId_),
          _workerCzarComIssue(protojson::WorkerCzarComIssue::create(
                  protojson::AuthContext(wconfig::WorkerConfig::instance()->replicationInstanceId(),
                                         wconfig::WorkerConfig::instance()->replicationAuthKey()))) {}

void WCzarInfo::czarMsgReceived(TIMEPOINT tm) {
    unique_lock<mutex> uniLock(_wciMtx);
    _lastTouch = tm;
    if (_alive.exchange(true) == false) {
        uniLock.unlock();
        auto msSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(tm.time_since_epoch());
        uint64_t msDeadNowAliveTime = msSinceEpoch.count();
        LOGS(_log, LOG_LVL_WARN, cName(__func__) << " was dead and is now alive ms=" << msDeadNowAliveTime);
        _workerCzarComIssue->setThoughtCzarWasDeadTime(msDeadNowAliveTime);
    }
}

void WCzarInfo::sendWorkerCzarComIssueIfNeeded(protojson::WorkerContactInfo::Ptr const& wInfo_,
                                               protojson::CzarContactInfo::Ptr const& czInfo_) {
    unique_lock<mutex> uniLock(_wciMtx);
    if (_workerCzarComIssue->needToSend()) {
        // Having more than one of this message being sent at one time
        // could cause race issues and it would be a problem if it was
        // stuck in a queue, so it gets its own thread.
        if (_msgThreadRunning.exchange(true) == true) {
            LOGS(_log, LOG_LVL_INFO, cName(__func__) << " message thread already running");
            return;
        }
        _workerCzarComIssue->setContactInfo(wInfo_, czInfo_);
        auto selfPtr = weak_from_this();
        auto thrdFunc = [selfPtr]() {
            auto sPtr = selfPtr.lock();
            if (sPtr == nullptr) {
                LOGS(_log, LOG_LVL_WARN, "WCzarInfo::sendWorkerCzarComIssueIfNeeded thrdFunc sPtr was null");
                return;
            }
            sPtr->_sendMessage();
        };

        thread thrd(thrdFunc);
        thrd.detach();
    }
}

void WCzarInfo::_sendMessage() {
    // Make certain _msgThreadRunning is set to false when this function ends.
    class ClearMsgThreadRunning {
    public:
        ClearMsgThreadRunning(WCzarInfo* wcInfo) : _wcInfo(wcInfo) {}
        ~ClearMsgThreadRunning() { _wcInfo->_msgThreadRunning = false; }
        WCzarInfo* const _wcInfo;
    };
    ClearMsgThreadRunning clearMsgThreadRunning(this);

    auto const method = http::Method::POST;

    unique_lock<mutex> uniLock(_wciMtx);
    // If thoughtCzarWasDead is set now, it needs to be cleared on successful reception from czar.
    auto czInfo = _workerCzarComIssue->getCzarInfo();
    if (czInfo == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " czar info was null");
        return;
    }
    vector<string> const headers = {"Content-Type: application/json"};
    string const url =
            "http://" + czInfo->czHostName + ":" + to_string(czInfo->czPort) + "/workerczarcomissue";
    auto jsReq = _workerCzarComIssue->toJson();
    uniLock.unlock();  // Must unlock before communication

    // Send the request to the czar to be handled by
    auto requestStr = jsReq.dump();
    http::Client client(method, url, requestStr, headers);
    bool transmitSuccess = false;

    size_t cleanupCount = 0;
    vector<protojson::UberJobIdentType> ujDataObsoleteList;
    vector<protojson::UberJobIdentType> ujIdNotFoundErrorList;
    try {
        LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " read start");
        nlohmann::json const response = client.readAsJson();
        LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " read end");
        auto respMsg = protojson::WorkerCzarComRespMsg::createFromJson(response);

        // `response` json was created by WorkerCzarComRespMsg::toJson on the czar.
        // The `response` from the czar needs to be used to remove the handled entries
        // from the `failedTransmits` map and to determine if any result files are obsolete
        // or if there were any parse errors.
        uniLock.lock();  // re-lock _wciMtx to protect members.
        if (respMsg->success) {
            transmitSuccess = true;
            /// Read the value sent back by the czar. If it is greater than or equal
            /// czDeadTime, then set dead time to zero.
            auto localDeadTime = _workerCzarComIssue->getThoughtCzarWasDeadTime();
            if (localDeadTime != 0) {
                auto respDeadTime = respMsg->thoughtCzarWasDeadTime;
                bool cleared = false;
                if (respDeadTime >= _workerCzarComIssue->getThoughtCzarWasDeadTime()) {
                    _workerCzarComIssue->setThoughtCzarWasDeadTime(0);
                    cleared = true;
                }
                LOGS(_log, LOG_LVL_WARN,
                     cName(__func__) << " ThoughtCzarWasDeadTime check local=" << localDeadTime
                                     << " resp=" << respDeadTime << " cleared=" << cleared);
            }
            tie(cleanupCount, ujDataObsoleteList, ujIdNotFoundErrorList) =
                    _workerCzarComIssue->clearMapEntries(response);

        } else {
            ++_czarSentFailCount;
            LOGS(_log, LOG_LVL_WARN,
                 cName(__func__) << " Transmit czarSentFailCount=" << _czarSentFailCount
                                 << " msg=" << *respMsg);
            // There's no point in re-sending as the czar got the message and didn't like
            // it.
            // TODO:  What to do here? Ignore this until its a problem? Czar failed to parse original
            //   message. Start counting and consider the czar dead when a threshold is reached?
        }
    } catch (exception const& ex) {
        ++_parseErrorCount;
        LOGS(_log, LOG_LVL_WARN,
             cName(__func__) << " " << protojson::pwHide(jsReq)
                             << " failed, parseErrorCount=" << _parseErrorCount << " ex:" << ex.what());
    }

    if (!transmitSuccess) {
        // If transmit fails, the czar will send another message eventually.
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " failed to send message");
        return;
    }

    auto foreman = Foreman::getForeman();
    if (foreman == nullptr) return;
    auto queriesAndChunks = foreman->getQueriesAndChunks();
    if (queriesAndChunks == nullptr) return;

    // Set these files as obsolete (at this point they are just deleted, but that may change).
    for (auto const& ujIdent : ujDataObsoleteList) {
        LOGS(_log, LOG_LVL_INFO,
             cName(__func__) << " marking qId=" << ujIdent.qId << "_ujId=" << ujIdent.ujId << " as obsolete");
        wbase::FileChannelShared::cleanUpResults(ujIdent.czInfo->czId, ujIdent.qId, ujIdent.ujId);
    }
    // Delete files where there were parse errors.
    for (auto const& ujIdent : ujIdNotFoundErrorList) {
        LOGS(_log, LOG_LVL_INFO,
             cName(__func__) << " deleting qId=" << ujIdent.qId << "_ujId=" << ujIdent.ujId
                             << " due to parse error");
        wbase::FileChannelShared::cleanUpResults(ujIdent.czInfo->czId, ujIdent.qId, ujIdent.ujId);
    }
}

bool WCzarInfo::checkAlive(TIMEPOINT tmMark) {
    lock_guard<mutex> lg(_wciMtx);
    if (_alive) {
        auto timeSinceContact = tmMark - _lastTouch;
        std::chrono::seconds deadTime(wconfig::WorkerConfig::instance()->getCzarDeadTimeSec());
        if (timeSinceContact >= deadTime) {
            // Contact with the czar has timed out.
            LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " czar timeout");
            _alive = false;
            // Kill all queries from this czar
            auto fMan = Foreman::getForeman();
            if (fMan != nullptr) {
                auto queriesAndChunks = fMan->getQueriesAndChunks();
                if (queriesAndChunks != nullptr) {
                    queriesAndChunks->killAllQueriesFromCzar(czarId);
                }
            }
        }
    }
    return _alive;
}

WCzarInfo::Ptr WCzarInfoMap::getWCzarInfo(CzarId czId) {
    std::lock_guard lg(_wczMapMtx);
    auto iter = _wczMap.find(czId);
    if (iter == _wczMap.end()) {
        LOGS(_log, LOG_LVL_INFO, cName(__func__) << " new czar contacted " << czId);
        auto const newCzInfo = WCzarInfo::create(czId);
        _wczMap[czId] = newCzInfo;
        return newCzInfo;
    }
    return iter->second;
}

}  // namespace lsst::qserv::wcontrol
