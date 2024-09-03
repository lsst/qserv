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
#include "czar/ActiveWorker.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "http/Client.h"
#include "http/MetaModule.h"
#include "util/common.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.czar.ActiveWorker");
}  // namespace

namespace lsst::qserv::czar {

/* &&&
string WorkerContactInfo::dump() const {
    stringstream os;
    os << "workerContactInfo{"
       << "id=" << wId << " host=" << wHost << " mgHost=" << wManagementHost << " port=" << wPort << "}";
    return os.str();
}
*/

string ActiveWorker::getStateStr(State st) {
    switch (st) {
        case ALIVE:
            return string("ALIVE");
        case QUESTIONABLE:
            return string("QUESTIONABLE");
        case DEAD:
            return string("DEAD");
    }
    return string("unknown");
}

bool ActiveWorker::compareContactInfo(http::WorkerContactInfo const& wcInfo) const {
    lock_guard<mutex> lg(_aMtx);
    return _wqsData->_wInfo->isSameContactInfo(wcInfo);
}

void ActiveWorker::setWorkerContactInfo(http::WorkerContactInfo::Ptr const& wcInfo) {
    LOGS(_log, LOG_LVL_WARN, cName(__func__) << " new info=" << wcInfo->dump());
    lock_guard<mutex> lg(_aMtx);
    _wqsData->_wInfo = wcInfo;
}

void ActiveWorker::_changeStateTo(State newState, double secsSinceUpdate, string const& note) {
    auto lLvl = (newState == DEAD) ? LOG_LVL_ERROR : LOG_LVL_INFO;
    LOGS(_log, lLvl,
         note << " oldState=" << getStateStr(_state) << " newState=" << getStateStr(newState)
              << " secsSince=" << secsSinceUpdate);
    _state = newState;
}

void ActiveWorker::updateStateAndSendMessages(double timeoutAliveSecs, double timeoutDeadSecs,
                                              double maxLifetime) {
    lock_guard<mutex> lg(_aMtx);
    double secsSinceUpdate = _wqsData->_wInfo->timeSinceRegUpdateSeconds();
    // Update the last time the registry contacted this worker.
    switch (_state) {
        case ALIVE: {
            if (secsSinceUpdate > timeoutAliveSecs) {
                _changeStateTo(QUESTIONABLE, secsSinceUpdate, cName(__func__));
                // &&& Anything else that should be done here?
            }
            break;
        }
        case QUESTIONABLE: {
            if (secsSinceUpdate < timeoutAliveSecs) {
                _changeStateTo(ALIVE, secsSinceUpdate, cName(__func__));
            }
            if (secsSinceUpdate > timeoutDeadSecs) {
                _changeStateTo(DEAD, secsSinceUpdate, cName(__func__));
                // &&& TODO:UJ all uberjobs for this worker need to die.
            }
            break;
        }
        case DEAD: {
            LOGS(_log, LOG_LVL_ERROR, "&&& NEED CODE");
            if (secsSinceUpdate < timeoutAliveSecs) {
                _changeStateTo(ALIVE, secsSinceUpdate, cName(__func__));
            } else {
                // Don't waste time on this worker until the registry has heard from it.
                return;
            }
            break;
        }
    }

    shared_ptr<json> jsWorkerReqPtr;
    {
        lock_guard<mutex> mapLg(_wqsData->_mapMtx);
        // Check how many messages are currently being sent to the worker, if at the limit, return
        if (_wqsData->_qIdDoneKeepFiles.empty() && _wqsData->_qIdDoneDeleteFiles.empty() &&
            _wqsData->_qIdDeadUberJobs.empty()) {
            return;
        }
        int tCount = _conThreadCount;
        if (tCount > _maxConThreadCount) {
            LOGS(_log, LOG_LVL_DEBUG,
                 cName(__func__) << " not sending message since at max threads " << tCount);
            return;
        }

        // Go through the _qIdDoneKeepFiles, _qIdDoneDeleteFiles, and _qIdDeadUberJobs lists to build a
        // message to send to the worker.
        jsWorkerReqPtr = _wqsData->serializeJson(maxLifetime);
    }

    // &&& Maybe only send the status message if the lists are not empty ???
    // Start a thread to send the message. (Maybe these should go on the qdisppool? &&&)
    // put this in a different function and start the thread.&&&;
    _sendStatusMsg(jsWorkerReqPtr);
}

void ActiveWorker::_sendStatusMsg(std::shared_ptr<nlohmann::json> const& jsWorkerReqPtr) {
    auto& jsWorkerReq = *jsWorkerReqPtr;
    auto const method = http::Method::POST;
    auto const& wInf = _wqsData->_wInfo;
    string const url = "http://" + wInf->wHost + ":" + to_string(wInf->wPort) + "/querystatus";
    vector<string> const headers = {"Content-Type: application/json"};
    auto const& czarConfig = cconfig::CzarConfig::instance();

    LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " REQ " << jsWorkerReq);
    string const requestContext = "Czar: '" + http::method2string(method) + "' stat request to '" + url + "'";
    LOGS(_log, LOG_LVL_TRACE,
         cName(__func__) << " czarPost url=" << url << " request=" << jsWorkerReq.dump()
                         << " headers=" << headers[0]);
    http::Client client(method, url, jsWorkerReq.dump(), headers);
    bool transmitSuccess = false;
    string exceptionWhat;
    try {
        json const response = client.readAsJson();
        if (0 != response.at("success").get<int>()) {
            transmitSuccess = _wqsData->handleResponseJson(response);
        } else {
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << " response success=0");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, requestContext + " failed, ex: " + ex.what());
        exceptionWhat = ex.what();
    }
    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " transmit failure");
    }
}

void ActiveWorker::addToDoneDeleteFiles(QueryId qId) { _wqsData->addToDoneDeleteFiles(qId); }

void ActiveWorker::addToDoneKeepFiles(QueryId qId) { _wqsData->addToDoneKeepFiles(qId); }

void ActiveWorker::removeDeadUberJobsFor(QueryId qId) { _wqsData->removeDeadUberJobsFor(qId); }

string ActiveWorker::dump() const {
    lock_guard<mutex> lg(_aMtx);
    return _dump();
}

string ActiveWorker::_dump() const {
    stringstream os;
    os << "ActiveWorker " << (_wqsData->dump());
    return os.str();
}

void ActiveWorkerMap::updateMap(http::WorkerContactInfo::WCMap const& wcMap,
                                http::CzarContactInfo::Ptr const& czInfo,
                                std::string const& replicationInstanceId,
                                std::string const& replicationAuthKey) {
    // Go through wcMap, update existing entries in _awMap, create new entries for those that don't exist,
    lock_guard<mutex> awLg(_awMapMtx);
    for (auto const& [wcKey, wcVal] : wcMap) {
        auto iter = _awMap.find(wcKey);
        if (iter == _awMap.end()) {
            auto newAW = ActiveWorker::create(wcVal, czInfo, replicationInstanceId, replicationAuthKey);
            _awMap[wcKey] = newAW;
            if (_czarCancelAfterRestart) {
                newAW->setCzarCancelAfterRestart(_czarCancelAfterRestartCzId, _czarCancelAfterRestartQId);
            }
        } else {
            auto aWorker = iter->second;
            if (!aWorker->compareContactInfo(*wcVal)) {
                // This should not happen, but try to handle it gracefully if it does.
                LOGS(_log, LOG_LVL_WARN,
                     cName(__func__) << " worker contact info changed for " << wcKey
                                     << " new=" << wcVal->dump() << " old=" << aWorker->dump());
                aWorker->setWorkerContactInfo(wcVal);
            }
        }
    }
}

/* &&&
void ActiveWorkerMap::pruneMap() {
    lock_guard<mutex> awLg(_awMapMtx);
    for (auto iter = _awMap.begin(); iter != _awMap.end();) {
        auto aWorker = iter->second;
        if (aWorker->getWInfo()->timeSinceTouchSeconds() > _maxDeadTimeSeconds) {
            iter = _awMap.erase(iter);
        } else {
            ++iter;
        }
    }
}
*/

void ActiveWorkerMap::setCzarCancelAfterRestart(CzarIdType czId, QueryId lastQId) {
    _czarCancelAfterRestart = true;
    _czarCancelAfterRestartCzId = czId;
    _czarCancelAfterRestartQId = lastQId;
}

void ActiveWorkerMap::sendActiveWorkersMessages() {
    // Send messages to each active worker as needed
    lock_guard<mutex> lck(_awMapMtx);
    for (auto&& [wName, awPtr] : _awMap) {
        awPtr->updateStateAndSendMessages(_timeoutAliveSecs, _timeoutDeadSecs, _maxLifetime);
    }
}

/// &&& doc
void ActiveWorkerMap::addToDoneDeleteFiles(QueryId qId) {
    lock_guard<mutex> lck(_awMapMtx);
    for (auto const& [wName, awPtr] : _awMap) {
        awPtr->addToDoneDeleteFiles(qId);
        awPtr->removeDeadUberJobsFor(qId);
    }
}

/// &&& doc
void ActiveWorkerMap::addToDoneKeepFiles(QueryId qId) {
    lock_guard<mutex> lck(_awMapMtx);
    for (auto const& [wName, awPtr] : _awMap) {
        awPtr->addToDoneKeepFiles(qId);
        awPtr->removeDeadUberJobsFor(qId);
    }
}

/* &&&
/// &&& doc
void ActiveWorkerMap::removeDeadUberJobsFor(QueryId qId) {
    lock_guard<mutex> lck(_awMapMtx);
    for (auto const& [wName, awPtr] : _awMap) {
        awPtr->removeDeadUberJobsFor(qId);
    }
}
*/

}  // namespace lsst::qserv::czar
