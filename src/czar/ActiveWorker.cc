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
    case ALIVE: return string("ALIVE");
    case QUESTIONABLE: return string("QUESTIONABLE");
    case DEAD: return string("DEAD");
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
    LOGS(_log, lLvl, note << " oldState=" << getStateStr(_state) << " newState=" << getStateStr(newState) << " secsSince=" << secsSinceUpdate);
    _state = newState;
}

void ActiveWorker::updateStateAndSendMessages(double timeoutAliveSecs, double timeoutDeadSecs, double maxLifetime) {
    // &&& function too long
    lock_guard<mutex> lg(_aMtx);
    double secsSinceUpdate = _wqsData->_wInfo->timeSinceRegUpdateSeconds();
    // Update the last time the registry contacted this worker.
    switch (_state) {
    case ALIVE: {
        if (secsSinceUpdate > timeoutAliveSecs) {
            _changeStateTo(QUESTIONABLE, secsSinceUpdate, cName(__func__));
            // Anything that should be done here?
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
    case DEAD:  {
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

    // Check how many messages are currently being sent to the worker, if at the limit, return
    if (_wqsData->_qIdDoneKeepFiles.empty() && _wqsData->_qIdDoneDeleteFiles.empty() && _wqsData->_qIdDeadUberJobs.empty()) {
        return;
    }
    int tCount = _conThreadCount;
    if (tCount > _maxConThreadCount) {
        LOGS(_log, LOG_LVL_DEBUG, cName(__func__) << " not sending message since at max threads " << tCount);
        return;
    }

    // Go through the _qIdDoneKeepFiles, _qIdDoneDeleteFiles, and _qIdDeadUberJobs lists to build a
    // message to send to the worker.
#if 0 // &&&
    auto now = CLOCK::now();
    auto const czarConfig = cconfig::CzarConfig::instance();

    shared_ptr<json> jsWorkerReqPtr = make_shared<json>();
    json& jsWorkerR = *jsWorkerReqPtr;
    jsWorkerR["version"] = http::MetaModule::version;
    jsWorkerR["instance_id"] = czarConfig->replicationInstanceId();
    jsWorkerR["auth_key"] = czarConfig->replicationAuthKey();
    jsWorkerR["worker"] = _wInfo->wId;
    jsWorkerR["qiddonekeepfiles"] = json::array();
    jsWorkerR["qiddonedeletefiles"] = json::array();
    jsWorkerR["qiddeaduberjobs"] = json::array();
    jsWorkerR["czar"] = json::object();
    auto& jsWCzar = jsWorkerR["czar"];
    jsWCzar["name"] = czarConfig->name();
    jsWCzar["id"]= czarConfig->id();
    jsWCzar["management-port"] = czarConfig->replicationHttpPort();
    jsWCzar["management-host-name"] = util::get_current_host_fqdn();


    {
        auto& jsDoneKeep = jsWorkerR["qiddonekeepfiles"];
        auto iterDoneKeep = _qIdDoneKeepFiles.begin();
        while (iterDoneKeep != _qIdDoneKeepFiles.end()) {
            auto qId = iterDoneKeep->first;
            jsDoneKeep.push_back(qId);
            auto tmStamp = iterDoneKeep->second;
            double ageSecs = std::chrono::duration<double>(now - tmStamp).count();
            if (ageSecs > maxLifetime) {
                iterDoneKeep = _qIdDoneKeepFiles.erase(iterDoneKeep);
            } else {
                ++iterDoneKeep;
            }
        }
    }
    {
        auto& jsDoneDelete = jsWorkerR["qiddonedeletefiles"];
        auto iterDoneDelete = _qIdDoneDeleteFiles.begin();
        while (iterDoneDelete != _qIdDoneDeleteFiles.end()) {
            auto qId = iterDoneDelete->first;
            jsDoneDelete.push_back(qId);
            auto tmStamp = iterDoneDelete->second;
            double ageSecs = std::chrono::duration<double>(now - tmStamp).count();
            if (ageSecs > maxLifetime) {
                iterDoneDelete = _qIdDoneDeleteFiles.erase(iterDoneDelete);
            } else {
                ++iterDoneDelete;
            }
        }
    }
    {
        auto& jsDeadUj = jsWorkerR["qiddeaduberjobs"];
        auto iterDeadUjQid = _qIdDeadUberJobs.begin();
        while (iterDeadUjQid != _qIdDeadUberJobs.end()) {
            TIMEPOINT oldestTm; // default is zero
            auto qId = iterDeadUjQid->first;
            auto& ujIdMap = iterDeadUjQid->second;

            json jsQidUj = {{"qid", qId}, {"ujids", json::array()}};
            auto& jsUjIds = jsQidUj["ujids"];

            auto iterUjId = ujIdMap.begin();
            bool addedUjId = false;
            while (iterUjId != ujIdMap.end()) {
                UberJobId ujId = iterUjId->first;
                auto tmStamp = iterUjId->second;
                if (tmStamp > oldestTm) {
                    oldestTm = tmStamp;
                }

                jsUjIds.push_back(ujId);
                addedUjId = true;
                double ageSecs = std::chrono::duration<double>(now - tmStamp).count();
                if (ageSecs > maxLifetime) {
                    iterUjId = ujIdMap.erase(iterUjId);
                } else {
                    ++iterUjId;
                }
            }

            if (addedUjId) {
                jsDeadUj.push_back(jsQidUj);
            }

            if (ujIdMap.empty()
                || std::chrono::duration<double>(now - oldestTm).count() > maxLifetime) {
                iterDeadUjQid = _qIdDeadUberJobs.erase(iterDeadUjQid);
            } else {
                ++iterDeadUjQid;
            }
        }
    }
#endif // &&&

    auto jsWorkerReqPtr = _wqsData->serializeJson(timeoutAliveSecs, timeoutDeadSecs, maxLifetime);

    // Start a thread to send the message. (Maybe these should go on the qdisppool? &&&)
    // put this in a different function and start the thread.&&&;
    _sendStatusMsg(jsWorkerReqPtr);
}

#if 0 // &&&
bool ActiveWorker::_parse(nlohmann::json const& jsWorkerReq) {
    auto const czarConfig = cconfig::CzarConfig::instance();

    http::RequestBodyJSON rbWReq(jsWorkerReq);
    if (jsWorkerReq["version"] != http::MetaModule::version) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " bad version");
        return false;
    }


    http::RequestBodyJSON rbCzar(rbWReq.required<json>("czar"));
    auto czarName = rbCzar.required<string>("name");
    auto czarId = rbCzar.required<qmeta::CzarId>("id");
    auto czarPort = rbCzar.required<int>("management-port");
    auto czarHostName = rbCzar.required<string>("management-host-name");
    /* &&&
    jsWorkerReq["instance_id"] != czarConfig->replicationInstanceId();
    jsWorkerReq["auth_key"] != czarConfig->replicationAuthKey();
    jsWorkerReq["worker"] != _wInfo->wId;
    auto& jsWCzar = jsWorkerReq["czar"];
    jsWCzar["name"] != czarConfig->name();
    jsWCzar["id"] != czarConfig->id();
    jsWCzar["management-port"] != czarConfig->replicationHttpPort();
    jsWCzar["management-host-name"] != util::get_current_host_fqdn();
    */


    auto& jsQIdDoneKeepFiles = jsWorkerReq["qiddonekeepfiles"];
    for (auto const& qidKeep : jsQIdDoneKeepFiles) {

    }

    auto& jsQIdDoneDeleteFiles = jsWorkerReq["qiddonedeletefiles"];

    auto& jsQIdDeadUberJobs = jsWorkerReq["qiddeaduberjobs"];

}
#endif // &&&

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
            transmitSuccess = true;
        } else {
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << " response success=0");
        }
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN, requestContext + " failed, ex: " + ex.what());
        exceptionWhat = ex.what();
    }
    if (!transmitSuccess) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " transmit failure");
    } else {
        // parse the return statement and remove the indicated entries from the list
        //HERE &&&;
    }
}


string ActiveWorker::dump() const {
    lock_guard<mutex> lg(_aMtx);
    return _dump();
}

string ActiveWorker::_dump() const {
    stringstream os;
    os << "ActiveWorker " << (_wqsData->dump());
    return os.str();
}


void ActiveWorkerMap::updateMap(http::WorkerContactInfo::WCMap const& wcMap, http::CzarContactInfo::Ptr const& czInfo, std::string const& replicationInstanceId, std::string const& replicationAuthKey) {
    // Go through wcMap, update existing entries in _awMap, create new entries for those that don't exist,
    lock_guard<mutex> awLg(_awMapMtx);
    for (auto const& [wcKey, wcVal] : wcMap) {
        auto iter = _awMap.find(wcKey);
        if (iter == _awMap.end()) {
            auto newAW = ActiveWorker::create(wcVal, czInfo, replicationInstanceId, replicationAuthKey);
            _awMap[wcKey] = newAW;
        } else {
            auto aWorker = iter->second;
            if (!aWorker->compareContactInfo(*wcVal)) {
                // This should not happen, but try to handle it gracefully if it does.
                LOGS(_log, LOG_LVL_WARN, cName(__func__) << " worker contact info changed for " << wcKey << " new=" << wcVal->dump() << " old=" << aWorker->dump());
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

void ActiveWorkerMap::sendActiveWorkersMessages() {
    // Send messages to each active worker as needed
    lock_guard<mutex> lck(_awMapMtx);
    for(auto&& [wName, awPtr] : _awMap) {
        awPtr->updateStateAndSendMessages(_timeoutAliveSecs, _timeoutDeadSecs, _maxLifetime);
    }
}


}  // namespace lsst::qserv::czar
