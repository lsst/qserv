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
#include "protojson/WorkerCzarComIssue.h"

#include <stdexcept>

// Qserv headers
#include "http/Client.h"
#include "http/MetaModule.h"
#include "http/RequestBodyJSON.h"
#include "protojson/PwHideJson.h"
#include "protojson/ResponseMsg.h"
#include "protojson/UberJobErrorMsg.h"
#include "protojson/UberJobReadyMsg.h"
#include "util/common.h"
#include "util/TimeUtils.h"
#include "wbase/UberJobData.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.WorkerCzarComIssue");
}  // namespace

namespace lsst::qserv::protojson {

json WorkerCzarComIssue::toJson() {
    json jsCzarR;
    lock_guard _lgWciMtx(_wciMtx);
    if (_wInfo == nullptr || _czInfo == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " _wInfo or _czInfo was null");
        return jsCzarR;
    }

    jsCzarR["version"] = http::MetaModule::version;
    jsCzarR["instance_id"] = _authContext.replicationInstanceId;
    jsCzarR["auth_key"] = _authContext.replicationAuthKey;
    jsCzarR["czarinfo"] = _czInfo->toJson();
    jsCzarR["czar"] = _czInfo->czName;
    jsCzarR["workerinfo"] = _wInfo->toJson();

    jsCzarR["thoughtczarwasdead"] = _thoughtCzarWasDeadTime;

    // List of failed transmits
    jsCzarR["failedtransmits"] = json::array();
    auto& jsFts = jsCzarR["failedtransmits"];
    auto iter = _failedTransmits->begin();
    while (iter != _failedTransmits->end()) {
        auto const& key = iter->first;
        QueryId qId = key.first;
        UberJobId ujId = key.second;
        UberJobStatusMsg::Ptr ujMsg = iter->second;
        auto const resp = ujMsg->toJson();
        json jsF = json{{"qId", qId}, {"ujId", ujId}, {"failed", resp}};
        jsFts.push_back(jsF);
        ++iter;
    }

    return jsCzarR;
}

WorkerCzarComIssue::Ptr WorkerCzarComIssue::createFromJson(nlohmann::json const& jsCzarReq,
                                                           AuthContext const& authContext_) {
    string const fName("WorkerCzarComIssue::createFromJson");
    LOGS(_log, LOG_LVL_DEBUG, fName);
    try {
        if (jsCzarReq["version"] != http::MetaModule::version) {
            LOGS(_log, LOG_LVL_ERROR, fName << " bad version");
            return nullptr;
        }

        auto czInfo_ = CzarContactInfo::createFromJson(jsCzarReq["czarinfo"]);
        auto now = CLOCK::now();
        auto wInfo_ = WorkerContactInfo::createFromJsonWorker(jsCzarReq["workerinfo"], now);
        if (czInfo_ == nullptr || wInfo_ == nullptr) {
            LOGS(_log, LOG_LVL_ERROR, fName << " or worker info could not be parsed in " << jsCzarReq);
        }
        auto wccIssue = create(authContext_);
        wccIssue->setContactInfo(wInfo_, czInfo_);
        wccIssue->_thoughtCzarWasDeadTime =
                http::RequestBodyJSON::required<uint64_t>(jsCzarReq, "thoughtczarwasdead");
        json fTransmits = json::array();
        fTransmits = jsCzarReq.at("failedtransmits");
        if (!fTransmits.is_array()) {
            throw std::invalid_argument(fName + " failedtransmits is not a json::array");
        }

        // Fill in _failedTransmits with the values in fTransmits
        for (auto const& jsElem : fTransmits) {
            try {
                auto const qId = http::RequestBodyJSON::required<QueryId>(jsElem, "qId");
                auto const ujId = http::RequestBodyJSON::required<UberJobId>(jsElem, "ujId");
                json jsFt = jsElem["failed"];
                UberJobStatusMsg::Ptr ujMsg;
                bool isReadyMsg = jsFt.contains("fileUrl");
                if (isReadyMsg) {
                    ujMsg = UberJobReadyMsg::createFromJson(jsFt);
                } else {
                    ujMsg = UberJobErrorMsg::createFromJson(jsFt);
                }
                wccIssue->addFailedTransmit(qId, ujId, ujMsg);
            } catch (std::invalid_argument const& ex) {
                // skip to next element
                LOGS(_log, LOG_LVL_WARN, fName << " failed to read failedTransmit:" << jsElem);
            }
        }
        return wccIssue;
    } catch (invalid_argument const& exc) {
        LOGS(_log, LOG_LVL_ERROR, string("WorkerQueryStatusData::createJson invalid ") << exc.what());
    }
    return nullptr;
}

bool WorkerCzarComIssue::operator==(WorkerCzarComIssue const& other) const {
    if ((*_wInfo != *other._wInfo) || (*_czInfo != *other._czInfo) || (_authContext != other._authContext) ||
        (_thoughtCzarWasDeadTime != other._thoughtCzarWasDeadTime)) {
        return false;
    }

    if (_failedTransmits->size() != other._failedTransmits->size()) {
        return false;
    }
    auto iterThis = _failedTransmits->begin();
    auto iterOther = other._failedTransmits->begin();
    for (; iterThis != _failedTransmits->end() && iterOther != other._failedTransmits->end();
         ++iterThis, ++iterOther) {
        if (iterThis->first != iterOther->first) {
            return false;
        }

        auto const& ftThis = iterThis->second;
        auto const& ftOther = iterOther->second;
        if (!(ftThis->equals(*ftOther))) {
            return false;
        }
    }
    return true;
}

void WorkerCzarComIssue::addFailedTransmit(QueryId qId, UberJobId ujId,
                                           std::shared_ptr<protojson::UberJobStatusMsg> const& ujMsg) {
    lock_guard _lgWciMtx(_wciMtx);
    _addFailedTransmit(qId, ujId, ujMsg);
}

void WorkerCzarComIssue::_addFailedTransmit(QueryId qId, UberJobId ujId,
                                            std::shared_ptr<protojson::UberJobStatusMsg> const& ujMsg) {
    auto key = make_pair(qId, ujId);
    (*_failedTransmits)[key] = ujMsg;
}

json WorkerCzarComIssue::responseToJson(uint64_t msgThoughtCzarWasDeadTime,
                                        vector<protojson::ExecutiveRespMsg::Ptr> const& execRespMsgs) const {
    lock_guard _lgWciMtx(_wciMtx);
    return _responseToJson(msgThoughtCzarWasDeadTime, execRespMsgs);
}

std::shared_ptr<FailedTransmitsMap> WorkerCzarComIssue::takeFailedTransmitsMap() {
    lock_guard _lgWciMtx(_wciMtx);
    auto res = _failedTransmits;
    _failedTransmits = make_shared<FailedTransmitsMap>();
    return res;
}

tuple<size_t, vector<UberJobIdentifierType>, vector<UberJobIdentifierType>>
WorkerCzarComIssue::clearMapEntries(nlohmann::json const& response) {
    vector<UberJobIdentifierType> ujDataObsoleteList;
    vector<UberJobIdentifierType> ujParseErrorList;
    size_t count = 0;
    if (!response.contains("execRespMsgs")) {
        LOGS(_log, LOG_LVL_WARN,
             cName(__func__) << " response did not have 'execRespMsgs' " << pwHide(response));
        return {count, ujDataObsoleteList, ujParseErrorList};
    }
    auto const& jsExecRespMsgs = response["execRespMsgs"];
    if (!jsExecRespMsgs.is_array()) {
        LOGS(_log, LOG_LVL_WARN,
             cName(__func__) << " response 'execRespMsgs' is not array " << pwHide(response));
        return {count, ujDataObsoleteList, ujParseErrorList};
    }

    lock_guard _lgWciMtx(_wciMtx);
    for (auto const& elem : jsExecRespMsgs) {
        if (!(elem.contains("qId") && elem.contains("ujId"))) {
            LOGS(_log, LOG_LVL_WARN, cName(__func__) << "elem missing qId or ujId elem=" << elem);
            continue;
        }
        QueryId qId = elem["qId"];
        UberJobId ujId = elem["ujId"];

        try {
            auto execRespMsg = ExecutiveRespMsg::createFromJson(elem);
            if (execRespMsg == nullptr) {
                LOGS(_log, LOG_LVL_WARN,
                     cName(__func__) << " failed to parse execRespMsg elem=" << pwHide(elem));
                continue;
            }
            // Find the appropriate Task and update it if needed.
            if (execRespMsg->success) {
                // Nothing needs to be done if the UberJob is not obsolete. If it
                // is obsolete, it could be useful to delete the result file.
                if (execRespMsg->dataObsolete) {
                    ujDataObsoleteList.emplace_back(_czInfo, qId, ujId);
                }
            } else {
                // The czar couldn't parse this for some reason, leaving no way to get the result
                // file back to the czar. Delete the result file and return an error.
                ujParseErrorList.emplace_back(_czInfo, qId, ujId);
            }
            LOGS(_log, LOG_LVL_DEBUG,
                 cName(__func__) << " removing qId=" << qId << "_ujId=" << ujId << " from map");
            _failedTransmits->erase(make_pair(qId, ujId));  //&&& probably needs to lock _wciMtx.
            count++;
        } catch (std::invalid_argument const& ex) {
            LOGS(_log, LOG_LVL_ERROR,
                 cName(__func__) << " failed to parse execRespMsg elem=" << elem
                                 << " exception: " << ex.what());
            // This should never happen as all messages should parse.
            // There's probably nothing else that can be done at this point. Hopefully, other
            // cleanup mechanisms will be enough to prevent this from causing problems.
            continue;
        }
    }
    return {count, ujDataObsoleteList, ujParseErrorList};
}

json WorkerCzarComIssue::_responseToJson(
        uint64_t msgThoughtCzarWasDeadTime,
        vector<protojson::ExecutiveRespMsg::Ptr> const& execRespMsgs_) const {
    protojson::WorkerCzarComRespMsg wccRespMsg(true, msgThoughtCzarWasDeadTime);
    wccRespMsg.execRespMsgs = execRespMsgs_;
    return wccRespMsg.toJson();
}

string WorkerCzarComIssue::dump() const {
    lock_guard _lgWciMtx(_wciMtx);
    return _dump();
}

string WorkerCzarComIssue::_dump() const {
    stringstream os;
    os << "WorkerCzarComIssue wInfo=" << ((_wInfo == nullptr) ? "?" : _wInfo->dump());
    os << " czInfo=" << _czInfo->dump();
    os << " thoughtCzarWasDeadTime=" << _thoughtCzarWasDeadTime;
    os << " failedTransmits[";
    for (auto const& [key, ft] : *_failedTransmits) {
        os << "{qId=" << key.first << " ujId=" << key.second << "{";
        auto ujMsg = ft;
        if (ujMsg == nullptr) {
            os << " ujMsg=nullptr";
        } else {
            os << " ujMsg=" << ujMsg->dump();
        }
        os << "}}";
    }
    os << "]";
    return os.str();
}

}  // namespace lsst::qserv::protojson
