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
#include "protojson/ResponseMsg.h"

#include <stdexcept>

// Qserv headers
#include "http/RequestBodyJSON.h"
#include "protojson/PwHideJson.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.ResponseMsg");
}  // namespace

namespace lsst::qserv::protojson {

ResponseMsg::ResponseMsg(bool success_, string const& errorType_, string const& note_)
        : success(success_), errorType(errorType_), note(note_) {}

json ResponseMsg::toJson() const {
    json js;
    int su = success ? 1 : 0;
    js["success"] = su;
    js["errortype"] = errorType;
    js["note"] = note;
    return js;
}

ResponseMsg::Ptr ResponseMsg::createFromJson(nlohmann::json const& jsRespMsg) {
    auto success_ = (0 != http::RequestBodyJSON::required<int>(jsRespMsg, "success"));
    auto errorType_ = http::RequestBodyJSON::required<string>(jsRespMsg, "errortype");
    auto note_ = http::RequestBodyJSON::required<string>(jsRespMsg, "note");
    return create(success_, errorType_, note_);
}

bool ResponseMsg::equal(ResponseMsg const& other) const {
    return (success == other.success) && (errorType == other.errorType) && (note == other.note);
}

string ResponseMsg::dump() const {
    ostringstream os;
    dumpOs(os);
    return os.str();
}

ostream& ResponseMsg::dumpOs(ostream& os) const {
    os << "protojson::ResponseMsg success=" << success << " errorType=" << errorType << " note=" << note;
    return os;
}

ostream& operator<<(ostream& os, ResponseMsg const& cmd) {
    cmd.dumpOs(os);
    return os;
}

ExecutiveRespMsg::ExecutiveRespMsg(bool success_, bool dataObsolete_, QueryId qId_, UberJobId ujId_,
                                   CzarId czId_, std::string const& errorType_, std::string const& note_)
        : ResponseMsg(success_, errorType_, note_),
          dataObsolete(dataObsolete_),
          qId(qId_),
          ujId(ujId_),
          czId(czId_) {}

ExecutiveRespMsg::Ptr ExecutiveRespMsg::createFromJson(nlohmann::json const& respJson) {
    auto basePtr = ResponseMsg::createFromJson(respJson);
    auto success_ = basePtr->success;
    auto errorType_ = basePtr->errorType;
    auto note_ = basePtr->note;

    auto dataObsolete_ = http::RequestBodyJSON::required<bool>(respJson, "dataObsolete");
    auto qId_ = http::RequestBodyJSON::required<QueryId>(respJson, "qId");
    auto ujId_ = http::RequestBodyJSON::required<UberJobId>(respJson, "ujId");
    auto czId_ = http::RequestBodyJSON::required<CzarId>(respJson, "czId");

    return ExecutiveRespMsg::create(success_, dataObsolete_, qId_, ujId_, czId_, errorType_, note_);
}

json ExecutiveRespMsg::toJson() const {
    json js = ResponseMsg::toJson();
    js["dataObsolete"] = dataObsolete;
    js["qId"] = qId;
    js["ujId"] = ujId;
    js["czId"] = czId;
    return js;
}

std::ostream& ExecutiveRespMsg::dumpOs(std::ostream& os) const {
    ResponseMsg::dumpOs(os);
    os << "(ExecutiveRespMsg ";
    os << " qId=" << qId;
    os << " ujId=" << ujId;
    os << " czId=" << czId;
    os << " dataObsolete=" << dataObsolete;
    os << ")";
    return os;
}

WorkerCzarComRespMsg::Ptr WorkerCzarComRespMsg::createFromJson(nlohmann::json const& inJson) {
    auto basePtr = ResponseMsg::createFromJson(inJson);
    auto success_ = basePtr->success;
    auto errorType_ = basePtr->errorType;
    auto note_ = basePtr->note;

    auto thoughtCzarWasDeadTime_ =
            http::RequestBodyJSON::required<uint64_t>(inJson, "thoughtCzarWasDeadTime");
    auto execRespMsgs_ = json::array();
    if (inJson.contains("execRespMsgs")) {
        execRespMsgs_ = inJson["execRespMsgs"];
    }
    vector<ExecutiveRespMsg::Ptr> execRMsgs;
    for (auto const& jsExecRespMsg : execRespMsgs_) {
        try {
            auto execRespMsg = ExecutiveRespMsg::createFromJson(jsExecRespMsg);
            execRMsgs.emplace_back(execRespMsg);
        } catch (std::invalid_argument const& ex) {
            // Can anything be done beyond logging the error?
            // The worker is probably going to send this until the qId/ujId is killed.
            // This error message should never show up, but good to know if it happens.
            LOGS(_log, LOG_LVL_WARN,
                 "WorkerCzarComRespMsg::createFromJson failed to read execRespMsg:"
                         << protojson::pwHide(jsExecRespMsg) << " exception: " << ex.what());
        }
    }
    auto wccRespMsg = WorkerCzarComRespMsg::create(success_, thoughtCzarWasDeadTime_, errorType_, note_);
    wccRespMsg->execRespMsgs = execRMsgs;
    return wccRespMsg;
}

json WorkerCzarComRespMsg::toJson() const {
    json js = ResponseMsg::toJson();
    js["thoughtCzarWasDeadTime"] = thoughtCzarWasDeadTime;

    json jsExecRespMsgs = json::array();
    for (auto const& erMsg : execRespMsgs) {
        jsExecRespMsgs.emplace_back(erMsg->toJson());
    }
    js["execRespMsgs"] = jsExecRespMsgs;
    return js;
}

std::ostream& WorkerCzarComRespMsg::dumpOs(std::ostream& os) const {
    ResponseMsg::dumpOs(os);
    os << "(WorkerCzarComRespMsg czarDeadTime=" << thoughtCzarWasDeadTime;
    os << " execRespMsgs(";
    for (auto const& msg : execRespMsgs) {
        os << "(";
        msg->dumpOs(os);
        os << ")";
    }
    os << "))";
    return os;
}

}  // namespace lsst::qserv::protojson
