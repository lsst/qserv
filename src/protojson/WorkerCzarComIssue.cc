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
#include "util/common.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace nlohmann;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.protojson.WorkerCzarComIssue");
}  // namespace

namespace lsst::qserv::protojson {

shared_ptr<json> WorkerCzarComIssue::serializeJson() {
    shared_ptr<json> jsCzarReqPtr = make_shared<json>();
    json& jsCzarR = *jsCzarReqPtr;
    lock_guard _lgWciMtx(_wciMtx);
    if (_wInfo == nullptr || _czInfo == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, cName(__func__) << " _wInfo or _czInfo was null");
        return jsCzarReqPtr;
    }

    jsCzarR["version"] = http::MetaModule::version;
    jsCzarR["instance_id"] = _replicationInstanceId;
    jsCzarR["auth_key"] = _replicationAuthKey;
    jsCzarR["czarinfo"] = _czInfo->serializeJson();
    jsCzarR["czar"] = _czInfo->czName;
    jsCzarR["workerinfo"] = _wInfo->serializeJson();

    jsCzarR["thoughtczarwasdead"] = _thoughtCzarWasDead;

    // TODO:UJ add list of failed transmits

    return jsCzarReqPtr;
}

WorkerCzarComIssue::Ptr WorkerCzarComIssue::createFromJson(nlohmann::json const& jsCzarReq,
                                                           std::string const& replicationInstanceId_,
                                                           std::string const& replicationAuthKey_) {
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
        auto wccIssue = create(replicationInstanceId_, replicationAuthKey_);
        wccIssue->setContactInfo(wInfo_, czInfo_);
        wccIssue->_thoughtCzarWasDead =
                http::RequestBodyJSON::required<bool>(jsCzarReq, "thoughtczarwasdead");
        return wccIssue;
    } catch (invalid_argument const& exc) {
        LOGS(_log, LOG_LVL_ERROR, string("WorkerQueryStatusData::createJson invalid ") << exc.what());
    }
    return nullptr;
}

json WorkerCzarComIssue::serializeResponseJson() {
    json jsResp = {{"success", 1}, {"errortype", "none"}, {"note", ""}};

    // TODO:UJ add lists of uberjobs that are scheduled to have files collected because of this message.
    return jsResp;
}

string WorkerCzarComIssue::dump() const {
    lock_guard _lgWciMtx(_wciMtx);
    return _dump();
}

string WorkerCzarComIssue::_dump() const {
    stringstream os;
    os << "WorkerCzarComIssue wInfo=" << ((_wInfo == nullptr) ? "?" : _wInfo->dump());
    os << " czInfo=" << _czInfo->dump();
    os << " thoughtCzarWasDead=" << _thoughtCzarWasDead;
    return os.str();
}

}  // namespace lsst::qserv::protojson
