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
    dump(os);
    return os.str();
}

ostream& ResponseMsg::dump(ostream& os) const {
    os << "protojson::ResponseMsg success=" << success << " errorType=" << errorType << " note=" << note;
    return os;
}

ostream& operator<<(ostream& os, ResponseMsg const& cmd) {
    cmd.dump(os);
    return os;
}

}  // namespace lsst::qserv::protojson
