// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#include "qdisp/ExecStatus.h"

// System headers
#include <iostream>

namespace lsst {
namespace qserv {
namespace qdisp {
// Static fields
std::string const ExecStatus::_empty;

ExecStatus::Info::Info(ResourceUnit const& resourceUnit_)
    : resourceUnit(resourceUnit_),
      state(UNKNOWN),
      stateCode(0) {
    stateTime = ::time(NULL);
}

/// @Return a C-string describing the State
char const* ExecStatus::stateText(ExecStatus::State s) {
    switch(s) {
    case UNKNOWN: return "Unknown";
    case PROVISION: return "Accessing resource";
    case PROVISION_OK: return "Provisioned ok";
    case PROVISION_ERROR: return "Error accessing resource";
    case PROVISION_NACK: return "Error accessing resource (delayed)";
    case REQUEST: return "Sending request to resource";
    case REQUEST_ERROR: return "Error sending request";
    case RESPONSE_READY: return "Response ready";
    case RESPONSE_ERROR: return "Response error";
    case RESPONSE_DATA: return "Retrieving response data";
    case RESPONSE_DATA_ERROR: return "Error retrieving response";
    case RESPONSE_DATA_NACK: return "Error in response data";
    case RESPONSE_DONE: return "Finished retrieving result";
    case RESULT_ERROR: return "Error in result data.";
    case MERGE_OK: return "Merge complete";
    case MERGE_ERROR: return "Error merging result";
    case COMPLETE: return "Complete (success)";
    default: return "State error (unrecognized)";
    }
}

std::ostream& operator<<(std::ostream& os, ExecStatus const& es) {
    ExecStatus::Info info = es.getInfo();
    return os << info;
}

std::ostream& operator<<(std::ostream& os, ExecStatus::Info const& info) {
    // At least 26 byes, according to "man ctime", but might be too small.
    const int BLEN=64;
    char buffer[BLEN];
    struct tm mytm;
    char const timefmt[] = "%Y%m%d-%H:%M:%S";
    int tsLen = ::strftime(buffer, BLEN, timefmt,
                           ::localtime_r(&info.stateTime, &mytm));
    std::string ts(buffer, tsLen);

    os << info.resourceUnit << ": " << ts << ", "
       << ExecStatus::stateText(info.state)
       << ", " << info.stateCode << ", " << info.stateDesc;
    return os;
}

}}} // namespace lsst::qserv::qdisp
