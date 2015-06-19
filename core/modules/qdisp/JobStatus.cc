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

#include "qdisp/JobStatus.h"

// System headers
#include <iostream>

namespace lsst {
namespace qserv {
namespace qdisp {
// Static fields
std::string const JobStatus::_empty;

JobStatus::Info::Info(ResourceUnit const& resourceUnit_)
    : resourceUnit(resourceUnit_),
      state(UNKNOWN),
      stateCode(0) {
    stateTime = ::time(NULL);
}

std::ostream& operator<<(std::ostream& os, JobStatus::State const& state) {

	std::map<JobStatus::State, std::string> state_message;

	state_message[JobStatus::UNKNOWN] = "Unknown";
	state_message[JobStatus::PROVISION] = "Accessing resource";
	state_message[JobStatus::PROVISION_NACK] =  "Error accessing resource (delayed)";
	state_message[JobStatus::REQUEST] = "Sending request to resource";
	state_message[JobStatus::REQUEST_ERROR] = "Error sending request";
	state_message[JobStatus::RESPONSE_READY] = "Response ready";
	state_message[JobStatus::RESPONSE_ERROR] = "Response error";
	state_message[JobStatus::RESPONSE_DATA] = "Retrieving response data";
	state_message[JobStatus::RESPONSE_DATA_ERROR] = "Error retrieving response data";
	state_message[JobStatus::RESPONSE_DATA_ERROR_OK] = "Error retrieving response, session is OK";
	state_message[JobStatus::RESPONSE_DATA_ERROR_CORRUPT] =  "Error retrieving response session is corrupt";
	state_message[JobStatus::RESPONSE_DATA_NACK] = "Error in response data";
	state_message[JobStatus::RESPONSE_DONE] = "Finished retrieving result";
	state_message[JobStatus::RESULT_ERROR] = "Error in result data.";
	state_message[JobStatus::MERGE_OK] = "Merge complete";
	state_message[JobStatus::MERGE_ERROR] = "Error merging result";
	state_message[JobStatus::COMPLETE] = "Complete (success)";

	auto it = state_message.find(state);
	if (it != state_message.end()) {
		os << it->second;
	} else
		os << "State error (unrecognized)";
	return os;
}

std::ostream& operator<<(std::ostream& os, JobStatus const& es) {
    JobStatus::Info info = es.getInfo();
    return os << info;
}

std::ostream& operator<<(std::ostream& os, JobStatus::Info const& info) {
    // At least 26 byes, according to "man ctime", but might be too small.
    const int BLEN=64;
    char buffer[BLEN];
    struct tm mytm;
    char const timefmt[] = "%Y%m%d-%H:%M:%S";
    int tsLen = ::strftime(buffer, BLEN, timefmt,
                           ::localtime_r(&info.stateTime, &mytm));
    std::string ts(buffer, tsLen);

    os << info.resourceUnit << ": " << ts << ", "
       << info.state
       << ", " << info.stateCode << ", " << info.stateDesc;
    return os;
}

}}} // namespace lsst::qserv::qdisp
