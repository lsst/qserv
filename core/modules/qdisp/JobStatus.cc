// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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

 /**
  * @file
  *
  * @brief JobStatus implementation
  *
  * Store information message issused by a job
  * running a chunkQuery on a xrootd ressource
  *
  * @author Fabrice Jammes, IN2P3
  */

// Class header
#include "qdisp/JobStatus.h"

// System headers
#include <ctime>
#include <iostream>

// LSST headers
#include "lsst/log/Log.h"

namespace lsst {
namespace qserv {
namespace qdisp {

namespace {


LOG_LOGGER getLogger() {
    static LOG_LOGGER logger = LOG_GET("lsst.qserv.qdisp.JobStatus");
    return logger;
}

}

// Static fields
std::string const JobStatus::_empty;

JobStatus::Info::Info(ResourceUnit const& resourceUnit_)
    : resourceUnit(resourceUnit_),
      state(UNKNOWN),
      stateCode(0) {
    stateTime = ::time(NULL);
}

void JobStatus::updateInfo(JobStatus::State s, int code /* =0 */, std::string const& desc /* =_empty */) {
    std::lock_guard<std::mutex> lock(_mutex);

    LOGF(getLogger(), LOG_LVL_TRACE, "Updating %1% state to: %2%" % (void*)this % s);
    _info.stateTime = ::time(NULL);
    _info.state = s;
    _info.stateCode = code;
    _info.stateDesc = desc;
}

std::ostream& operator<<(std::ostream& os, JobStatus::State const& state) {
    switch(state)
    {
    case JobStatus::UNKNOWN:
        os << "Unknown";
        break;
    case JobStatus::PROVISION:
        os << "Accessing resource";
        break;
    case JobStatus::PROVISION_NACK:
        os << "Error accessing resource (delayed)";
        break;
    case JobStatus::REQUEST:
        os << "Sending request to resource";
        break;
    case JobStatus::REQUEST_ERROR:
        os << "Error sending request";
        break;
    case JobStatus::RESPONSE_READY:
        os << "Response ready";
        break;
    case JobStatus::RESPONSE_ERROR:
        os << "Response error";
        break;
    case JobStatus::RESPONSE_DATA:
        os << "Retrieving response data";
        break;
    case JobStatus::RESPONSE_DATA_ERROR:
        os << "Error retrieving response data";
        break;
    case JobStatus::RESPONSE_DATA_ERROR_OK:
        os << "Error retrieving response, session is OK";
        break;
    case JobStatus::RESPONSE_DATA_ERROR_CORRUPT:
        os << "Error retrieving response session is corrupt";
        break;
    case JobStatus::RESPONSE_DATA_NACK:
        os << "Error in response data";
        break;
    case JobStatus::RESPONSE_DONE:
        os << "Finished retrieving result";
        break;
    case JobStatus::RESULT_ERROR:
        os << "Error in worker result data";
        break;
    case JobStatus::MERGE_OK:
        os << "Merge complete";
        break;
    case JobStatus::MERGE_ERROR:
        os << "Error merging result";
        break;
    case JobStatus::COMPLETE:
        os << "Complete (success)";
        break;
    default:
        os << "State error (unrecognized)";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, JobStatus const& jobstatus) {
    return os << jobstatus.getInfo();
}

std::ostream& operator<<(std::ostream& os, JobStatus::Info const& info) {
    // At least 26 bytes, according to "man ctime", but might be too small.
    const int date_buf_len=sizeof "2011-10-08T07:07:09+0200";
    char date_buf[date_buf_len];
    struct tm tmp_tm;
    ::tzset();
    // localtime_r() is thread-safe
    ::strftime(date_buf, date_buf_len,
               "%FT%T%z",
               ::localtime_r(&info.stateTime, &tmp_tm));

    os << info.resourceUnit << ": "
       << date_buf << ", "
       << info.state << ", "
       << info.stateCode << ", "
       << info.stateDesc;
    return os;
}

}}} // namespace lsst::qserv::qdisp
