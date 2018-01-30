// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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
  * running a chunkQuery on an SSI ressource
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

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.JobStatus");
}

namespace lsst {
namespace qserv {
namespace qdisp {

JobStatus::Info::Info()
    : state(UNKNOWN), stateCode(0) {
    stateTime = ::time(NULL);
}

void JobStatus::updateInfo(std::string const& idMsg, JobStatus::State s, int code, std::string const& desc) {
    std::lock_guard<std::mutex> lock(_mutex);

    LOGS(_log, LOG_LVL_INFO, idMsg << " Updating state to: " << s << " code=" << code << " " << desc);
    _info.stateTime = ::time(NULL);
    _info.state = s;
    _info.stateCode = code;
    _info.stateDesc = desc;
}

std::ostream& operator<<(std::ostream& os, JobStatus::State const& state) {
    std::string msg("?");
    switch(state)
    {
    case JobStatus::UNKNOWN:
        msg = "UNKNOWN";
        break;
    case JobStatus::REQUEST:
        msg = "REQUEST";
        break;
    case JobStatus::RESPONSE_READY:
        msg = "RESPONSE_READY";
        break;
    case JobStatus::RESPONSE_ERROR:
        msg = "RESPONSE_ERROR";
        break;
    case JobStatus::RESPONSE_DATA:
        msg = "RESPONSE_DATA";
        break;
    case JobStatus::RESPONSE_DATA_NACK:
        msg = "RESPONSE_DATA_NACK";
        break;
    case JobStatus::RESPONSE_DONE:
        msg = "RESPONSE_DONE";
        break;
    case JobStatus::RESULT_ERROR:
        msg = "RESULT_ERROR";
        break;
    case JobStatus::MERGE_ERROR:
        msg = "MERGE_ERROR";
        break;
    case JobStatus::COMPLETE:
        msg = "COMPLETE (success)";
        break;
    case JobStatus::CANCEL:
        msg = "CANCEL";
        break;
    default:
        msg = "(unrecognized) state=" + std::to_string((int)state);
    }
    os << msg;
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

    os << ": " << date_buf << ", " << info.state << ", " << info.stateCode << ", " << info.stateDesc;
    return os;
}

}}} // namespace lsst::qserv::qdisp
