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
// class Executive is in charge of "executing" user query fragments on
// a qserv cluster.

#ifndef LSST_QSERV_QDISP_EXECSTATUS_H
#define LSST_QSERV_QDISP_EXECSTATUS_H

// System headers
#include <string>
#include <time.h>

// Third-party headers
#include <boost/shared_ptr.hpp>

// Local headers
#include "global/ResourceUnit.h"

namespace lsst {
namespace qserv {
namespace qdisp {

class ExecStatus {
public:
    typedef boost::shared_ptr<ExecStatus> Ptr;
    ExecStatus(ResourceUnit const& r) : resourceUnit(r) {}

    // TODO: these shouldn't be exposed, and so shouldn't be user-level error
    // codes, but maybe we can be clever and avoid an ugly remap/translation
    // with msgCode.h. 1201-1289 (inclusive) are free and MSG_FINALIZED==2000
    enum State { UNKNOWN=0,
                 PROVISION=1201,
                 PROVISION_ERROR, PROVISION_NACK,
                 PROVISION_OK, // ???
                 REQUEST, REQUEST_ERROR,
                 RESPONSE_READY, RESPONSE_ERROR,
                 RESPONSE_DATA, RESPONSE_DATA_ERROR, RESPONSE_DATA_NACK,
                 RESPONSE_DONE,
                 RESULT_ERROR,
                 MERGE_OK, // ???
                 MERGE_ERROR, COMPLETE=2000};

    inline void report(State s, int code=0, std::string const& desc=_empty) {
        stateTime = ::time(NULL);
        state = s;
        stateCode = code;
        stateDesc = desc;
    }

    static char const* stateText(State s);

    ResourceUnit resourceUnit;
    // More detailed debugging may store a vector of states, appending
    // with each invocation of report().
    State state;
    time_t stateTime;
    int stateCode;
    std::string stateDesc;
private:
    static std::string _empty;
};
std::ostream& operator<<(std::ostream& os, ExecStatus const& es);

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_EXECSTATUS_H
