// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 *
 * @author John Gates, SLAC
 */

// System headers
#include <string>
#include <stdlib.h>
#include <thread>
#include <unistd.h>

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "qdisp/Executive.h"
#include "qdisp/XrdSsiMocks.h"

using namespace std;

namespace lsst {
namespace qserv {
namespace qdisp {

class QueryResourceDebug {
public:
    static ExecStatus& getStatus(QueryResource& qr) {
        return qr._status;
    }
    static std::string const& getPayload(QueryResource& qr) {
        return qr._payload;
    }
    static void finish(QueryResource& qr) {
        qr._finishFunc->operator ()(true);
    }
};

util::FlagNotify<bool> XrdSsiServiceMock::_go(true);
util::Sequential<int> XrdSsiServiceMock::_count(0);

/** Class to fake being a request to xrootd.
 * Fire up thread that sleeps for a bit and then indicates it was successful.
 */
void XrdSsiServiceMock::Provision(Resource *resP, unsigned short  timeOut){
    if (resP == NULL) {
        LOGF_ERROR("XrdSsiServiceMock::Provision() invoked with a null Resource pointer.");
        return;
    }
    lsst::qserv::qdisp::QueryResource *qr = dynamic_cast<lsst::qserv::qdisp::QueryResource*>(resP);
    if (qr == NULL) {
        LOGF_ERROR("XrdSsiServiceMock::Provision() unexpected resource type.");
        return;
    }
    _count.incr();

    std::thread t(&XrdSsiServiceMock::mockProvisionTest, this, qr, timeOut);
    // Thread must live past the end of this function, and the calling body
    // is not really dealing with threads, and this is for testing only.
    t.detach();
}

/** Mock class for testing Executive.
 * The payload of qr should contain the number of milliseconds this function will
 * sleep before returning.
 */
void XrdSsiServiceMock::mockProvisionTest(qdisp::QueryResource *qr,
                                          unsigned short timeOut) {
    string payload = QueryResourceDebug::getPayload(*qr);
    int millisecs = atoi(payload.c_str());
    // barrier for all threads when _go is false.
    _go.wait(true);
    LOGF_INFO("XrdSsiServiceMock::mockProvisionTest sleep begin");
    usleep(1000*millisecs);
    LOGF_INFO("XrdSsiServiceMock::mockProvisionTest sleep end");
    QueryResourceDebug::getStatus(*qr).report(ExecStatus::RESPONSE_DONE);
    QueryResourceDebug::finish(*qr); // This should call class NotifyExecutive::operator()
    // qr->ProvisionDone would normally cause qr to commit suicide, but that requires
    // a session object. Instead, take care of deletion ourselves.
    delete qr;
}

}}} // namespace
