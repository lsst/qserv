// -*- LSST-C++ -*-

/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 

// A Driver program to replay qserv xrootd query transactions.

#include <deque>
#include <fstream>
#include <iostream>
#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/thread.h"
#include "lsst/qserv/master/dispatcher.h"
#include "boost/thread.hpp"
#include "boost/make_shared.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

using boost::make_shared;

namespace qMaster = lsst::qserv::master;


void alternative() {
    std::map<std::string,std::string> dumbMap;
    int stopCount = 0;
    char* envStopCount = ::getenv("QS_STOPCOUNT");
    if(envStopCount != (char*)0) { stopCount = atoi(envStopCount); }

    // alternate harness.
    using qMaster::QueryState;
    qMaster::initDispatcher();
    int session = qMaster::newSession(dumbMap);
    qMaster::TransactionSpec::Reader r("xrdTransaction.trace");
    int i = 0;
    for(qMaster::TransactionSpec s = r.getSpec(); !s.isNull();
        s = r.getSpec()) {
        submitQuery(session, s);
        ++i;
        if(stopCount && (i > stopCount)) break; // Stop early for debugging.
    }
    QueryState s = qMaster::joinSession(session);
    qMaster::discardSession(session);
}

void original() {
    qMaster::Manager m;
    std::cout << "Setting up file\n";
    m.setupFile("xrdTransaction.trace");
    std::cout << "Running\n";
    m.run();
}

int main(int const argc, char const* argv[]) {
    //original();
    alternative();
    return 0;
}//
