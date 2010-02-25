// -*- LSST-C++ -*-

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
    int stopCount = 0;
    char* envStopCount = ::getenv("QS_STOPCOUNT");
    if(envStopCount != (char*)0) { stopCount = atoi(envStopCount); }


    // alternate harness.
    using qMaster::QueryState;
    qMaster::initDispatcher();
    int session = qMaster::newSession();
    qMaster::TransactionSpec::Reader r("xrdTransaction.trace");
    int i = 0;
    for(qMaster::TransactionSpec s = r.getSpec(); !s.isNull();
	s = r.getSpec()) {
	submitQuery(session, s);
	++i;
	if(stopCount && (i > stopCount)) break; // Stop early for debugging.
    }
    QueryState s = qMaster::joinSession(session);
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
