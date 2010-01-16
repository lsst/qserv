// -*- LSST-C++ -*-

// A Driver program to replay qserv xrootd query transactions.

#include <deque>
#include <fstream>
#include <iostream>
#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/thread.h"
#include "boost/thread.hpp"
#include "boost/make_shared.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

using boost::make_shared;

namespace qMaster = lsst::qserv::master;


int main(int const argc, char const* argv[]) {
    qMaster::Manager m;
    std::cout << "Setting up file\n";
    m.setupFile("xrdTransaction.trace");
    std::cout << "Running\n";
    m.run();
    
    return 0;
}//
