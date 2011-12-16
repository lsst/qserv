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
#define BOOST_TEST_MODULE Protocol_1
#include "boost/test/included/unit_test.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "boost/scoped_ptr.hpp"
#include "worker.pb.h"

namespace test = boost::test_tools;

struct ProtocolFixture {
    ProtocolFixture(void) {}
    ~ProtocolFixture(void) { };
};

BOOST_FIXTURE_TEST_SUITE(ProtocolTestSuite, ProtocolFixture)

BOOST_AUTO_TEST_CASE(Sanity) {
    std::stringstream ss;
    std::ostream& output(ss);
    Qserv::Task* t;
    t = new Qserv::Task();
    t->set_session(123456);
    t->set_chunkid(20);
    for(int i=0; i < 3; ++i) {
        Qserv::Task::Fragment* f = t->add_fragment();
        f->set_query("Hello, this is a query.");
        f->add_subchunk(100+i); 
    }
    t->SerializeToOstream(&output);
}

#if 0
int main(int,char**) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    using std::ios;
    std::fstream outFile("testtaskproto", ios::out | ios::trunc | ios::binary);
    makeTest(outFile);
    return 0;
}
#endif
BOOST_AUTO_TEST_SUITE_END()
