// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 AURA/LSST.
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

// System headers
#include <cassert>
#include <unistd.h>

// Third-party headers

// Local headers
#include "css/EmptyChunks.h"


// Boost unit test header
#define BOOST_TEST_MODULE testEmptyChunks
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using lsst::qserv::css::EmptyChunks;

struct DummyFile {
    DummyFile() {
        std::ostringstream os;
        int pid = ::getpid();
        assert(pid > 0);
        os << "/tmp/testEC_" << pid;
        _path = os.str();
        _fallback = _path + "/" + "emptyChunks.txt";
        // Construction:
        // mkdir /tmp/testEC_<pid>
        // cat > /tmp/testEC_<pid>/empty_TestOne.txt
        // cat > /tmp/testEC_<pid>/empty_TestTwo.txt
        // cat > /tmp/testEC_<pid>/emptyChunks.txt
        // Destruction:
        // rm -r /tmp/testEC_<pid>
        mkDir();
        writeFile("TestOne", 1, 20);
        writeFile("TestTwo", 100, 200);
        writeFile(NULL, 1000, 1010);
    }
    ~DummyFile() {
        rmDir();
    }
    void mkDir() {
        // Slightly dangerous: invoke OS system interpreter
        // Doesn't seem worth it to be more precise for a unit test.
        assert(_path.find("/tmp/") == 0);
        std::string c = "mkdir ";
        c += _path;
        ::system(c.c_str());
    }
    void rmDir() {
        // Slightly dangerous: invoke OS system interpreter
        // Doesn't seem worth it to be more precise for a unit test.
        assert(_path.find("/tmp/") == 0);
        std::string c = "rm -r ";
        c += _path;
        ::system(c.c_str());
    }
    void writeFile(char const* dbname, int begin, int end) {
        std::string filename = _path;
        if(dbname) {
            filename = filename + "/empty_" + dbname + ".txt";
        }  else {
            filename = _fallback;
        }
        std::ofstream os(filename.c_str());
        for(int i=begin; i < end; ++i) {
            os << i << std::endl;
        }
        os.close();
    }
    std::string _path;
    std::string _fallback;
};

struct Fixture {
    Fixture() {

    }
    ~Fixture() {
    }
    DummyFile dummyFile;
};

BOOST_FIXTURE_TEST_SUITE(Suite, Fixture)

BOOST_AUTO_TEST_CASE(Basic) {
    EmptyChunks ec(dummyFile._path, dummyFile._fallback);
    EmptyChunks::IntSetConstPtr s = ec.getEmpty("TestOne");
    BOOST_CHECK(s->find(3) != s->end());
    BOOST_CHECK(s->find(103) == s->end());
    BOOST_CHECK(s->find(1001) == s->end());

    s = ec.getEmpty("TestTwo");
    BOOST_CHECK(s->find(3) == s->end());
    BOOST_CHECK(s->find(103) != s->end());
    BOOST_CHECK(s->find(1001) == s->end());

    BOOST_CHECK(ec.isEmpty("TestOne", 3));
    BOOST_CHECK(ec.isEmpty("TestTwo", 103));
    BOOST_CHECK(ec.isEmpty("Default", 1003));

}

BOOST_AUTO_TEST_SUITE_END()

