// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Class header
#include "rproc/ProtoCsvBuffer.h"

// Qserv headers
#include "proto/worker.pb.h"
#include "proto/FakeProtocolFixture.h"

// Boost unit test header
#define BOOST_TEST_MODULE ProtoCsvBuffer_1
#include <boost/test/unit_test.hpp>

namespace test = boost::test_tools;
namespace gio = google::protobuf::io;

using lsst::qserv::rproc::ProtoCsvBuffer;

struct Fixture {
    Fixture(void) {}
    ~Fixture(void) {}
};

BOOST_FIXTURE_TEST_SUITE(suite, Fixture)

BOOST_AUTO_TEST_CASE(TestCopyColumn) {
    std::string simple = "Hello my name is bob";
    std::string eSimple = "'" + simple + "'";
    std::string target;
    int copied = ProtoCsvBuffer::copyColumn(target, simple);
    BOOST_CHECK_EQUAL(copied, static_cast<int>(eSimple.size()));
    BOOST_CHECK_EQUAL(target, eSimple);
}

BOOST_AUTO_TEST_SUITE_END()
