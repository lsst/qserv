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
 */

// System headers
#include <string>
#include <unistd.h>

// Boost unit test header
#define BOOST_TEST_MODULE CControl_1
#include "boost/test/included/unit_test.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/userQueryProxy.h"

namespace test = boost::test_tools;
using namespace lsst::qserv;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(UserQueryException) {
    LOGF_INFO("UserQuery exception catching");
    // Test that invalid session values do not crash the program.
    int session = 1000;
    ccontrol::UserQuery_getQueryProcessingError(session++);
    ccontrol::UserQuery_getExecutionError(session++);
    ccontrol::UserQuery_kill(session++);
    qproc::ChunkSpec chunkSpec;
    ccontrol::UserQuery_addChunk(session++, chunkSpec);
    ccontrol::UserQuery_submit(session++);
    ccontrol::UserQuery_join(session++);
}

BOOST_AUTO_TEST_SUITE_END()


