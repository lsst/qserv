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

 /**
  * @file
  *
  * @brief Test functions and structures used in QueryAnalysis tests
  *
  * @author Fabrice Jammes, IN2P3/SLAC
  */


#ifndef LSST_QSERV_TESTS_QUERYANAFIXTURE_H
#define LSST_QSERV_TESTS_QUERYANAFIXTURE_H

// System headers
#include <string>

// Qserv headers
#include "css/CssAccess.h"
#include "qproc/QuerySession.h"
#include "tests/testKvMap.h"
#include "tests/QueryAnaHelper.h"


namespace {

int cfgNum = 0;
std::string defaultDb = "LSST";
std::string mapBuffer(testKvMap);

}


namespace lsst {
namespace qserv {
namespace tests {

struct QueryAnaFixture {
    // To learn how to dump the map, see qserv/core/css/KvInterfaceImplMem.cc
    // Use admin/examples/testMap_generateMap

    QueryAnaFixture()
        : qsTest(cfgNum, css::CssAccess::createFromData(mapBuffer), defaultDb,
                 sql::SqlConfig(sql::SqlConfig::MOCK)) {
    };

    qproc::QuerySession::Test qsTest;
    QueryAnaHelper queryAnaHelper;
};

}}} // namespace lsst::qserv::tests


#endif // LSST_QSERV_TESTS_QUERYANAFIXTURE_H
