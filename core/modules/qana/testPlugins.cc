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
  /**
  *
  * @brief Simple testing for QueryPlugin implementations
  *
  */
#define BOOST_TEST_MODULE QueryPlugins_1
#include "boost/test/included/unit_test.hpp"

// Local headers
#include "css/Facade.h"
#include "qana/AnalysisError.h"
#include "qana/QueryPlugin.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/TestFactory.h"

namespace test = boost::test_tools;

using lsst::qserv::qana::AnalysisError;
using lsst::qserv::qana::QueryPlugin;
using lsst::qserv::query::QueryContext;
using lsst::qserv::query::SelectStmt;
using lsst::qserv::query::TestFactory;


struct TestFixture {
    TestFixture(void) {
        // To learn how to dump the map, see qserv/core/css/KvInterfaceImplMem.cc
        // Use admin/examples/testMap_generateMap
        std::string kvMapPath = "./modules/qana/testPlugins.kvmap"; // FIXME
        cssFacade = lsst::qserv::css::FacadeFactory::createMemFacade(kvMapPath);
    }

    ~TestFixture(void) {}

    boost::shared_ptr<lsst::qserv::css::Facade> cssFacade;
    int metaSession;
};


BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)

BOOST_AUTO_TEST_CASE(Exceptions) {
    // Should throw an Analysis error, because columnref is invalid.
    // Under normal operation, the columnref is patched by the TablePlugin
    QueryPlugin::Ptr qp = QueryPlugin::newInstance("QservRestrictor");
    TestFactory factory;
    boost::shared_ptr<QueryContext> qc = factory.newContext(cssFacade);
    boost::shared_ptr<SelectStmt> stmt = factory.newStmt();
    qp->prepare();
    BOOST_CHECK_THROW(qp->applyLogical(*stmt, *qc), AnalysisError);
#if 0
    std::list<boost::shared_ptr<SelectStmt> > parallel;
    parallel.push_back(stmt->copyDeep());
    boost::shared_ptr<SelectStmt> mergeStmt = stmt->copyMerge();
    QueryPlugin::Plan p(*stmt, parallel, *mergeStmt, false);
    qp->applyPhysical(p, *qc);
    qp->applyFinal(*qc);
#endif
}


BOOST_AUTO_TEST_SUITE_END()



