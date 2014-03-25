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
#include "qana/QueryPlugin.h"
#include "query/TestFactory.h"
#include "query/SelectStmt.h"
#include "query/QueryContext.h"
#include "meta/ifaceMeta.h"
#include "meta/MetadataCache.h"
#include "qana/AnalysisError.h"

namespace test = boost::test_tools;

using lsst::qserv::master::MetadataCache;
using lsst::qserv::master::QueryContext;
using lsst::qserv::master::SelectStmt;
using lsst::qserv::master::QueryPlugin;
using lsst::qserv::query::TestFactory;
using lsst::qserv::qana::AnalysisError;

struct TestFixture {
    TestFixture(void) {
        prepareMetadata();
    }
    void prepareMetadata() {
        metaSession = lsst::qserv::master::newMetadataSession();
        metadataCache =
            lsst::qserv::master::getMetadataCache(metaSession);
        boost::shared_ptr<MetadataCache> mc = metadataCache;
        mc->addDbInfoPartitionedSphBox("Somedb",
                                       60,        // number of stripes
                                       18,        // number of substripes
                                       0.01,      // default overlap fuzziness
                                       0.025);    // default overlap near neighbor

        mc->addTbInfoNonPartitioned("Somedb", "Bar");
        mc->addDbInfoPartitionedSphBox("LSST",
                                       60,        // number of stripes
                                       18,        // number of substripes
                                       0.01,      // default overlap fuzziness
                                       0.025);    // default overlap near neighbor
        mc->addTbInfoPartitionedSphBox("LSST", "Object",
                                       0.025,     // actual overlap
                                       "ra_Test", "decl_Test", "objectIdObjTest",
                                       1, 2, 0,   // positions of the above columns
                                       2,         // 2-level chunking
                                       0x0021);   // 1-level persisted
        mc->addTbInfoPartitionedSphBox("LSST", "Source",
                                       0,         // actual overlap
                                       "raObjectTest", "declObjectTest", "objectIdSourceTest",
                                       1, 2, 0,   // positions of the above columns
                                       1,         // 1-level chunking
                                       0x0011);   // 1-level persisted
    }


    ~TestFixture(void) {}

    boost::shared_ptr<lsst::qserv::master::MetadataCache> metadataCache;
    int metaSession;
};


BOOST_FIXTURE_TEST_SUITE(Suite, TestFixture)

BOOST_AUTO_TEST_CASE(Exceptions) {
    // Should throw an Analysis error, because columnref is invalid.
    // Under normal operation, the columnref is patched by the TablePlugin
    QueryPlugin::Ptr qp = QueryPlugin::newInstance("QservRestrictor");
    TestFactory factory;
    boost::shared_ptr<QueryContext> qc = factory.newContext(metadataCache.get());
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



