// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
#include "rproc/InfileMerger.h"

// System Headers
#include <set>

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE InvalidJobAttemptMgr_1
#include <boost/test/unit_test.hpp>

LOG_LOGGER _log = LOG_GET("lsst.qserv.rproc.testInvalidJobAttemptMgr");

namespace test = boost::test_tools;

namespace rproc = lsst::qserv::rproc;

struct Fixture {
    Fixture(void) {}
    ~Fixture(void) {}
};

class MockResult {
public:
    MockResult() {
        iJAMgr.setDeleteFunc([this](rproc::InvalidJobAttemptMgr::jASetType const& jobAttempts) -> bool {
            return deleteFunc(jobAttempts);
        });
    }

    bool deleteFunc(std::set<int> const& jobAttempts) {
        deleteCalled_ = true;
        for (auto iter = jobAttempts.begin(), end = jobAttempts.end(); iter != end; ++iter) {
            int id = *iter;
            testSet.erase(id);
        }
        return true;
    }

    rproc::InvalidJobAttemptMgr iJAMgr;
    std::multiset<int> testSet;
    std::mutex mtx;
    bool tableExists_{false};
    bool deleteCalled_{false};

    void insert(int begin, int end) {
        tableExists_ = true;
        for (int j = begin; j <= end; ++j) {
            if (!iJAMgr.incrConcurrentMergeCount(j)) {
                {
                    std::lock_guard<std::mutex> lck(mtx);
                    testSet.insert(j);
                }
                iJAMgr.decrConcurrentMergeCount();
            }
        }
    }

    std::string dumpTestSet() {
        std::string str;
        for (auto i : testSet) {
            str += std::to_string(i) + ", ";
        }
        return str;
    }
};

BOOST_FIXTURE_TEST_SUITE(suite, Fixture)

BOOST_AUTO_TEST_CASE(InvalidJob) {
    MockResult mRes;

    LOGS_DEBUG("test: DeleteFunc should not be called since table doesn't exist.");
    int delRow0 = 7;
    mRes.iJAMgr.prepScrub(delRow0);
    mRes.iJAMgr.holdMergingForRowDelete();
    BOOST_CHECK(mRes.deleteCalled_ == false);

    LOGS_DEBUG("test: Check if row removed from results.");
    mRes.insert(0, 20);
    unsigned int expectedSize = 20;  // 21 - 1 for delRow0
    BOOST_CHECK(mRes.testSet.find(delRow0) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.size() == expectedSize);

    LOGS_DEBUG("test: Check if existing row removed from results.");
    int delRow1 = 11;
    BOOST_CHECK(mRes.testSet.find(delRow1) != mRes.testSet.end());
    mRes.iJAMgr.prepScrub(delRow1);
    mRes.iJAMgr.holdMergingForRowDelete();
    --expectedSize;
    LOGS_DEBUG("testSet=" << mRes.dumpTestSet());
    BOOST_CHECK(mRes.testSet.find(delRow1) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.size() == expectedSize);
    BOOST_CHECK(mRes.deleteCalled_ == true);

    LOGS_DEBUG("test: Check if row prevented from being added to results.");
    BOOST_CHECK(mRes.iJAMgr.isJobAttemptInvalid(delRow1) == true);
    mRes.insert(delRow1, delRow1);
    BOOST_CHECK(mRes.testSet.find(delRow1) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.size() == expectedSize);

    LOGS_DEBUG("test: Check to make sure delete is not called on row that has not been added.");
    int delRow2 = 37;  // Does not exist in result set.
    BOOST_CHECK(mRes.testSet.find(delRow2) == mRes.testSet.end());
    mRes.deleteCalled_ = false;
    mRes.iJAMgr.prepScrub(delRow2);
    mRes.iJAMgr.holdMergingForRowDelete();
    BOOST_CHECK(mRes.deleteCalled_ == false);

    LOGS_DEBUG("Concurrent test");
    auto insertFunc = [&mRes](int b, int e) { mRes.insert(b, e); };

    std::vector<std::shared_ptr<std::thread>> tVect;
    int concurrent = 50;
    int count = 5000;
    for (int j = 0; j < concurrent; ++j) {
        std::shared_ptr<std::thread> t(new std::thread(insertFunc, 0, count));
        tVect.push_back(t);
        expectedSize += count - 2;  // count +1 for including 0, -1 for delRow0,
                                    // -1 for delRow1, -1 for delRow2
    }

    int delRow3 = 42;
    mRes.iJAMgr.prepScrub(delRow3);
    mRes.iJAMgr.holdMergingForRowDelete();
    expectedSize -= concurrent;
    BOOST_CHECK(mRes.iJAMgr.isJobAttemptInvalid(delRow3) == true);

    LOGS_DEBUG("Concurrent test join");
    for (auto& thrd : tVect) {
        thrd->join();
    }

    int delRow4 = 101;
    mRes.deleteCalled_ = false;
    mRes.iJAMgr.prepScrub(delRow4);
    mRes.iJAMgr.holdMergingForRowDelete();
    expectedSize -= concurrent;
    BOOST_CHECK(mRes.deleteCalled_ == true);
    BOOST_CHECK(mRes.iJAMgr.isJobAttemptInvalid(delRow4) == true);

    LOGS_DEBUG("Concurrent test size should be correct, deleted rows should not be in the set.");
    BOOST_CHECK_EQUAL(mRes.testSet.size(), expectedSize);
    BOOST_CHECK(mRes.testSet.find(delRow0) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.find(delRow1) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.find(delRow2) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.find(delRow3) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.find(delRow4) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.find(count) != mRes.testSet.end());
    // LOGS_DEBUG("testSet=" << mRes.dumpTestSet());
}

BOOST_AUTO_TEST_SUITE_END()
