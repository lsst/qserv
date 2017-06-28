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
#define BOOST_TEST_MODULE ProtoRowBuffer_1
#include "boost/test/included/unit_test.hpp"


LOG_LOGGER _log = LOG_GET("lsst.qserv.rproc.testInvalidJobAttemptMgr");


namespace test = boost::test_tools;

namespace rproc = lsst::qserv::rproc;

struct Fixture {
    Fixture(void) {}
    ~Fixture(void) { }
};

class MockResult {
public:
    MockResult() {
        iJAMgr.setDeleteFunc([this](int id) -> bool {
            return deleteFunc(id);
        });
        iJAMgr.setTableExistsFunc([this]() -> bool {
            return tableExists_;
        });
    }

    bool deleteFunc(int id) {
        deleteCalled_ = true;
        testSet.erase(id);
        return deleteSuccess_;
    }

    rproc::InvalidJobAttemptMgr iJAMgr;
    std::multiset<int> testSet;
    std::mutex mtx;
    bool tableExists_{false};
    bool deleteCalled_{false};
    bool deleteSuccess_{true};

    void insert(int begin, int end) {
        tableExists_ = true;
        for(int j=begin; j <= end; ++j) {
            if (iJAMgr.isJobAttemptInvalid(j)) continue;
            iJAMgr.incrConcurrentMergeCount();
            {
                std::lock_guard<std::mutex> lck(mtx);
                testSet.insert(j);
            }
            iJAMgr.decrConcurrentMergeCount();
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

    LOGS_DEBUG("DeleteFunc should not be called since table doesn't exist.");
    int delRow0 = 7;
    mRes.iJAMgr.holdMergingForRowDelete(delRow0);
    BOOST_CHECK(mRes.deleteCalled_ == false);

    LOGS_DEBUG("Check if row removed from results.");
    mRes.insert(0, 20);
    unsigned int expectedSize = 20; // 21 - 1 for delRow0
    BOOST_CHECK(mRes.testSet.find(delRow0) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.size() == expectedSize);

    int delRow1 = 11;
    BOOST_CHECK(mRes.testSet.find(delRow1) != mRes.testSet.end());
    mRes.iJAMgr.holdMergingForRowDelete(delRow1);
    --expectedSize;
    LOGS_DEBUG("testSet=" << mRes.dumpTestSet());
    BOOST_CHECK(mRes.testSet.find(delRow1) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.size() == expectedSize);
    BOOST_CHECK(mRes.deleteCalled_ == true);

    LOGS_DEBUG("Check if row prevented from being added to results.");
    BOOST_CHECK(mRes.iJAMgr.isJobAttemptInvalid(delRow1) == true);
    mRes.insert(delRow1, delRow1);
    BOOST_CHECK(mRes.testSet.find(delRow1) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.size() == expectedSize);


    LOGS_DEBUG("Concurrent test");
    auto insertFunc = [&mRes](int b, int e) {
        mRes.insert(b, e);
    };

    std::vector<std::shared_ptr<std::thread>> tVect;
    int concurrent = 50;
    int count = 5000;
    for(int j=0; j < concurrent; ++j) {
        std::shared_ptr<std::thread> t(new std::thread(insertFunc, 0, count));
        tVect.push_back(t);
        expectedSize += count - 1; // count +1 for including 0, -1 for delRow0, -1 for delRow1
    }

    int delRow2 = 42;
    mRes.deleteCalled_ = false;
    mRes.iJAMgr.holdMergingForRowDelete(delRow2);
    expectedSize -= concurrent;
    BOOST_CHECK(mRes.deleteCalled_ == true);
    BOOST_CHECK(mRes.iJAMgr.isJobAttemptInvalid(delRow2) == true);

    LOGS_DEBUG("Concurrent test join");
    for (auto& thrd : tVect) {
        thrd->join();
    }

    LOGS_DEBUG("Concurrent test size should be correct, deleted rows should not be in the set.");
    BOOST_CHECK_EQUAL(mRes.testSet.size(), expectedSize);
    BOOST_CHECK(mRes.testSet.find(delRow0) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.find(delRow1) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.find(delRow2) == mRes.testSet.end());
    BOOST_CHECK(mRes.testSet.find(count) != mRes.testSet.end());
    LOGS_DEBUG("testSet=" << mRes.dumpTestSet());
}



BOOST_AUTO_TEST_SUITE_END()
