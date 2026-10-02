// -*- LSST-C++ -*-

// System headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/Raii.h"

// Boost unit test header
#define BOOST_TEST_MODULE RaiiTest
#include <boost/test/unit_test.hpp>

namespace test = boost::test_tools;
namespace util = lsst::qserv::util;

using namespace std;


BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(RaiiCounterTest) {
    LOGS_INFO("RaiiCounterTest begins");

    auto counterA = util::RaiiCounter<int>::create();
    BOOST_REQUIRE(counterA->getCount() == 0);
    BOOST_REQUIRE(counterA->getName() == "none");

    auto counterB = util::RaiiCounter<unsigned int>::create("counterB");
    BOOST_REQUIRE(counterB->getCount() == 0);
    BOOST_REQUIRE(counterB->getName() == "counterB");

    uint64_t const cStart = 89;
    auto counterC = util::RaiiCounter<uint64_t>::create(cStart);
    BOOST_REQUIRE(counterC->getCount() == cStart);
    BOOST_REQUIRE(counterC->getName() == "none");

    int64_t const dStart = 9876;
    auto counterD = util::RaiiCounter<int64_t>::create("counterD", dStart);
    BOOST_REQUIRE(counterD->getCount() == dStart);
    BOOST_REQUIRE(counterD->getName() == "counterD");

    {
        auto raiiA = counterA->createRaii();
        BOOST_REQUIRE(counterA->getCount() == 1);

        auto raiiB = counterB->createRaii();
        BOOST_REQUIRE(counterB->getCount() == 1);

        auto raiiC = counterC->createRaii();
        BOOST_REQUIRE(counterC->getCount() == cStart + 1);

        auto raiiD = counterD->createRaii();
        BOOST_REQUIRE(counterD->getCount() == dStart + 1);

    }
    BOOST_REQUIRE(counterA->getCount() == 0);
    BOOST_REQUIRE(counterB->getCount() == 0);
    BOOST_REQUIRE(counterC->getCount() == cStart);
    BOOST_REQUIRE(counterD->getCount() == dStart);
}

BOOST_AUTO_TEST_SUITE_END()




