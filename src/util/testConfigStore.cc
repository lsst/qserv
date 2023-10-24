// -*- LSST-C++ -*-
/*
 * LSST Data Management System
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
#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <ostream>
#include <set>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "util/ConfigStore.h"
#include "util/ConfigStoreError.h"

// Boost unit test header
#define BOOST_TEST_MODULE ConfigStore
#include <boost/test/unit_test.hpp>

namespace test = boost::test_tools;
namespace util = lsst::qserv::util;

BOOST_AUTO_TEST_SUITE(Suite)

std::ostream& operator<<(std::ostream& os, std::set<std::string> const& coll) {
    os << "[";
    std::ranges::copy(coll, std::ostream_iterator<std::string>(os, ","));
    os << "]";
    return os;
}

BOOST_AUTO_TEST_CASE(ConfigStoreTest) {
    LOGS_INFO("ConfigStore test begins");
    std::map<std::string, std::string> const keyValMap = {{"s1.p1", "v11"},       {"s1.p2", "v12"},
                                                          {"sect2.p1", "v21"},    {"sect2.p2", "v22"},
                                                          {"section3.p1", "v31"}, {"section3.p2", "v32"}};
    std::set<std::string> const expectedSections = {"s1", "sect2", "section3"};
    std::set<std::string> const expectedParameters = {"p1", "p2"};
    std::map<std::string, std::map<std::string, std::string>> const sectionKeyValMap = {
            {"s1", std::map<std::string, std::string>({{"p1", "v11"}, {"p2", "v12"}})},
            {"sect2", std::map<std::string, std::string>({{"p1", "v21"}, {"p2", "v22"}})},
            {"section3", std::map<std::string, std::string>({{"p1", "v31"}, {"p2", "v32"}})}};

    std::unique_ptr<util::ConfigStore> configStore;
    BOOST_REQUIRE_NO_THROW({ configStore.reset(new util::ConfigStore(keyValMap)); });
    BOOST_CHECK(configStore != nullptr);
    BOOST_REQUIRE_NO_THROW({
        for (auto [key, val] : keyValMap) {
            BOOST_CHECK_EQUAL(configStore->getRequired(key), val);
        }
    });
    std::string const unknownKey = "section4.p1";
    std::string const unknownVal = "v41";
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(configStore->get(unknownKey, unknownVal), unknownVal); });

    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK_EQUAL(configStore->get(unknownKey, unknownVal), unknownVal); });

    BOOST_CHECK_THROW({ configStore->getIntRequired(unknownKey); }, util::KeyNotFoundError);

    for (auto [key, val] : keyValMap) {
        BOOST_CHECK_THROW({ configStore->getIntRequired(key); }, util::InvalidIntegerValue);
    }

    for (auto [key, val] : keyValMap) {
        BOOST_CHECK_THROW({ configStore->getInt(key); }, util::InvalidIntegerValue);
    }

    std::set<std::string> sections;
    BOOST_REQUIRE_NO_THROW({ sections = configStore->getSections(); });
    BOOST_CHECK(expectedSections == sections);
    std::cout << "expectedSections: " << expectedSections << " sections: " << sections << std::endl;

    for (auto section : expectedSections) {
        BOOST_REQUIRE_NO_THROW({
            auto configSection = configStore->getSectionConfigMap(section);
            std::set<std::string> params;
            for (auto [param, val] : configSection) {
                params.insert(param);
                auto const expectedVal = sectionKeyValMap.at(section).at(param);
                BOOST_CHECK_EQUAL(val, expectedVal);
            }
            BOOST_CHECK(expectedParameters == params);
            std::cout << "expectedParameters: " << expectedParameters << " params: " << params << std::endl;
        });
    }
}

BOOST_AUTO_TEST_SUITE_END()
