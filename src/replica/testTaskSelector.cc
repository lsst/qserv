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
/**
 * @brief Test a translation of wbase::TaskSelector into an HTTP query.
 */

// System headers
#include <string>
#include <vector>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/intTypes.h"
#include "replica/GetStatusQservMgtRequest.h"
#include "wbase/TaskState.h"

// Boost unit test header
#define BOOST_TEST_MODULE TaskSelector
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
namespace qserv = lsst::qserv;
namespace replica = lsst::qserv::replica;
namespace wbase = lsst::qserv::wbase;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(TaskSelectorTest) {
    LOGS_INFO("TaskSelectorTest test begins");

    string query;
    wbase::TaskSelector selector;
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(replica::taskSelectorToHttpQuery(selector), "?include_tasks=0&max_tasks=0");
    });

    selector.includeTasks = true;
    selector.maxTasks = 2U;
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(replica::taskSelectorToHttpQuery(selector), "?include_tasks=1&max_tasks=2");
    });

    selector.queryIds = {1, 2, 3};
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(replica::taskSelectorToHttpQuery(selector),
                          "?include_tasks=1&max_tasks=2&query_ids=1,2,3");
    });

    selector.taskStates.push_back(wbase::TaskState::EXECUTING_QUERY);
    selector.taskStates.push_back(wbase::TaskState::READING_DATA);
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(
                replica::taskSelectorToHttpQuery(selector),
                "?include_tasks=1&max_tasks=2&query_ids=1,2,3&task_states=EXECUTING_QUERY,READING_DATA");
    });

    selector.queryIds = {};
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(replica::taskSelectorToHttpQuery(selector),
                          "?include_tasks=1&max_tasks=2&task_states=EXECUTING_QUERY,READING_DATA");
    });

    LOGS_INFO("TaskSelectorTest test ends");
}

BOOST_AUTO_TEST_SUITE_END()
