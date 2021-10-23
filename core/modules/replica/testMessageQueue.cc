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
#include <memory>
#include <ostream>
#include <string>

// Qserv headers
#include "replica/MessageQueue.h"

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE NamedMutexRegistry
#include "boost/test/included/unit_test.hpp"

using namespace std;
using namespace boost::unit_test;
using namespace lsst::qserv::replica;

namespace {
/**
 * Class Element represents a simplified version of class MessageWrapperBase
 * that's used by Messanger.
 */
class Element {
public:
    Element(string const& id, int priority) : _id(id), _priority(priority) {}
    Element() = default;
    Element(Element const&) = default;
    Element& operator=(Element const&) = default;
    ~Element() = default;
    string const& id() const { return _id; }
    int priority() const { return _priority; }
    bool operator==(Element const& e) const { return _id == e._id && _priority == e._priority; }
private:
    string _id;
    int _priority = 0;
};
ostream& operator<<(ostream& os, Element const& e) {
    os << "Element(" << e.id() << "," << e.priority() << ")";
    return os;
}
}

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(MessageQueueTest) {
    LOGS_INFO("MessageQueueTest BEGIN");

    MessageQueue<Element> queue;

    BOOST_CHECK(queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), 0U);
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(queue.front() == nullptr); });
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(queue.find("missing-element-id") == nullptr); });
    BOOST_REQUIRE_NO_THROW({ queue.remove("missing-element-id"); });

    // Elements that will be used for testing the queue.
    shared_ptr<Element> const id_1_pri_1(new Element("id_1", 1));
    shared_ptr<Element> const id_2_pri_1(new Element("id_2", 1));
    shared_ptr<Element> const id_3_pri_2(new Element("id_3", 2));
    shared_ptr<Element> const id_4_pri_2(new Element("id_4", 2));
    shared_ptr<Element> const id_5_pri_2(new Element("id_5", 2));
    shared_ptr<Element> const id_6_pri_3(new Element("id_6", 3));
    shared_ptr<Element> const id_7_pri_3(new Element("id_7", 3));
    shared_ptr<Element> const id_8_pri_3(new Element("id_8", 3));
    shared_ptr<Element> const id_9_pri_3(new Element("id_9", 3));
    vector<shared_ptr<Element>> const allElements = {
        id_1_pri_1, id_2_pri_1,
        id_3_pri_2, id_4_pri_2, id_5_pri_2,
        id_6_pri_3, id_7_pri_3, id_8_pri_3, id_9_pri_3
    };

    // Check adding and then removing a single element from the queue.
    BOOST_REQUIRE_NO_THROW({ queue.push_back(id_1_pri_1); });
    BOOST_CHECK(!queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), 1U);
    {
        shared_ptr<Element> e = queue.front();
        BOOST_CHECK(e != nullptr);
        BOOST_CHECK_EQUAL(*e, *id_1_pri_1);
        BOOST_CHECK(queue.empty());
        BOOST_CHECK_EQUAL(queue.size(), 0U);
    }

    // Add all elements and check that the're pulling in the expected order,
    // in which higher priority elements are returned in the FIFO order they
    // were insert.
    for (auto&& e: allElements) {
        BOOST_REQUIRE_NO_THROW({ queue.push_back(e); });
    }
    BOOST_CHECK(!queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), allElements.size());
    // - priority lane: 3
    BOOST_CHECK_EQUAL(*(queue.front()), *id_6_pri_3);
    BOOST_CHECK_EQUAL(*(queue.front()), *id_7_pri_3);
    BOOST_CHECK_EQUAL(*(queue.front()), *id_8_pri_3);
    BOOST_CHECK_EQUAL(*(queue.front()), *id_9_pri_3);
    // - priority lane: 2
    BOOST_CHECK_EQUAL(*(queue.front()), *id_3_pri_2);
    BOOST_CHECK_EQUAL(*(queue.front()), *id_4_pri_2);
    BOOST_CHECK_EQUAL(*(queue.front()), *id_5_pri_2);
    // - priority lane: 1
    BOOST_CHECK_EQUAL(*(queue.front()), *id_1_pri_1);
    BOOST_CHECK_EQUAL(*(queue.front()), *id_2_pri_1);
    // - the queue must be empty at this point
    BOOST_CHECK(queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), 0U);

    // Check if the FIFO ordering works within the same priority lane
    for (auto&& e: allElements) {
        BOOST_REQUIRE_NO_THROW({ queue.push_back(e); });
    }
    BOOST_CHECK(!queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), allElements.size());
    {
        shared_ptr<Element> e = queue.front();
        BOOST_CHECK_EQUAL(*e, *id_6_pri_3);
        BOOST_CHECK_EQUAL(queue.size(), allElements.size() - 1);
        queue.push_front(e);
        BOOST_CHECK_EQUAL(queue.size(), allElements.size());
        e = queue.front();
        BOOST_CHECK_EQUAL(*e, *id_6_pri_3);
        BOOST_CHECK_EQUAL(queue.size(), allElements.size() - 1);
        queue.push_back(e);
        BOOST_CHECK_EQUAL(queue.size(), allElements.size());
        e = queue.front();
        BOOST_CHECK_EQUAL(*e, *id_7_pri_3);
        BOOST_CHECK_EQUAL(queue.size(), allElements.size() - 1);
    }

    // Pull the remaining elements to clear the queue in preparation for
    // further tests.
    for (size_t i = 0, size = queue.size(); i < size; ++i) queue.front();
    BOOST_CHECK(queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), 0U);

    // Test locating elements by the identifiers.
    // Note that elements should be staying in the queue s a result of this operaton.
    for (auto&& e: allElements) queue.push_back(e);
    BOOST_CHECK(!queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), allElements.size());
    for (auto&& e: allElements) {
        shared_ptr<Element> found;
        BOOST_REQUIRE_NO_THROW({ found = queue.find(e->id()); });
        BOOST_CHECK(found != nullptr);
        BOOST_CHECK_EQUAL(*e, *found);
    }
    BOOST_CHECK(!queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), allElements.size());

    // Test removing elements by the identifiers.
    size_t remainingSize = allElements.size();
    for (auto&& e: allElements) {
        BOOST_CHECK(queue.find(e->id()) != nullptr);
        BOOST_REQUIRE_NO_THROW({ queue.remove(e->id()); });
        BOOST_CHECK_EQUAL(queue.size(), --remainingSize);
        BOOST_CHECK(queue.find(e->id()) == nullptr);
    }
    BOOST_CHECK(queue.empty());
    BOOST_CHECK_EQUAL(queue.size(), 0U);

    LOGS_INFO("MessageQueueTest END");
}

BOOST_AUTO_TEST_SUITE_END()