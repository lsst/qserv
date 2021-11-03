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
#ifndef LSST_QSERV_REPLICA_MESSAGEQUEUE_H
#define LSST_QSERV_REPLICA_MESSAGEQUEUE_H

// System headers
#include <algorithm>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>


// Qserv headers
#include "replica/Common.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class MessageQueue is the priority-based queue for storing shared pointers
 * to objects of class MessageWrapperBase. Requests of the same priority are
 * organized as the FIFO-based sub-queues (priority "lanes").
 * 
 * The implementation is optimized for three most frequent operations with
 * the queue:
 * - fetching the first element from the front of the highest-priority lane,
 * - pushing elements to the front of the corresponding priority lane,
 * - pushing elements to the end of the corresponding priority lane.
 * Since the number of unique priorities in the Replication system's framework
 * is rather small the performances of the above-mentioned operations is nearly
 * constant in this implementation.
 * 
 * The lookup operation based on a unique identifier of an object has the 'O(n)'
 * performance. The operation is of no concern since it's only used in response
 * to the message cancellation requests, which are rather infrequent.
 *
 * @note The implementation won't enforce the uniqueness of elements of
 *   the same identifier accross different priority lanes. It's up to
 *   the application to prevent this.
 * @note an alternative option of using the Standard Library's adaptor class
 *   'std::priority_queue<T>' would result in the poor performance here because
 *   (according to the documentation for the class) it "...provides constant time
 *   lookup of the largest (by default) element, at the expense of logarithmic
 *   insertion and extraction...". Another problem with the adaptor is that it
 *   won't guarantee the FIFO ordering of objects within the same priority lane,
 *   which is essential for this application.
 * @note The implementation is not thread safe. It's up to the user code
 *   to ensure exclusive access to the queue.
 * @note The class template is needed to allow unit testing the queue using
 *   simplified element classes.
 *
 * @see class MessageWrapperBase.
 */
template <class T>
class MessageQueue {
public:
    MessageQueue() = default;
    MessageQueue(MessageQueue const&) = default;
    MessageQueue& operator=(MessageQueue const&) = default;
    ~MessageQueue() = default;

    /// @return 'true' if the collection is empty.
    bool empty() const { return size() == 0; }

    /// @return The total number of elements (of any priority) in the collection.
    size_t size() const {
        size_t num = 0;
        for (auto&& itr: _priority2lane) num += itr.second.size();
        return num;
    }

    /// Push the element to the back of the corresponding priority lane.
    void push_back(std::shared_ptr<T> const& e) { _priority2lane[e->priority()].push_back(e); }

    /// Push the element to the front of the corresponding priority lane.
    void push_front(std::shared_ptr<T> const& e) { _priority2lane[e->priority()].push_front(e); }

    /**
     * Locate and return an element from the front of the highest priority lane.
     * @note The element gets removed from the collection.
     * @return A copy of the element or an empty element initialized with 'nullptr'
     *   if the collection is empty.
     */
    std::shared_ptr<T> front() {
        std::vector<int> priorities;
        priorities.reserve(_priority2lane.size());
        for (auto&& itr: _priority2lane) priorities.push_back(itr.first);
        sort(priorities.begin(), priorities.end(), std::greater<int>());
        for (int priority: priorities) {
            auto& lane = _priority2lane[priority];
            if (!lane.empty()) {
                std::shared_ptr<T> const ptr = lane.front();
                lane.pop_front();
                return ptr;
            }
        }
        return nullptr;
    }

    /**
     * Locate and return an element matching the specified identifier.
     * @return A copy of the element or an empty element initialized with 'nullptr'
     *   if no such element exists in the collection.
     */
    std::shared_ptr<T> find(std::string const& id) const {
        for (auto&& itr: _priority2lane) {
            auto& lane = itr.second;
            if (!lane.empty()) {
                auto laneItr = std::find_if(lane.cbegin(), lane.cend(), [&id] (auto ptr) { return ptr->id() == id; });
                if (laneItr != lane.cend()) return *laneItr;
            }
        }
        return nullptr;
    }

    /// Locate and remove an element matching the specified identifier.
    void remove(std::string const& id) {
        for (auto&& itr: _priority2lane) {
            auto&& lane = itr.second;
            lane.remove_if([&id] (auto ptr) { return ptr->id() == id; });
        }
    }
private:
    std::map<int, std::list<std::shared_ptr<T>>> _priority2lane;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_MESSAGEQUEUE_H
