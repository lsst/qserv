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
#ifndef LSST_QSERV_CZAR_WORKERINGESTPROCESSOR_H
#define LSST_QSERV_CZAR_WORKERINGESTPROCESSOR_H

// System headers
#include <condition_variable>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// This header declarations
namespace lsst::qserv::czar::ingest {

/**
 * The synchronized queue class. The class is used for storing the worker ingest requests
 * and results.
 */
template <typename Entry>
class Queue : public std::enable_shared_from_this<Queue<Entry>> {
public:
    static std::shared_ptr<Queue<Entry>> create() {
        return std::shared_ptr<Queue<Entry>>(new Queue<Entry>());
    }
    Queue(Queue const&) = delete;
    Queue& operator=(Queue const&) = delete;
    void push(Entry const& entry) {
        std::lock_guard<std::mutex> lock(_mtx);
        _entries.push_back(entry);
        _cv.notify_one();
    }
    Entry pop() {
        std::unique_lock<std::mutex> lock(_mtx);
        _cv.wait(lock, [this]() { return !_entries.empty(); });
        Entry const result = _entries.front();
        _entries.pop_front();
        return result;
    }

private:
    Queue() = default;
    std::mutex _mtx;
    std::condition_variable _cv;
    std::list<Entry> _entries;
};

/**
 * A structure for storing the worker ingest result.
 */
struct Result {
    std::string worker;
    std::string error;
};
using ResultQueue = Queue<Result>;

/**
 * A class for storing the worker ingest request. The class is used for storing the request
 * processing function and the result queue where the function result is stored.
 * @note The function should not throw exceptions.
 */
class Request {
public:
    Request() = default;
    Request(Request const&) = default;
    Request(std::function<Result()> const& processor, std::shared_ptr<ResultQueue> resultQueue);
    void process();

private:
    std::function<Result()> const _processor;
    std::shared_ptr<ResultQueue> const _resultQueue;
};
using RequestQueue = Queue<Request>;

/**
 * A class for processing the worker ingest requests. The class is used for processing the
 * worker ingest requests in parallel by a number of threads.
 */
class Processor : public std::enable_shared_from_this<Processor> {
public:
    static std::shared_ptr<Processor> create(unsigned int numThreads);
    Processor(Processor const&) = delete;
    Processor& operator=(Processor const&) = delete;
    void push(Request const& req);

private:
    Processor(unsigned int numThreads);
    std::vector<std::thread> _threads;
    std::shared_ptr<RequestQueue> _requestQueue;
};

}  // namespace lsst::qserv::czar::ingest

#endif  // LSST_QSERV_CZAR_WORKERINGESTPROCESSOR_H
