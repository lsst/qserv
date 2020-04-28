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
#include <cassert>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/NamedMutexRegistry.h"
#include "util/Mutex.h"
#include "util/BlockPost.h"

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE NamedMutexRegistry
#include "boost/test/included/unit_test.hpp"

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {
/**
 * Generator of numbers uniformally distributed in the closed interval [from,to]
 * (values at both ends are included into the distribution).
 * @note The class has thread-safe implementation.
 */
class RandomUniformIndex {
public:
    /**
     * @param from The minimum value of the distribution.
     * @param to The minimum value of the distribution.
     */
    explicit RandomUniformIndex(size_t from, size_t to)
        :   _rd(),
            _gen(_rd()),
            _distrib(from, to) {
        assert(from <= to);
    }
    RandomUniformIndex(RandomUniformIndex const&) = delete;
    RandomUniformIndex& operator=(RandomUniformIndex const&) = delete;
    ~RandomUniformIndex() = default;

    /// @return The next value generated.
    size_t next() {
        lock_guard<mutex> lock(_mtx);
        size_t const val = _distrib(_gen);
        _stats[val] += 1;
        return val;
    }

    /// @param val The value to be inspected.
    /// @return The number of times the specified value was produced.
    size_t stats(size_t val) const {
        lock_guard<mutex> lock(_mtx);
        auto&& itr = _stats.find(val);
        return itr == _stats.cend() ? 0 : itr->second;
    }

private:
    mutable mutex _mtx;
    random_device _rd;
    mt19937 _gen;
    uniform_int_distribution<size_t> _distrib;
    map<size_t, size_t> _stats;
};
}

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(NamedMutexRegistryTest1) {

    LOGS_INFO("NamedMutexRegistryTest1 BEGIN");

    // Test non-throwing constructor
    BOOST_REQUIRE_NO_THROW({
        NamedMutexRegistry registry;
    });

    // Method get() should throw if the empty string is passed as the name of
    // a mutex.
    BOOST_CHECK_THROW({
        NamedMutexRegistry registry;
        registry.get(string());
    }, invalid_argument);

    // Method get() should not throw if called more than one time in a row for the same
    // name of a mutex.
    BOOST_REQUIRE_NO_THROW({
        NamedMutexRegistry registry;
        registry.get("a");
        registry.get("a");
    });

    // Test that Mutex::id() is the same for the same name.
    BOOST_REQUIRE_NO_THROW({
        NamedMutexRegistry registry;
        auto const m1 = registry.get("a");
        auto const m2 = registry.get("a");
        BOOST_CHECK_EQUAL(m1->id(), m2->id());
    });

    // Test that Mutex::id() is different for different names.
    BOOST_REQUIRE_NO_THROW({
        NamedMutexRegistry registry;
        auto const m1 = registry.get("a");
        auto const m2 = registry.get("b");
        BOOST_CHECK(m1->id() != m2->id());
    });
    LOGS_INFO("NamedMutexRegistryTest1 END");
}

BOOST_AUTO_TEST_CASE(NamedMutexRegistryTest2) {

    LOGS_INFO("NamedMutexRegistryTest2 BEGIN");

    // Test the correctness of the registry using a non-atomic
    // counter to be incremented after acquiring a lock.
    unique_ptr<NamedMutexRegistry> registry;
    BOOST_REQUIRE_NO_THROW({
        registry = make_unique<NamedMutexRegistry>();
    });
    if (registry != nullptr) {
        unsigned int counter = 0;
        unsigned int const steps = 1024;
        unsigned int const numThreads = min(2U, thread::hardware_concurrency());
        vector<unique_ptr<thread>> threads(numThreads);
        for (auto&& thr: threads) {
            thr = make_unique<thread>([&](){
                for (unsigned int i = 0; i < steps; ++i) {
                    util::Lock const lock(registry->get("a"), __func__);
                    ++counter;
                }
            });
        }
        for (auto&& thr: threads) {
            thr->join();
        }
        BOOST_CHECK_EQUAL(counter, steps * numThreads);
    }
    LOGS_INFO("NamedMutexRegistryTest2 END");
}

BOOST_AUTO_TEST_CASE(NamedMutexRegistryTest3) {

    // This is a more sophisticated test that would exercise the "garbage collection"
    // algorithm of the Registry. The test would make N objects of KeyCounterContext
    // type that encapsulate the unique name of a mutex along with a numeric counter
    // to be incremented under protection of a lock held on the mutex. The algorithm
    // will start many threads. Each threads has a loop that would be picking a random
    // key and incrementing a counter correspoding to the key. Then the thread
    // will optionally keep the lock for the specified duration of time and/or wait for
    // another interval after releasing the last lock and before acquiring another one.

    struct TestPlan {
        string name;
        unsigned int numMutexes;
        unsigned int numLocksPerThread;
        unsigned int numThreads;
        int keepLockTimeMs;                 // if 0 then release lock immediately
        int waitAfterReleaseLockTimeMs;     // if 0 then acquire another lock immediately
    };

    auto test = [](TestPlan const& plan) {

        LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] BEGIN");
        LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] numMutexes: " << plan.numMutexes);
        LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] numLocksPerThread: " << plan.numLocksPerThread);
        LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] numThreads: " << plan.numThreads);
        LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] keepLockTimeMs: " << plan.keepLockTimeMs);
        LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] waitAfterReleaseLockTimeMs: " << plan.waitAfterReleaseLockTimeMs);
 
        unique_ptr<NamedMutexRegistry> registry;
        BOOST_REQUIRE_NO_THROW({
            registry = make_unique<NamedMutexRegistry>();
        });
        if (registry != nullptr) {

            // Contexts simulate data objects (simple counters) protected by mutexes. 
            struct KeyCounterContext {
                string const key = Generators::uniqueId();
                unsigned int counter = 0;
            };
            vector<KeyCounterContext> contexts(plan.numMutexes);

            // The generator of indexes to the contexts is used to allow random access to
            // the contexts by each threads.
            assert(contexts.size() > 0);
            RandomUniformIndex index(0, contexts.size() - 1);

            vector<unique_ptr<thread>> threads(plan.numThreads);
            for (auto&& thr: threads) {
                thr = make_unique<thread>([&]() {

                    // For generating delays in the specified interval of milliseconds.
                    util::BlockPost delay(0, max(1, max(plan.keepLockTimeMs, plan.waitAfterReleaseLockTimeMs)));

                    for (unsigned int i = 0; i < plan.numLocksPerThread; ++i) {
                        size_t const idx = index.next();
                        assert(idx <= contexts.size());
                        KeyCounterContext& context = contexts[idx];

                        // This lock will be released after suspending the thread for
                        // a duration of the delay (if any).
                        {
                            util::Lock const lock(registry->get(context.key), __func__);
                            context.counter++;
                            if (plan.keepLockTimeMs > 0) {
                                delay.wait(plan.keepLockTimeMs);
                            }
                        }
                        // Another lock will be requested after suspending the thread for
                        // a duration of another delay (if any).
                        if (plan.waitAfterReleaseLockTimeMs > 0) {
                            delay.wait(plan.waitAfterReleaseLockTimeMs);
                        }
                    }
                });
            }
            for (auto&& thr: threads) {
                thr->join();
            }

            // This loop will check if values of the  counters match the number of times
            // the corresponding keys were used. This test relies upon the statistics on
            // indexes (of the contexts) collected by the index generator.
            for (size_t idx = 0; idx < contexts.size(); ++idx) {
                auto&& context = contexts[idx];
                size_t const keyUseCounter = index.stats(idx);
                LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] key: " << context.key
                        << " counter: " << context.counter
                        << " keyUseCounter: " << keyUseCounter);
                BOOST_CHECK_EQUAL(context.counter, keyUseCounter);
            }

            // In the worst case scenario, the remaining number of entries in the registry
            // won't exceed the number of active threads.
            LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] registry.size: " << registry->size());
            BOOST_CHECK(registry->size() <= plan.numThreads);
        }
        LOGS_INFO("NamedMutexRegistryTest3 [" << plan.name << "] END");
    };
    {
        TestPlan plan;
        plan.name = "SINGLE_THREAD";
        plan.numMutexes = 128;
        plan.numLocksPerThread = 128 * 1024;
        plan.numThreads = 1;
        plan.keepLockTimeMs = 0;
        plan.waitAfterReleaseLockTimeMs = 0;
        test(plan);
    }
     {
        TestPlan plan;
        plan.name = "MAX_CPU_USAGE";
        plan.numMutexes = 128;
        plan.numLocksPerThread = 16 * 1024;
        plan.numThreads = 64 * thread::hardware_concurrency();
        plan.keepLockTimeMs = 0;
        plan.waitAfterReleaseLockTimeMs = 0;
        test(plan);
    }
    {
        TestPlan plan;
        plan.name = "KEEP_LOCKS";
        plan.numMutexes = 128;
        plan.numLocksPerThread = 1024;
        plan.numThreads = thread::hardware_concurrency();
        plan.keepLockTimeMs = 2;
        plan.waitAfterReleaseLockTimeMs = 0;
        test(plan);
    }
    {
        TestPlan plan;
        plan.name = "KEEP_LOCKS_AND_WAIT";
        plan.numMutexes = 128;
        plan.numLocksPerThread = 1024;
        plan.numThreads = thread::hardware_concurrency();
        plan.keepLockTimeMs = 1;
        plan.waitAfterReleaseLockTimeMs = 1;
        test(plan);
    }
    {
        TestPlan plan;
        plan.name = "WAIT_BETWEEN_LOCKS";
        plan.numMutexes = 128;
        plan.numLocksPerThread = 1024;
        plan.numThreads = thread::hardware_concurrency();
        plan.keepLockTimeMs = 0;
        plan.waitAfterReleaseLockTimeMs = 1;
        test(plan);
    }
}

BOOST_AUTO_TEST_SUITE_END()