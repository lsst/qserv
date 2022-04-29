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
 * @brief test SemanticMap
 */

// System headers
#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/SemanticMaps.h"

// Boost unit test header
#define BOOST_TEST_MODULE SemanticMaps
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(SemanticMapsTest) {
    LOGS_INFO("SemanticMaps test begins");

    vector<unsigned int> chunkNumbers;
    for (unsigned int chunk = 0; chunk < 10; ++chunk) {
        chunkNumbers.push_back(chunk);
    }
    vector<string> const workerNames{"host-1", "host-2", "host-3"};
    vector<string> const databaseNames{"A", "B", "C"};

    // ----------------------------------------------------
    // Test basic API using a 1-layer map for chunk numbers
    // ----------------------------------------------------

    // Testing an empty map

    detail::ChunkMap<double> chunkMap;
    detail::ChunkMap<double> const& constChunkMap = chunkMap;

    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK(chunkMap.size() == 0);
        BOOST_CHECK(constChunkMap.size() == 0);

        BOOST_CHECK(chunkMap.empty());
        BOOST_CHECK(constChunkMap.empty());

        BOOST_CHECK(not chunkMap.chunkExists(0));
        BOOST_CHECK(not constChunkMap.chunkExists(0));

        BOOST_CHECK(chunkMap.chunkNumbers().size() == 0);
        BOOST_CHECK(constChunkMap.chunkNumbers().size() == 0);

        BOOST_CHECK(chunkMap.empty());
    });
    BOOST_CHECK_THROW(chunkMap.chunk(0), out_of_range);
    BOOST_CHECK_THROW(constChunkMap.chunk(0), out_of_range);

    // Implicit insert-then-modify

    BOOST_REQUIRE_NO_THROW({
        chunkMap.clear();
        BOOST_CHECK(chunkMap.empty());
    });
    for (auto&& chunk : chunkNumbers) {
        double const value = chunk * 10.;

        BOOST_REQUIRE_NO_THROW({
            chunkMap.atChunk(chunk) = value;

            BOOST_CHECK(chunkMap.size() == (chunk + 1));
            BOOST_CHECK(constChunkMap.size() == (chunk + 1));

            BOOST_CHECK(not chunkMap.empty());
            BOOST_CHECK(not constChunkMap.empty());

            BOOST_CHECK(chunkMap.chunkExists(chunk));
            BOOST_CHECK(constChunkMap.chunkExists(chunk));

            BOOST_CHECK(chunkMap.chunkNumbers().size() == (chunk + 1));
            BOOST_CHECK(constChunkMap.chunkNumbers().size() == (chunk + 1));
        });

        // Test if key values retrieved by the method match inserts

        BOOST_REQUIRE_NO_THROW(BOOST_CHECK(chunkMap.chunk(chunk) == value);
                               BOOST_CHECK(constChunkMap.chunk(chunk) == value);
                               BOOST_CHECK(chunkMap.atChunk(chunk) == value););
    }
    BOOST_REQUIRE_NO_THROW({
        // Test if all required keys are reported by the key extraction method

        vector<unsigned int> chunksFromMap = chunkMap.chunkNumbers();
        sort(chunksFromMap.begin(), chunksFromMap.end());
        BOOST_CHECK(chunksFromMap == chunkNumbers);

        vector<unsigned int> chunksFromConstMap = constChunkMap.chunkNumbers();
        sort(chunksFromConstMap.begin(), chunksFromConstMap.end());
        BOOST_CHECK(chunksFromConstMap == chunkNumbers);
    });

    // Explicit insert

    BOOST_REQUIRE_NO_THROW({
        chunkMap.clear();
        BOOST_CHECK(chunkMap.empty());
    });
    for (auto&& chunk : chunkNumbers) {
        double const value = chunk * 20.;

        BOOST_REQUIRE_NO_THROW({
            chunkMap.insertChunk(chunk, value);

            BOOST_CHECK(chunkMap.size() == (chunk + 1));
            BOOST_CHECK(constChunkMap.size() == (chunk + 1));

            BOOST_CHECK(not chunkMap.empty());
            BOOST_CHECK(not constChunkMap.empty());

            BOOST_CHECK(chunkMap.chunkExists(chunk));
            BOOST_CHECK(constChunkMap.chunkExists(chunk));

            BOOST_CHECK(chunkMap.chunkNumbers().size() == (chunk + 1));
            BOOST_CHECK(constChunkMap.chunkNumbers().size() == (chunk + 1));
        });

        // Test if key values retrieved by the method match inserts

        BOOST_REQUIRE_NO_THROW(BOOST_CHECK(chunkMap.chunk(chunk) == value);
                               BOOST_CHECK(constChunkMap.chunk(chunk) == value);
                               BOOST_CHECK(chunkMap.atChunk(chunk) == value););
    }

    // ----------------------------
    // Test API using a 3-layer map
    // ----------------------------

    WorkerDatabaseChunkMap<double> workerDatabaseChunkMap;
    WorkerDatabaseChunkMap<double> const& constWorkerDatabaseChunkMap = workerDatabaseChunkMap;

    for (auto&& worker : workerNames) {
        for (auto&& database : databaseNames) {
            for (auto&& chunk : chunkNumbers) {
                double const value = chunk * 30.;

                BOOST_REQUIRE_NO_THROW({
                    workerDatabaseChunkMap.atWorker(worker).atDatabase(database).atChunk(chunk) = value;

                    BOOST_CHECK(constWorkerDatabaseChunkMap.worker(worker).database(database).chunk(chunk) ==
                                value);
                });
            }
        }
    }
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK(workerDatabaseChunkMap.size() == workerNames.size());

        for (auto&& worker : workerNames) {
            auto& databaseChunkMap = workerDatabaseChunkMap.worker(worker);
            BOOST_CHECK(databaseChunkMap.size() == databaseNames.size());

            for (auto&& database : databaseNames) {
                auto& chunkMap = databaseChunkMap.database(database);
                BOOST_CHECK(chunkMap.size() == chunkNumbers.size());

                for (auto&& chunk : chunkNumbers) {
                    BOOST_CHECK(chunkMap.chunkExists(chunk));
                    BOOST_CHECK(chunkMap.chunk(chunk) == chunk * 30.);
                }
            }
        }
    });

    // ---------------------------------------
    // Test 'diff2' and 'intersect' algorithms
    // ---------------------------------------

    auto dump = [](WorkerDatabaseChunkMap<int> const& d, string const& indent = "  ") {
        for (auto&& worker : d.workerNames()) {
            for (auto&& database : d.worker(worker).databaseNames()) {
                for (auto&& chunk : d.worker(worker).database(database).chunkNumbers()) {
                    LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"),
                               indent << "[" << worker + "][" << database + "][" << chunk << "] = "
                                      << to_string(d.worker(worker).database(database).chunk(chunk)));
                }
            }
        }
    };

    WorkerDatabaseChunkMap<int> one;
    WorkerDatabaseChunkMap<int> two;

    // common
    one.atWorker("A").atDatabase("a").atChunk(1) = 1;
    two.atWorker("A").atDatabase("a").atChunk(1) = 1;

    // diff #1
    one.atWorker("A").atDatabase("a").atChunk(2) = 2;
    one.atWorker("A").atDatabase("a").atChunk(3) = 3;
    one.atWorker("A").atDatabase("b").atChunk(4) = 4;
    one.atWorker("B").atDatabase("c").atChunk(5) = 5;

    // diff #2
    two.atWorker("C").atDatabase("x").atChunk(6) = 6;

    // Find intersects
    WorkerDatabaseChunkMap<int> inBoth;
    BOOST_REQUIRE_NO_THROW({ SemanticMaps::intersect(one, two, inBoth); });

    // Find differences
    WorkerDatabaseChunkMap<int> inOneOnly;
    WorkerDatabaseChunkMap<int> inTwoOnly;
    BOOST_REQUIRE_NO_THROW({ BOOST_CHECK(SemanticMaps::diff2(one, two, inOneOnly, inTwoOnly)); });

    // Report and test the findings
    LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"), "one:");
    dump(one);

    LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"), "two:");
    dump(two);

    LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"), "inBoth:");
    dump(inBoth);

    LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"), "inOneOnly:");
    dump(inOneOnly);

    LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"), "inTwoOnly:");
    dump(inTwoOnly);

    // assert: diff #2
    BOOST_CHECK(inBoth.size() == 1 and inBoth.workerExists("A") and inBoth.worker("A").size() == 1 and
                inBoth.worker("A").databaseExists("a") and inBoth.worker("A").database("a").size() == 1 and
                inBoth.worker("A").database("a").chunkExists(1) and
                inBoth.worker("A").database("a").chunk(1) == 1);

    // assert: diff #1
    BOOST_CHECK(
            inOneOnly.size() == 2 and inOneOnly.workerExists("A") and inOneOnly.worker("A").size() == 2 and
            inOneOnly.worker("A").databaseExists("a") and inOneOnly.worker("A").database("a").size() == 2 and
            inOneOnly.worker("A").database("a").chunkExists(2) and
            inOneOnly.worker("A").database("a").chunk(2) == 2 and
            inOneOnly.worker("A").database("a").chunkExists(3) and
            inOneOnly.worker("A").database("a").chunk(3) == 3 and
            inOneOnly.worker("A").databaseExists("b") and inOneOnly.worker("A").database("b").size() == 1 and
            inOneOnly.worker("A").database("b").chunkExists(4) and
            inOneOnly.worker("A").database("b").chunk(4) == 4 and inOneOnly.workerExists("B") and
            inOneOnly.worker("B").size() == 1 and inOneOnly.worker("B").databaseExists("c") and
            inOneOnly.worker("B").database("c").size() == 1 and
            inOneOnly.worker("B").database("c").chunkExists(5) and
            inOneOnly.worker("B").database("c").chunk(5) == 5);

    // assert: diff #2
    BOOST_CHECK(inTwoOnly.size() == 1 and inTwoOnly.workerExists("C") and
                inTwoOnly.worker("C").size() == 1 and inTwoOnly.worker("C").databaseExists("x") and
                inTwoOnly.worker("C").database("x").size() == 1 and
                inTwoOnly.worker("C").database("x").chunkExists(6) and
                inTwoOnly.worker("C").database("x").chunk(6) == 6);

    // -----------------------
    // Test iterator semantics
    // -----------------------

    // prepare a map

    WorkerDatabaseChunkMap<int> workers;

    BOOST_REQUIRE_NO_THROW({
        workers.atWorker("A").atDatabase("a").atChunk(1) = 1;
        workers.atWorker("A").atDatabase("a").atChunk(2) = 2;
        workers.atWorker("A").atDatabase("b").atChunk(3) = 3;
        workers.atWorker("B").atDatabase("c").atChunk(4) = 4;

        LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"), "workers");

        for (auto&& worker : workers) {
            auto&& workerName = worker.first;
            auto&& databases = worker.second;

            for (auto&& database : databases) {
                auto&& databaseName = database.first;
                auto&& chunks = database.second;

                for (auto&& chunk : chunks) {
                    auto&& chunkNumber = chunk.first;
                    auto&& value = chunk.second;

                    LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"),
                               "  [" << workerName + "][" << databaseName + "][" << chunkNumber
                                     << "] = " << to_string(value));
                }
            }
        }

        LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"), "constWorkers");

        WorkerDatabaseChunkMap<int> const& constWorkers = workers;

        for (auto&& worker : constWorkers) {
            auto&& workerName = worker.first;
            auto&& databases = worker.second;

            for (auto&& database : databases) {
                auto&& databaseName = database.first;
                auto&& chunks = database.second;

                for (auto&& chunk : chunks) {
                    auto&& chunkNumber = chunk.first;
                    auto&& value = chunk.second;

                    LOGLS_INFO(LOG_GET("lsst.qserv.testSemanticMap"),
                               "  [" << workerName + "][" << databaseName + "][" << chunkNumber
                                     << "] = " << to_string(value));
                }
            }
        }
    });

    LOGS_INFO("SemanticMaps test ends");
}

BOOST_AUTO_TEST_SUITE_END()
