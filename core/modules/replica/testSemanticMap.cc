// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(SemanticMapsTest) {

    LOGS_INFO("SemanticMaps test begins");

    
    std::vector<unsigned int> chunks;
    for (unsigned int chunk = 0; chunk < 10; ++chunk) {
        chunks.push_back(chunk);
    }
    std::vector<std::string> const workers{"host-1","host-2","host-3"};
    std::vector<std::string> const databases{"A","B","C"};

    // ----------------------------------------------------
    // Test basic API using a 1-layer map for chunk numbers
    // ----------------------------------------------------

    // Testing an empty map

    detail::ChunkMap<double>        chunkMap;
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
    BOOST_CHECK_THROW(chunkMap.chunk(0), std::out_of_range);
    BOOST_CHECK_THROW(constChunkMap.chunk(0), std::out_of_range);

    // Implicit insert-then-modify

    BOOST_REQUIRE_NO_THROW({
        chunkMap.clear();
        BOOST_CHECK(chunkMap.empty());
    });
    for (auto&& chunk: chunks) {

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

        BOOST_REQUIRE_NO_THROW(
            BOOST_CHECK(chunkMap.chunk(chunk) == value);
            BOOST_CHECK(constChunkMap.chunk(chunk) == value);
            BOOST_CHECK(chunkMap.atChunk(chunk) == value);
        );
    }
    BOOST_REQUIRE_NO_THROW({

        // Test if all required keys are reported by the key extraction method

        std::vector<unsigned int> chunksFromMap = chunkMap.chunkNumbers();
        std::sort(chunksFromMap.begin(), chunksFromMap.end());
        BOOST_CHECK(chunksFromMap == chunks);

        std::vector<unsigned int> chunksFromConstMap = constChunkMap.chunkNumbers();
        std::sort(chunksFromConstMap.begin(), chunksFromConstMap.end());
        BOOST_CHECK(chunksFromConstMap == chunks);
    });

    // Explicit insert


    BOOST_REQUIRE_NO_THROW({
        chunkMap.clear();
        BOOST_CHECK(chunkMap.empty());
    });
    for (auto&& chunk: chunks) {

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

        BOOST_REQUIRE_NO_THROW(
            BOOST_CHECK(chunkMap.chunk(chunk) == value);
            BOOST_CHECK(constChunkMap.chunk(chunk) == value);
            BOOST_CHECK(chunkMap.atChunk(chunk) == value);
        );
    }

    // ----------------------------
    // Test API using a 3-layer map
    // ----------------------------

    typedef detail::WorkerMap<
            detail::DatabaseMap<
            detail::ChunkMap<double>>> WorkerDatabaseChunkMap;

    WorkerDatabaseChunkMap workerDatabaseChunkMap;
    WorkerDatabaseChunkMap const& constWorkerDatabaseChunkMap = workerDatabaseChunkMap;

    for (auto&& worker: workers) {
        for (auto&& database: databases) {
            for (auto&& chunk: chunks) {

                double const value = chunk * 30.;

                BOOST_REQUIRE_NO_THROW({

                    workerDatabaseChunkMap.atWorker(worker)
                                          .atDatabase(database)
                                          .atChunk(chunk) = value;

                    BOOST_CHECK(constWorkerDatabaseChunkMap.worker(worker)
                                                           .database(database)
                                                           .chunk(chunk) == value);
                });
            }
        }
    }
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK(workerDatabaseChunkMap.size() == workers.size());

        for (auto&& worker: workers) {
            auto& databaseChunkMap = workerDatabaseChunkMap.worker(worker);
            BOOST_CHECK(databaseChunkMap.size() == databases.size());

            for (auto&& database: databases) {
                auto& chunkMap = databaseChunkMap.database(database);
                BOOST_CHECK(chunkMap.size() == chunks.size());
                
                for (auto&& chunk: chunks) {
                    BOOST_CHECK(chunkMap.chunkExists(chunk));
                    BOOST_CHECK(chunkMap.chunk(chunk) == chunk * 30.);
                }
            }
        }
    });

    LOGS_INFO("SemanticMaps test ends");
}

BOOST_AUTO_TEST_SUITE_END()
