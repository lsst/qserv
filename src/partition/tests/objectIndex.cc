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
#define BOOST_TEST_MODULE ObjectIndex
#include "boost/test/unit_test.hpp"

#include "partition/Chunker.h"
#include "partition/Csv.h"
#include "partition/ObjectIndex.h"
#include "TempFile.h"

#include <stdexcept>
#include <string>
#include <vector>

#include "boost/filesystem.hpp"

using namespace lsst::partition;
namespace fs = boost::filesystem;

BOOST_AUTO_TEST_CASE(ObjectIndexTest) {
    BOOST_REQUIRE_NO_THROW({ ObjectIndex::instance(); });

    // It should be safe to close an index when it's not open
    BOOST_REQUIRE_NO_THROW({ ObjectIndex::instance()->close(); });
    BOOST_REQUIRE_NO_THROW({ ObjectIndex::instance()->isOpen(); });
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->isOpen(), false);
    BOOST_REQUIRE_NO_THROW({ ObjectIndex::instance()->mode(); });

    // Test for a failure to use an index while it's not open
    BOOST_CHECK_THROW(
            {
                std::string const id = "12345";
                ObjectIndex::instance()->read(id);
            },
            std::logic_error);

    // Test for a failure to use an index if it was not created
    BOOST_CHECK_THROW(
            {
                std::string const id = "12345";
                int32_t const chunkId = 1;
                int32_t const subChunkId = 2;
                bool const overlap = false;
                ChunkLocation const location(chunkId, subChunkId, overlap);
                ObjectIndex::instance()->write(id, location);
            },
            std::logic_error);

    TempFile t;
    std::string const indexFileName = fs::absolute(t.path()).string();
    std::string const indexUrl = "file://" + indexFileName;

    std::string const null = "\\N";
    char const delimiter = ',';
    char const escape = '\\';
    char const noQuote = '\0';
    csv::Dialect const dialect(null, delimiter, escape, noQuote);
    std::vector<std::string> const fields = {"id", "chunkId", "subChunkId"};
    csv::Editor editor(dialect, dialect, fields, fields);

    // Make sure input parameters are properly validated
    BOOST_CHECK_THROW({ ObjectIndex::instance()->create("", editor, "id", "chunkId", "subChunkId"); },
                      std::invalid_argument);
    BOOST_CHECK_THROW(
            { ObjectIndex::instance()->create(indexFileName, editor, "", "chunkId", "subChunkId"); },
            std::invalid_argument);
    BOOST_CHECK_THROW({ ObjectIndex::instance()->create(indexFileName, editor, "id", "", "subChunkId"); },
                      std::invalid_argument);
    BOOST_CHECK_THROW({ ObjectIndex::instance()->create(indexFileName, editor, "id", "chunkId", ""); },
                      std::invalid_argument);
    BOOST_CHECK_THROW({ ObjectIndex::instance()->open("", dialect); }, std::invalid_argument);
    BOOST_CHECK_THROW({ ObjectIndex::instance()->open("file:///", dialect); }, std::invalid_argument);

    // Creating an empty index
    BOOST_REQUIRE_NO_THROW(
            { ObjectIndex::instance()->create(indexFileName, editor, "id", "chunkId", "subChunkId"); });
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->isOpen(), true);
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->mode(), ObjectIndex::WRITE);
    BOOST_REQUIRE_NO_THROW({ ObjectIndex::instance()->close(); });
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->isOpen(), false);

    // Creating an index and populate it with one key.
    BOOST_REQUIRE_NO_THROW(
            { ObjectIndex::instance()->create(indexFileName, editor, "id", "chunkId", "subChunkId"); });
    std::string const validId = "123";
    BOOST_REQUIRE_NO_THROW({
        int32_t const chunkId = 1;
        int32_t const subChunkId = 2;
        bool const overlap = false;
        ChunkLocation const location(chunkId, subChunkId, overlap);
        ObjectIndex::instance()->write(validId, location);
    });

    // Test writing an object with a non-valid location (constructed using the default c-tor)
    BOOST_CHECK_THROW(
            {
                ChunkLocation const location;
                ObjectIndex::instance()->write(validId, location);
            },
            std::invalid_argument);

    // Close and open the index. Then fetch one valid object.
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->isOpen(), true);
    BOOST_REQUIRE_NO_THROW({
        ObjectIndex::instance()->close();
        ObjectIndex::instance()->open(indexUrl, dialect);
    });
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->isOpen(), true);
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->mode(), ObjectIndex::READ);
    std::pair<int32_t, int32_t> chunkSubChunk;
    BOOST_REQUIRE_NO_THROW({ chunkSubChunk = ObjectIndex::instance()->read(validId); });
    int32_t const chunkId = chunkSubChunk.first;
    int32_t const subChunkId = chunkSubChunk.second;
    BOOST_CHECK_EQUAL(chunkId, 1U);
    BOOST_CHECK_EQUAL(subChunkId, 2U);

    // While keeping the index open try fetching an non-valid object
    std::string const notValidId = "456";
    BOOST_CHECK_THROW({ chunkSubChunk = ObjectIndex::instance()->read(notValidId); }, std::out_of_range);
    BOOST_REQUIRE_NO_THROW({ ObjectIndex::instance()->close(); });
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->isOpen(), false);

    // Test if the index always works in the append mode if writing into the same
    // index file.
    std::string const anotherValidId = "456";
    BOOST_REQUIRE_NO_THROW({
        ObjectIndex::instance()->create(indexFileName, editor, "id", "chunkId", "subChunkId");
        int32_t const chunkId = 3;
        int32_t const subChunkId = 4;
        bool const overlap = false;
        ChunkLocation const location(chunkId, subChunkId, overlap);
        ObjectIndex::instance()->write(anotherValidId, location);
    });
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->isOpen(), true);
    BOOST_REQUIRE_NO_THROW({
        ObjectIndex::instance()->close();
        ObjectIndex::instance()->open(indexUrl, dialect);
    });
    BOOST_REQUIRE_NO_THROW({ chunkSubChunk = ObjectIndex::instance()->read(validId); });
    BOOST_CHECK_EQUAL(chunkSubChunk.first, 1U);
    BOOST_CHECK_EQUAL(chunkSubChunk.second, 2U);
    BOOST_REQUIRE_NO_THROW({ chunkSubChunk = ObjectIndex::instance()->read(anotherValidId); });
    BOOST_CHECK_EQUAL(chunkSubChunk.first, 3U);
    BOOST_CHECK_EQUAL(chunkSubChunk.second, 4U);
    BOOST_REQUIRE_NO_THROW({ ObjectIndex::instance()->close(); });
    BOOST_CHECK_EQUAL(ObjectIndex::instance()->isOpen(), false);

    // Open the index using non-empty name. Then fetch one valid object.
    {
        std::string const indexName = "id1";
        BOOST_CHECK_EQUAL(ObjectIndex::instance(indexName)->isOpen(), false);
        BOOST_REQUIRE_NO_THROW({ ObjectIndex::instance(indexName)->open(indexUrl, dialect); });
        BOOST_CHECK_EQUAL(ObjectIndex::instance(indexName)->isOpen(), true);
        BOOST_CHECK_EQUAL(ObjectIndex::instance(indexName)->mode(), ObjectIndex::READ);
        std::pair<int32_t, int32_t> chunkSubChunk;
        BOOST_REQUIRE_NO_THROW({ chunkSubChunk = ObjectIndex::instance(indexName)->read(validId); });
        int32_t const chunkId = chunkSubChunk.first;
        int32_t const subChunkId = chunkSubChunk.second;
        BOOST_CHECK_EQUAL(chunkId, 1U);
        BOOST_CHECK_EQUAL(subChunkId, 2U);
    }
}
