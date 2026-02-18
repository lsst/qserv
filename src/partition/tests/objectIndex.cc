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
    std::string const validId = "123";
    std::string const anotherValidId = "456";

    // The write test for the index.
    {
        ObjectIndex index;
        BOOST_REQUIRE_NO_THROW({ index.isOpen(); });
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_REQUIRE_NO_THROW({ index.mode(); });

        // Test for a failure to use an index while it's not open
        BOOST_CHECK_THROW(
                {
                    std::string const id = "12345";
                    index.read(id);
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
                    index.write(id, location);
                },
                std::logic_error);

        // Make sure input parameters are properly validated
        BOOST_CHECK_THROW(
                { index.create("", editor, "id", "chunkId", "subChunkId"); }, std::invalid_argument);
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_CHECK_THROW(
                { index.create(indexFileName, editor, "", "chunkId", "subChunkId"); }, std::invalid_argument);
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_CHECK_THROW(
                { index.create(indexFileName, editor, "id", "", "subChunkId"); }, std::invalid_argument);
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_CHECK_THROW(
                { index.create(indexFileName, editor, "id", "chunkId", ""); }, std::invalid_argument);
        BOOST_CHECK_EQUAL(index.isOpen(), false);

        // Creating an empty index and populate it with one key.
        BOOST_REQUIRE_NO_THROW({ index.create(indexFileName, editor, "id", "chunkId", "subChunkId"); });
        BOOST_CHECK_EQUAL(index.isOpen(), true);
        BOOST_CHECK_EQUAL(index.mode(), ObjectIndex::WRITE);

        // Creating an empty index and populate it with the first key.
        BOOST_REQUIRE_NO_THROW({ index.create(indexFileName, editor, "id", "chunkId", "subChunkId"); });
        BOOST_REQUIRE_NO_THROW({
            int32_t const chunkId = 1;
            int32_t const subChunkId = 2;
            bool const overlap = false;
            ChunkLocation const location(chunkId, subChunkId, overlap);
            index.write(validId, location);
        });

        // Test writing an object with a non-valid location (constructed using the default c-tor)
        BOOST_CHECK_THROW(
                {
                    ChunkLocation const location;
                    index.write(validId, location);
                },
                std::invalid_argument);
    }

    // The read test for the index that was stored in the file by the previous test.
    {
        ObjectIndex index;
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_CHECK_THROW({ index.open("", dialect); }, std::invalid_argument);
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_CHECK_THROW({ index.open("file:///", dialect); }, std::invalid_argument);
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_REQUIRE_NO_THROW({ index.open(indexUrl, dialect); });
        BOOST_CHECK_EQUAL(index.isOpen(), true);
        BOOST_CHECK_EQUAL(index.mode(), ObjectIndex::READ);
        std::pair<int32_t, int32_t> chunkSubChunk;
        BOOST_REQUIRE_NO_THROW({ chunkSubChunk = index.read(validId); });
        int32_t const chunkId = chunkSubChunk.first;
        int32_t const subChunkId = chunkSubChunk.second;
        BOOST_CHECK_EQUAL(chunkId, 1U);
        BOOST_CHECK_EQUAL(subChunkId, 2U);

        // While keeping the index open try fetching an non-valid object
        std::string const notValidId = "456";
        BOOST_CHECK_THROW({ chunkSubChunk = index.read(notValidId); }, std::out_of_range);
    }

    // Test if the index always works in the append mode if writing into the same
    // index file.
    {
        ObjectIndex index;
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_REQUIRE_NO_THROW({
            index.create(indexFileName, editor, "id", "chunkId", "subChunkId");
            int32_t const chunkId = 3;
            int32_t const subChunkId = 4;
            bool const overlap = false;
            ChunkLocation const location(chunkId, subChunkId, overlap);
            index.write(anotherValidId, location);
        });
        BOOST_CHECK_EQUAL(index.isOpen(), true);
        BOOST_CHECK_EQUAL(index.mode(), ObjectIndex::WRITE);
    }

    // Now read the index file again and make sure that both entries are there.
    {
        ObjectIndex index;
        BOOST_CHECK_EQUAL(index.isOpen(), false);
        BOOST_REQUIRE_NO_THROW({ index.open(indexUrl, dialect); });
        BOOST_CHECK_EQUAL(index.isOpen(), true);
        BOOST_CHECK_EQUAL(index.mode(), ObjectIndex::READ);
        std::pair<int32_t, int32_t> chunkSubChunk;
        BOOST_REQUIRE_NO_THROW({ chunkSubChunk = index.read(validId); });
        BOOST_CHECK_EQUAL(chunkSubChunk.first, 1U);
        BOOST_CHECK_EQUAL(chunkSubChunk.second, 2U);
        BOOST_REQUIRE_NO_THROW({ chunkSubChunk = index.read(anotherValidId); });
        BOOST_CHECK_EQUAL(chunkSubChunk.first, 3U);
        BOOST_CHECK_EQUAL(chunkSubChunk.second, 4U);
    }
}
