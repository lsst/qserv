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

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <algorithm>
#include <list>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

// Qserv headers
#include "replica/mysql/DatabaseMySQLUtils.h"

// Boost unit test header
#define BOOST_TEST_MODULE DatabaseMySQLUtils
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ObjectNameTranslationTest) {
    LOGS_INFO("ObjectNameTranslation test begins");

    // The empty name is not allowed.
    BOOST_CHECK_THROW(database::mysql::obj2fs(""), std::invalid_argument);
    BOOST_CHECK_THROW(database::mysql::fs2obj(""), std::invalid_argument);

    // Translation is not required for characters in this name.
    // Reverse translation must produce the input name.
    string const objectNameNoTrans = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    BOOST_CHECK_EQUAL(objectNameNoTrans, database::mysql::obj2fs(objectNameNoTrans));
    BOOST_CHECK_EQUAL(objectNameNoTrans, database::mysql::fs2obj(objectNameNoTrans));

    // Translation is required for all characters in this name.
    string const objectNameTransRequired1 = R"( !"#$%&'()*+,-./:;<=>?@[\]^`{|}~)";
    string const expectedFileSystemName1 =
            "@0020"
            "@0021"
            "@0022"
            "@0023"
            "@0024"
            "@0025"
            "@0026"
            "@0027"
            "@0028"
            "@0029"
            "@002a"
            "@002b"
            "@002c"
            "@002d"
            "@002e"
            "@002f"
            "@003a"
            "@003b"
            "@003c"
            "@003d"
            "@003e"
            "@003f"
            "@0040"
            "@005b"
            "@005c"
            "@005d"
            "@005e"
            "@0060"
            "@007b"
            "@007c"
            "@007d"
            "@007e";
    BOOST_CHECK_EQUAL(expectedFileSystemName1, database::mysql::obj2fs(objectNameTransRequired1));

    // Test the case when each special character is met exactly twice
    string const objectNameTransRequired2 = objectNameTransRequired1 + objectNameTransRequired1;
    string const expectedFileSystemName2 = expectedFileSystemName1 + expectedFileSystemName1;
    BOOST_CHECK_EQUAL(expectedFileSystemName2, database::mysql::obj2fs(objectNameTransRequired2));

    // Check a few corner cases for the file system-safe names
    string const fileSystemName3 =
            "abcd@"
            "@002"
            "@0021"
            "@00222"
            "@00"
            "@0"
            "@"
            "@0026"
            "@0027"
            "123456789_@"
            "@007e";
    string const expectedObjectName3 =
            "abcd@"
            "@002"
            "!"
            "\"2"
            "@00"
            "@0"
            "@"
            "&"
            "'"
            "123456789_@"
            "~";
    BOOST_CHECK_EQUAL(expectedObjectName3, database::mysql::fs2obj(fileSystemName3));

    // Bidirectional translation must produce the input name.
    BOOST_CHECK_EQUAL(objectNameTransRequired1,
                      database::mysql::fs2obj(database::mysql::obj2fs(objectNameTransRequired1)));
    BOOST_CHECK_EQUAL(objectNameTransRequired2,
                      database::mysql::fs2obj(database::mysql::obj2fs(objectNameTransRequired2)));
    BOOST_CHECK_EQUAL(expectedObjectName3,
                      database::mysql::fs2obj(database::mysql::obj2fs(expectedObjectName3)));

    // Check the validity of the object names
    BOOST_CHECK_THROW(database::mysql::isValidObjectName(""), std::invalid_argument);
    BOOST_CHECK(database::mysql::isValidObjectName(objectNameNoTrans));
    BOOST_CHECK(database::mysql::isValidObjectName(objectNameTransRequired1));
    BOOST_CHECK(database::mysql::isValidObjectName(objectNameTransRequired2));
    BOOST_CHECK(database::mysql::isValidObjectName(expectedObjectName3));
    BOOST_CHECK(!database::mysql::isValidObjectName("\t\n\r\f\v"));
}

BOOST_AUTO_TEST_SUITE_END()