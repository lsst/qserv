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
#include <stdexcept>
#include <iostream>

// Qserv headers
#include "replica/Url.h"

// Boost unit test header
#define BOOST_TEST_MODULE Url
#include "boost/test/included/unit_test.hpp"

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(UrlTest) {

    unique_ptr<Url> ptr;

    // Empty resources aren't allowed
    BOOST_CHECK_THROW({
        ptr.reset(new Url(string()));
    }, invalid_argument);

    // Resources which are too short to include anyting by the name of a scheme
    // aren't allowed.
    BOOST_CHECK_THROW({
        ptr.reset(new Url("file:///"));
    }, invalid_argument);
    BOOST_CHECK_THROW({
        ptr.reset(new Url("http://"));
    }, invalid_argument);
    BOOST_CHECK_THROW({
        ptr.reset(new Url("https://"));
    }, invalid_argument);

    // Check for non-supported resources 
    BOOST_CHECK_THROW({
        ptr.reset(new Url("other:///////"));
    }, invalid_argument);

    // Test file-based URLs
    string const fileUrl = "file:///a";
    //BOOST_REQUIRE_NO_THROW({
        ptr.reset(new Url(fileUrl));
    //});
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(ptr->url(), fileUrl);
    });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(ptr->scheme(), Url::Scheme::FILE);
    });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(ptr->filePath(), "/a");
    });

    // Test HTTP-based URLs
    string const httpUrl = "http://a";
    BOOST_REQUIRE_NO_THROW({
        ptr.reset(new Url(httpUrl));
    });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(ptr->url(), httpUrl);
    });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(ptr->scheme(), Url::Scheme::HTTP);
    });
    BOOST_CHECK_THROW({
        ptr->filePath();
    }, logic_error);

    // Test HTTPS-based URLs
    string const httpsUrl = "https://b";
    BOOST_REQUIRE_NO_THROW({
        ptr.reset(new Url(httpsUrl));
    });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(ptr->url(), httpsUrl);
    });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK_EQUAL(ptr->scheme(), Url::Scheme::HTTPS);
    });
    BOOST_CHECK_THROW({
        ptr->filePath();
    }, logic_error);
}

BOOST_AUTO_TEST_SUITE_END()