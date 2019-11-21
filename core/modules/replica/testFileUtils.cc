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

// Qserv headers
#include "replica/FileUtils.h"

// System headers
#include <iostream>
#include <fstream>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE FileUtils
#include "boost/test/included/unit_test.hpp"

using namespace std;
namespace fs = boost::filesystem;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

namespace {

bool fileExistsAndEmpty(string const& filePath) {
    boost::system::error_code errCode;
    const bool result = fs::exists(filePath, errCode);
    if (errCode.value() != 0) {
        throw runtime_error(
                string(__func__) + "failed to obtain a status of the temporary file: '" + filePath
                + "', error: " + errCode.message());
    }
    if (0 != fs::file_size(filePath)) {
        throw runtime_error(
                string(__func__) + "the temporary file: '" + filePath + "' is not empty");
    }
    return result;
}
}

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(FileUtils_createTemporaryFile) {

    LOGS_INFO("FileUtils::createTemporaryFile test begins");

    string const baseDir = "/tmp";
    string prefix;
    string model;
    string suffix;

    boost::system::error_code errCode;

    // NOTE: exceptions within \BOOST_REQUIRE_NO_THROW are intercepted
    // to improve the reporting of failures.

    string filePath;
    BOOST_REQUIRE_NO_THROW({
        try {
            filePath = FileUtils::createTemporaryFile(baseDir);
        } catch (exception const& ex) {
            cout << ex.what() << endl;
            throw;
        }
    });
    BOOST_REQUIRE_NO_THROW({
        try {
            BOOST_CHECK(fileExistsAndEmpty(filePath));
        } catch (exception const& ex) {
            cout << ex.what() << endl;
            throw;
        }
    });
    fs::remove(filePath, errCode);

    // Test if throws when the model is empty
    prefix = string();
    model = string();
    BOOST_CHECK_THROW({
        filePath = FileUtils::createTemporaryFile(baseDir, prefix, model);
    }, invalid_argument);

    // Test if throws when the maximum number of retries is less than 1
    prefix = string();
    model = "%%%%-%%%%-%%%%-%%%%";
    suffix = string();
    unsigned int maxRetries = 0;
    BOOST_CHECK_THROW({
        filePath = FileUtils::createTemporaryFile(baseDir, prefix, model, suffix, maxRetries);
    }, invalid_argument);

    // The following test pre-creates 16 files based on a fact that a single
    // letter '%' in the temporary model is replaced with a single character
    // representing a hexadecimal digit: ['0'-'f']. This will make the temporary
    // file creation utility to fail on any of of those 16 files due to
    // exceeding the total number of retries.
    
    vector<string> const digits = {
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
        "a", "b", "c", "d", "e", "f"
    };

    string baseFilePath;
    BOOST_REQUIRE_NO_THROW({
        try {
            baseFilePath = FileUtils::createTemporaryFile(baseDir);
        } catch (exception const& ex) {
            cout << ex.what() << endl;
            throw;
        }
    });
    for (auto&& d: digits) {
        string const filePath = baseFilePath + "-" + d;
        ofstream file(filePath);
        LOGS_INFO("FileUtils::createTemporaryFile pre-creating file: " + filePath);
        BOOST_CHECK(file.is_open());
    }

    suffix = string();
    maxRetries = digits.size();
    for (auto&& d: digits) {
        string const filePath = baseFilePath + "-" + d;
        LOGS_INFO("FileUtils::createTemporaryFile creating a temporary file: " + filePath);
        BOOST_CHECK_THROW({
            FileUtils::createTemporaryFile(baseFilePath, "-", "%", suffix, maxRetries);
        }, runtime_error);
    }

    fs::remove(baseFilePath, errCode);
    for (auto&& d: digits) {
        string const filePath = baseFilePath + "-" + d;
        fs::remove(filePath, errCode);
    }
    LOGS_INFO("FileUtils::createTemporaryFile test ends");
}

BOOST_AUTO_TEST_SUITE_END()
