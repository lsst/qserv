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
#include "replica/util/FileUtils.h"
#include "util/String.h"

// System headers
#include <iostream>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <system_error>

// Boost unit test header
#define BOOST_TEST_MODULE FileUtils
#include <boost/test/unit_test.hpp>

using namespace std;
namespace fs = std::filesystem;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

bool fileExistsAndEmpty(string const& filePath) {
    std::error_code errCode;
    const bool result = fs::exists(filePath, errCode);
    if (errCode.value() != 0) {
        throw runtime_error(string(__func__) + "failed to obtain a status of the temporary file: '" +
                            filePath + "', error: " + errCode.message());
    }
    if (0 != fs::file_size(filePath)) {
        throw runtime_error(string(__func__) + "the temporary file: '" + filePath + "' is not empty");
    }
    return result;
}
}  // namespace

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(FileUtils_createTemporaryFile) {
    LOGS_INFO("FileUtils::createTemporaryFile test begins");

    string const baseDir = "/tmp";
    string prefix;
    string model;
    string suffix;

    std::error_code errCode;

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
    BOOST_CHECK_THROW(
            { filePath = FileUtils::createTemporaryFile(baseDir, prefix, model); }, invalid_argument);

    // Test if throws when the maximum number of retries is less than 1
    prefix = string();
    model = "%%%%-%%%%-%%%%-%%%%";
    suffix = string();
    unsigned int maxRetries = 0;
    BOOST_CHECK_THROW(
            { filePath = FileUtils::createTemporaryFile(baseDir, prefix, model, suffix, maxRetries); },
            invalid_argument);

    // The following test pre-creates 16 files based on a fact that a single
    // letter '%' in the temporary model is replaced with a single character
    // representing a hexadecimal digit: ['0'-'f']. This will make the temporary
    // file creation utility to fail on any of of those 16 files due to
    // exceeding the total number of retries.

    vector<string> const digits = {"0", "1", "2", "3", "4", "5", "6", "7",
                                   "8", "9", "a", "b", "c", "d", "e", "f"};

    string baseFilePath;
    BOOST_REQUIRE_NO_THROW({
        try {
            baseFilePath = FileUtils::createTemporaryFile(baseDir);
        } catch (exception const& ex) {
            cout << ex.what() << endl;
            throw;
        }
    });
    for (auto&& d : digits) {
        string const filePath = baseFilePath + "-" + d;
        ofstream file(filePath);
        LOGS_INFO("FileUtils::createTemporaryFile pre-creating file: " + filePath);
        BOOST_CHECK(file.is_open());
    }

    suffix = string();
    maxRetries = digits.size();
    for (auto&& d : digits) {
        string const filePath = baseFilePath + "-" + d;
        LOGS_INFO("FileUtils::createTemporaryFile creating a temporary file: " + filePath);
        BOOST_CHECK_THROW(
                { FileUtils::createTemporaryFile(baseFilePath, "-", "%", suffix, maxRetries); },
                runtime_error);
    }

    fs::remove(baseFilePath, errCode);
    for (auto&& d : digits) {
        string const filePath = baseFilePath + "-" + d;
        fs::remove(filePath, errCode);
    }
    LOGS_INFO("FileUtils::createTemporaryFile test ends");
}

BOOST_AUTO_TEST_CASE(FileUtils_verifyFolders) {
    LOGS_INFO("FileUtils::verifyFolders test begins");

    bool const createMissingFolders = true;

    {
        vector<string> const folders = {string()};
        BOOST_CHECK_THROW({ FileUtils::verifyFolders("TEST", folders); }, invalid_argument);
    }
    {
        vector<string> const folders = {"relative/path"};
        BOOST_CHECK_THROW({ FileUtils::verifyFolders("TEST", folders); }, invalid_argument);
    }

    string const pattern = "/tmp/test-folder-%%%%-%%%%-%%%%-%%%%";

    // This test is allowed to be repeated one time in a very unlikely case
    // if the randomly generated folder name is already taken.
    unsigned int const maxRetries = 1;
    unsigned int numRetriesLeft = maxRetries;

    bool success = false;

    while (numRetriesLeft-- > 0) {
        std::error_code ec;

        // Generate a unique path for the folder to be tested/created
        fs::path const uniqueFolderPath = util::String::translateModel(pattern);

        // Make sure the folder (or a file) doesn't exist. Otherwise make another
        // attempt.
        fs::file_status const stat = fs::status(uniqueFolderPath, ec);
        if (stat.type() == fs::file_type::none) {
            throw runtime_error("Failed to check a status of the temporary folder: '" +
                                uniqueFolderPath.string() + "', error: " + ec.message());
        }
        if (fs::exists(stat)) continue;

        LOGS_INFO("FileUtils::verifyFolders temporary folder: " + uniqueFolderPath.string());

        // At the very first run of the method do not attempt to create
        // the missing folder.
        vector<string> const folders = {uniqueFolderPath.string()};
        BOOST_CHECK_THROW({ FileUtils::verifyFolders("TEST", folders, !createMissingFolders); }, exception);

        // Now launch the method to force create the folder.
        BOOST_REQUIRE_NO_THROW({ FileUtils::verifyFolders("TEST", folders, createMissingFolders); });

        // Repeat the preious operation. It should not fail since the method first checks
        // if the path already exists and if it's a valid directory before creating the one.
        BOOST_REQUIRE_NO_THROW({ FileUtils::verifyFolders("TEST", folders, createMissingFolders); });

        // Make another run w/o attempting creating a folder
        BOOST_REQUIRE_NO_THROW({ FileUtils::verifyFolders("TEST", folders, !createMissingFolders); });

        // Now, make the best attempt to delete the folder. Ignore any errors.
        fs::remove(fs::path(uniqueFolderPath), ec);

        success = true;
        break;
    }
    if (!success) {
        throw runtime_error(
                "The maximum number of attempts to generate a unique folder name has exceeded the limit.");
    }
    LOGS_INFO("FileUtils::verifyFolders test ends");
}

BOOST_AUTO_TEST_SUITE_END()
