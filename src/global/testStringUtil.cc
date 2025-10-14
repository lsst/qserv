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
#include <filesystem>
#include <fstream>
#include <string>
#include <random>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/stringUtil.h"

// Boost unit test header
#define BOOST_TEST_MODULE stringUtil
#include <boost/test/unit_test.hpp>

namespace fs = std::filesystem;

namespace {

// Generate a random alphanumeric suffix for temp dirs
static std::string randomSuffix(size_t len = 8) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static thread_local std::mt19937 rng(std::random_device{}());
    static thread_local std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);
    std::string s;
    s.reserve(len);
    for (size_t i = 0; i < len; ++i) s.push_back(charset[dist(rng)]);
    return s;
}

// RAII class to manage a temporary directory
struct TempDir {
    fs::path path;
    explicit TempDir(std::string_view prefix = "global-test-") {
        path = fs::temp_directory_path() / fs::path(prefix) / randomSuffix();
        fs::create_directories(path);
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// Write data to a file, creating parent directories as needed
static void writeFile(fs::path const& p, std::string_view data) {
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
    BOOST_REQUIRE_MESSAGE(!ec, "Failed to create parent directories for " << p << ": " << ec.message());
    std::ofstream out(p, std::ios::binary);
    BOOST_REQUIRE_MESSAGE(out, "Failed to open file for write: " << p);
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    out.close();
    BOOST_REQUIRE_MESSAGE(fs::exists(p), "Expected file to exist after write: " << p);
}

}  // namespace

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(InterpFileTest) {
    TempDir td;

    const fs::path ver_txt = td.path / "version.txt";
    const fs::path token_txt = td.path / "secrets" / "api_token";

    writeFile(ver_txt, "1.2.3");
    writeFile(token_txt, "abc \n");

    // No "file:" prefix, no interpolation
    const std::string s3 = lsst::qserv::interpolateFile("no interpolation", td.path);
    BOOST_CHECK_EQUAL(s3, "no interpolation");

    // With "file:" prefix, relative path
    const std::string s1 = lsst::qserv::interpolateFile("file:version.txt", td.path);
    BOOST_CHECK_EQUAL(s1, "1.2.3");
    const std::string s2 = lsst::qserv::interpolateFile("file:secrets/api_token", td.path);
    BOOST_CHECK_EQUAL(s2, "abc");

    // With "file:" prefix, absolute path
    const std::string s4 = lsst::qserv::interpolateFile("file:" + ver_txt.string(), td.path);
    BOOST_CHECK_EQUAL(s4, "1.2.3");
    const std::string s5 = lsst::qserv::interpolateFile("file:" + token_txt.string(), td.path);
    BOOST_CHECK_EQUAL(s5, "abc");

    // Non-existent file
    BOOST_CHECK_THROW(lsst::qserv::interpolateFile("file:no_such_file.txt", td.path), std::runtime_error);
}

BOOST_AUTO_TEST_SUITE_END()
