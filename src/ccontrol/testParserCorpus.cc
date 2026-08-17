// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2026 LSST.
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
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#define BOOST_TEST_MODULE ParserCorpus

// Third-party headers
#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>

// Qserv headers
#include "ccontrol/ParseRunner.h"

using namespace std;
namespace fs = std::filesystem;
using namespace lsst::qserv;

namespace {

string readFile(fs::path const& path) {
    ifstream input(path);
    BOOST_REQUIRE_MESSAGE(input.is_open(), "Failed to open file: " << path);
    ostringstream content;
    content << input.rdbuf();
    return content.str();
}

}  // namespace

BOOST_AUTO_TEST_SUITE(Suite)

// This test case reads files from QSERV_PARSER_CORPUS_DIR, runs them through the parser,
// and converts them to Qserv IR. It serves as both a stress test for the parser/adapter
// and also will highlight any performance regressions in either.
BOOST_AUTO_TEST_CASE(parseCorpus) {
    auto corpusDir = QSERV_PARSER_CORPUS_DIR;
    BOOST_REQUIRE_MESSAGE(fs::exists(corpusDir), "Parser corpus directory does not exist: " << corpusDir);
    BOOST_REQUIRE_MESSAGE(fs::is_directory(corpusDir), "Parser corpus path not a directory: " << corpusDir);

    unsigned int corpusSize = 0;
    auto const start = chrono::steady_clock::now();
    for (auto const& entry : fs::recursive_directory_iterator(corpusDir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".sql") {
            ++corpusSize;
            cout << entry.path() << "\n";
            BOOST_TEST_CONTEXT("file=" << entry.path()) {
                auto testSql = readFile(entry.path());
                auto selectStmt = ccontrol::ParseRunner::makeSelectStmt(testSql);
                BOOST_REQUIRE_MESSAGE(selectStmt != nullptr,
                                      "Test produced null Qserv IR: " << entry.path() << " -> " << testSql);
            }
        }
    }

    auto const stop = chrono::steady_clock::now();
    auto const elapsed = chrono::duration<double>(stop - start).count();
    cout << "testParserCorpus: parsed " << corpusSize << " statement(s) in " << elapsed << "s\n";
}

BOOST_AUTO_TEST_SUITE_END()
