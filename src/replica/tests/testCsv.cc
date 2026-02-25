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
 * @brief test ReplicaInfo
 */

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <stdexcept>
#include <sstream>
#include <vector>

// Qserv headers
#include "replica/proto/protocol.pb.h"
#include "replica/util/Csv.h"

// Boost unit test header
#define BOOST_TEST_MODULE Csv
#include "boost/test/unit_test.hpp"

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(TestCsvDialectInput) {
    LOGS_INFO("TestCsvDialectInput test begins");
    csv::DialectInput dialectInput;
    dialectInput.fieldsTerminatedBy = "a";
    dialectInput.fieldsEnclosedBy = "b";
    dialectInput.fieldsEscapedBy = "c";
    dialectInput.linesTerminatedBy = "d";
    BOOST_REQUIRE_NO_THROW({
        unique_ptr<ProtocolDialectInput> const ptr = dialectInput.toProto();
        BOOST_CHECK_EQUAL(dialectInput.fieldsTerminatedBy, ptr->fields_terminated_by());
        BOOST_CHECK_EQUAL(dialectInput.fieldsEnclosedBy, ptr->fields_enclosed_by());
        BOOST_CHECK_EQUAL(dialectInput.fieldsEscapedBy, ptr->fields_escaped_by());
        BOOST_CHECK_EQUAL(dialectInput.linesTerminatedBy, ptr->lines_terminated_by());
        csv::DialectInput const dialectInputToo(*(ptr.get()));
        BOOST_CHECK_EQUAL(dialectInput.fieldsTerminatedBy, dialectInputToo.fieldsTerminatedBy);
        BOOST_CHECK_EQUAL(dialectInput.fieldsEnclosedBy, dialectInputToo.fieldsEnclosedBy);
        BOOST_CHECK_EQUAL(dialectInput.fieldsEscapedBy, dialectInputToo.fieldsEscapedBy);
        BOOST_CHECK_EQUAL(dialectInput.linesTerminatedBy, dialectInputToo.linesTerminatedBy);
    });
    LOGS_INFO("TestCsvDialectInput test ends");
}

BOOST_AUTO_TEST_CASE(TestCsvDialect) {
    LOGS_INFO("TestCsvDialect test begins");
    BOOST_REQUIRE_NO_THROW({
        csv::Dialect const dialect;
        csv::Dialect const dialectToo = dialect;
        BOOST_CHECK_EQUAL(dialect.fieldsTerminatedBy(), '\t');
        BOOST_CHECK_EQUAL(dialect.fieldsEnclosedBy(), '\0');
        BOOST_CHECK_EQUAL(dialect.fieldsEscapedBy(), '\\');
        BOOST_CHECK_EQUAL(dialect.linesTerminatedBy(), '\n');
        BOOST_CHECK(!dialect.sqlOptions().empty());
    });
    BOOST_REQUIRE_THROW(
            {
                csv::DialectInput emptyDialectInput;
                emptyDialectInput.fieldsTerminatedBy.clear();
                emptyDialectInput.fieldsEnclosedBy.clear();
                emptyDialectInput.fieldsEscapedBy.clear();
                emptyDialectInput.linesTerminatedBy.clear();
                csv::Dialect const dialect(emptyDialectInput);
            },
            std::invalid_argument);
    LOGS_INFO("TestCsvDialect test ends");
}

BOOST_AUTO_TEST_CASE(TestCsvParser) {
    LOGS_INFO("TestCsvParser test begins");
    csv::Dialect const dialect;
    csv::Parser parser(dialect);
    vector<string> const in = {
            "Line 1\nLine 2\nNon-terminated line ",
            "3\nLine 4\nNon-terminated line 5",
            "\nLine 6\nLine 7 ends with the escaped terminator \\\n",
            "\n\n\n\n\n",
            "Line 8 has escaped terminator \\\n in the middle\n"
            "\\\n\\\n",
            "Line 9 starts with 2 escaped terminators and ends with 1 escaped terminator\\",
            "\n\nLine 10",
            "\nLine 11 has escaped escape followed by the non-escaped terminator in the end\\\\\n",
            "Line 12",
    };
    vector<string> lines;
    for (size_t i = 0, size = in.size(); i < size; ++i) {
        bool const flush = (i == size - 1);
        parser.parse(in[i].data(), in[i].size(), flush,
                     [&lines](char const* out, size_t size) { lines.emplace_back(string(out, size)); });
    }
    for (auto const& line : lines) {
        LOGS_INFO("TestCsv: " + line);
    }
    BOOST_CHECK_EQUAL(parser.numLines(), 16ULL);
    BOOST_CHECK_EQUAL(parser.numLines(), lines.size());
    BOOST_CHECK_EQUAL(lines[0], string("Line 1\n"));
    BOOST_CHECK_EQUAL(lines[1], string("Line 2\n"));
    BOOST_CHECK_EQUAL(lines[2], string("Non-terminated line 3\n"));
    BOOST_CHECK_EQUAL(lines[3], string("Line 4\n"));
    BOOST_CHECK_EQUAL(lines[4], string("Non-terminated line 5\n"));
    BOOST_CHECK_EQUAL(lines[5], string("Line 6\n"));
    BOOST_CHECK_EQUAL(lines[6], string("Line 7 ends with the escaped terminator \\\n\n"));
    BOOST_CHECK_EQUAL(lines[7], string("\n"));
    BOOST_CHECK_EQUAL(lines[8], string("\n"));
    BOOST_CHECK_EQUAL(lines[9], string("\n"));
    BOOST_CHECK_EQUAL(lines[10], string("\n"));
    BOOST_CHECK_EQUAL(lines[11], string("Line 8 has escaped terminator \\\n in the middle\n"));
    BOOST_CHECK_EQUAL(lines[12], string("\\\n\\\nLine 9 starts with 2 escaped terminators and ends with 1 "
                                        "escaped terminator\\\n\n"));
    BOOST_CHECK_EQUAL(lines[13], string("Line 10\n"));
    BOOST_CHECK_EQUAL(
            lines[14],
            string("Line 11 has escaped escape followed by the non-escaped terminator in the end\\\\\n"));
    BOOST_CHECK_EQUAL(lines[15], string("Line 12"));
    LOGS_INFO("TestCsvParser test ends");
}

// Test with the following dialect parameters:
// - enclosure character is the null character
// - tab is the field separator
BOOST_AUTO_TEST_CASE(TestCsvRowParser) {
    LOGS_INFO("TestCsvRowParser test begins");
    csv::Dialect const dialect;
    csv::RowParser rowParser(dialect);
    vector<string> const in = {
            "Field 1\tField 2\tField 3",
            "Field 4\tField 5 with \"escaped\" enclosure\tField 6",
            "Field 7\tField 8 with escaped terminator \\ \tField 9",
    };
    vector<vector<string>> rows;
    for (auto const& row : in) {
        vector<string> fields;
        rowParser.parse(row.data(), row.size(),
                        [&fields](char const* out, size_t size) { fields.emplace_back(string(out, size)); });
        rows.emplace_back(move(fields));
    }
    for (auto const& row : rows) {
        LOGS_INFO("TestCsvRowParser: " + to_string(row.size()) + " fields");
        for (auto const& field : row) {
            LOGS_INFO("TestCsvRowParser: " + field);
        }
    }
    BOOST_CHECK_EQUAL(rows.size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[0].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[0][0], string("Field 1"));
    BOOST_CHECK_EQUAL(rows[0][1], string("Field 2"));
    BOOST_CHECK_EQUAL(rows[0][2], string("Field 3"));
    BOOST_CHECK_EQUAL(rows[1].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[1][0], string("Field 4"));
    BOOST_CHECK_EQUAL(rows[1][1], string("Field 5 with \"escaped\" enclosure"));
    BOOST_CHECK_EQUAL(rows[1][2], string("Field 6"));
    BOOST_CHECK_EQUAL(rows[2].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[2][0], string("Field 7"));
    BOOST_CHECK_EQUAL(rows[2][1], string("Field 8 with escaped terminator \\ "));
    BOOST_CHECK_EQUAL(rows[2][2], string("Field 9"));
    LOGS_INFO("TestCsvRowParser test ends");
}

// Test with the following dialect parameters:
// - enclosure character is a double quote
// - field separator is a comma
BOOST_AUTO_TEST_CASE(TestCsvRowParser1) {
    LOGS_INFO("TestCsvRowParser1 test begins");
    csv::DialectInput dialectInput;
    dialectInput.fieldsTerminatedBy = ",";
    dialectInput.fieldsEnclosedBy = "\"";
    csv::Dialect const dialect(dialectInput);
    csv::RowParser rowParser(dialect);
    vector<string> const in = {
            "\"Field 1\",\"Field 2\",\"Field 3\"",
            "\"Field 4\",\"Field 5 with \\\"escaped\\\" enclosure\",\"Field 6\"",
            "\"Field 7\",\"Field 8 with escaped terminator \\ \",\"Field 9\"",
    };
    vector<vector<string>> rows;
    for (auto const& row : in) {
        vector<string> fields;
        rowParser.parse(row.data(), row.size(),
                        [&fields](char const* out, size_t size) { fields.emplace_back(string(out, size)); });
        rows.emplace_back(move(fields));
    }
    for (auto const& row : rows) {
        LOGS_INFO("TestCsvRowParser1: " + to_string(row.size()) + " fields");
        for (auto const& field : row) {
            LOGS_INFO("TestCsvRowParser1: " + field);
        }
    }
    BOOST_CHECK_EQUAL(rows.size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[0].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[0][0], string("Field 1"));
    BOOST_CHECK_EQUAL(rows[0][1], string("Field 2"));
    BOOST_CHECK_EQUAL(rows[0][2], string("Field 3"));
    BOOST_CHECK_EQUAL(rows[1].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[1][0], string("Field 4"));
    BOOST_CHECK_EQUAL(rows[1][1], string("Field 5 with \\\"escaped\\\" enclosure"));
    BOOST_CHECK_EQUAL(rows[1][2], string("Field 6"));
    BOOST_CHECK_EQUAL(rows[2].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[2][0], string("Field 7"));
    BOOST_CHECK_EQUAL(rows[2][1], string("Field 8 with escaped terminator \\ "));
    BOOST_CHECK_EQUAL(rows[2][2], string("Field 9"));
    LOGS_INFO("TestCsvRowParser1 test ends");
}

// Test with the following dialect parameters:
// - enclosure character is a single quote
// - field separator is a comma
// Also note that the second row has the unquoted numeric value in the first field and
// a null value represented as \N in the last field. MySQL allows unquoted fields in the input CSV
// as long as they don't contain special characters (enclosure, escape, field terminator, line terminator).
// The test checks that the parser correctly handles such cases.
BOOST_AUTO_TEST_CASE(TestCsvRowParser2) {
    LOGS_INFO("TestCsvRowParser2 test begins");
    csv::DialectInput dialectInput;
    dialectInput.fieldsTerminatedBy = ",";
    dialectInput.fieldsEnclosedBy = "'";
    csv::Dialect const dialect(dialectInput);
    csv::RowParser rowParser(dialect);
    vector<string> const in = {
            "'Field 1','Field 2','Field 3'",
            "'Field 4','Field 5 with \\'escaped\\' enclosure','Field 6'",
            "1234,'Field 8 with escaped terminator \\ ',\\N",
    };
    vector<vector<string>> rows;
    for (auto const& row : in) {
        vector<string> fields;
        rowParser.parse(row.data(), row.size(),
                        [&fields](char const* out, size_t size) { fields.emplace_back(string(out, size)); });
        rows.emplace_back(move(fields));
    }
    for (auto const& row : rows) {
        LOGS_INFO("TestCsvRowParser2: " + to_string(row.size()) + " fields");
        for (auto const& field : row) {
            LOGS_INFO("TestCsvRowParser2: " + field);
        }
    }
    BOOST_CHECK_EQUAL(rows.size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[0].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[0][0], string("Field 1"));
    BOOST_CHECK_EQUAL(rows[0][1], string("Field 2"));
    BOOST_CHECK_EQUAL(rows[0][2], string("Field 3"));
    BOOST_CHECK_EQUAL(rows[1].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[1][0], string("Field 4"));
    BOOST_CHECK_EQUAL(rows[1][1], string("Field 5 with \\'escaped\\' enclosure"));
    BOOST_CHECK_EQUAL(rows[1][2], string("Field 6"));
    BOOST_CHECK_EQUAL(rows[2].size(), 3ULL);
    BOOST_CHECK_EQUAL(rows[2][0], string("1234"));
    BOOST_CHECK_EQUAL(rows[2][1], string("Field 8 with escaped terminator \\ "));
    BOOST_CHECK_EQUAL(rows[2][2], string("\\N"));
    LOGS_INFO("TestCsvRowParser2 test ends");
}

// Test that enclosure characters appearing in the middle of fields (not at the start)
// are treated as regular characters and do not trigger field content stripping.
// Also tests malformed input where the opening enclosure is missing its closing pair.
BOOST_AUTO_TEST_CASE(TestCsvRowParserEnclosureEdgeCases) {
    LOGS_INFO("TestCsvRowParserEnclosureEdgeCases test begins");
    csv::DialectInput dialectInput;
    dialectInput.fieldsTerminatedBy = ",";
    dialectInput.fieldsEnclosedBy = "\"";
    csv::Dialect const dialect(dialectInput);
    csv::RowParser rowParser(dialect);

    // Fields with the enclosure character in the middle (not at the start): must be treated as plain text.
    // Fields with the enclosure character at the start but not the end: malformed, parsed best-effort.
    vector<string> const in = {
            // Enclosure in the middle of a field: should be treated as a regular character.
            "hello\"world\",\"normal\"",
            // Malformed last field: only an opening enclosure with no closing pair.
            "normal,\"",
    };
    vector<vector<string>> rows;
    for (auto const& row : in) {
        vector<string> fields;
        rowParser.parse(row.data(), row.size(),
                        [&fields](char const* out, size_t size) { fields.emplace_back(string(out, size)); });
        rows.emplace_back(move(fields));
    }
    // Row 0: hello"world" has enclosure in the middle (not at the start), so the quotes are treated
    // as regular characters. The second field "normal" is properly enclosed.
    BOOST_CHECK_EQUAL(rows[0].size(), 2ULL);
    BOOST_CHECK_EQUAL(rows[0][0], string("hello\"world\""));
    BOOST_CHECK_EQUAL(rows[0][1], string("normal"));
    // Row 1: first field "normal" is plain text. The last field consists of only an opening enclosure
    // with no closing pair (malformed), which is reported as an empty string without undefined behavior.
    BOOST_CHECK_EQUAL(rows[1].size(), 2ULL);
    BOOST_CHECK_EQUAL(rows[1][0], string("normal"));
    BOOST_CHECK_EQUAL(rows[1][1], string(""));
    LOGS_INFO("TestCsvRowParserEnclosureEdgeCases test ends");
}

BOOST_AUTO_TEST_SUITE_END()
