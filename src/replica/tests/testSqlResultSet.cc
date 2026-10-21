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
 * @brief test the iteration over rows in the result sets of the class SqlResultSet.
 */

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "http/BinaryEncoding.h"
#include "replica/mysql/DatabaseMySQLRow.h"
#include "replica/proto/Protocol.h"
#include "replica/requests/SqlResultSet.h"

// System headers
#include <stdexcept>

// Third party headers
#include "nlohmann/json.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE SqlResultSet
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(SqlResultSetTest) {
    LOGS_INFO("SqlResultSet test begins");

    // Minimal initialization of the result set object as required
    // for testing the iteration.
    SqlResultSet::ResultSet resultSet;
    resultSet.fields.push_back(SqlResultSet::ResultSet::Field(string("a")));
    resultSet.fields.push_back(SqlResultSet::ResultSet::Field(string("b")));
    resultSet.fields.push_back(SqlResultSet::ResultSet::Field(string("c")));
    SqlResultSet::ResultSet::Row row;
    row.cells = {"abc", "12", ""};
    row.nulls = {0, 0, 1};
    resultSet.rows.push_back(row);
    row.cells = {"1.2", "0", ""};
    row.nulls = {0, 0, 0};
    resultSet.rows.push_back(row);

    // Test the iterator
    size_t rowNum = 0;
    SqlResultSet::iterate(resultSet, [&rowNum](database::mysql::Row const& row) {
        if (rowNum == 0) {
            BOOST_CHECK(!row.isNull(0));
            BOOST_CHECK(!row.isNull("a"));
            string s;
            BOOST_CHECK(row.get(0, s));
            BOOST_CHECK(s == "abc");
            s = string();
            BOOST_CHECK(row.get("a", s));
            BOOST_CHECK(s == "abc");

            BOOST_CHECK(!row.isNull(1));
            BOOST_CHECK(!row.isNull("b"));
            int i = 0;
            BOOST_CHECK(row.get(1, i));
            BOOST_CHECK(i == 12);
            i = 0;
            BOOST_CHECK(row.get("b", i));
            BOOST_CHECK(i == 12);

            BOOST_CHECK(row.isNull(2));
            BOOST_CHECK(row.isNull("c"));
            BOOST_CHECK(!row.get(2, s));
            BOOST_CHECK(!row.get("c", s));
        } else {
            BOOST_CHECK(!row.isNull(0));
            BOOST_CHECK(!row.isNull("a"));
            float f = 0.;
            BOOST_CHECK(row.get(0, f));
            BOOST_CHECK_EQUAL(f, 1.2f);
            f = 0.;
            BOOST_CHECK(row.get("a", f));
            BOOST_CHECK_EQUAL(f, 1.2f);

            BOOST_CHECK(!row.isNull(1));
            BOOST_CHECK(!row.isNull("b"));
            bool b = true;
            BOOST_CHECK(row.get(1, b));
            BOOST_CHECK(b == false);
            b = true;
            BOOST_CHECK(row.get("b", b));
            BOOST_CHECK(b == false);

            BOOST_CHECK(!row.isNull(2));
            BOOST_CHECK(!row.isNull("c"));
            string s = "123";
            BOOST_CHECK(row.get(2, s));
            BOOST_CHECK(s.empty());
            s = "123";
            BOOST_CHECK(row.get("c", s));
            BOOST_CHECK(s.empty());
        }
        rowNum++;
    });

    LOGS_INFO("SqlResultSet test ends");
}

BOOST_AUTO_TEST_CASE(SqlResultSetFromJson) {
    LOGS_INFO("SqlResultSet JSON initialization test begins");

    SqlResultSet result;
    result.queryResultSet.emplace("stale", SqlResultSet::ResultSet());

    json resultSetsJson = json::array();

    json resultSetJson = json::object();
    resultSetJson["scope"] = "db.table";
    resultSetJson["result_set"] = json::object();
    resultSetJson["result_set"]["status_ext"] = protocol::StatusExt::NONE;
    resultSetJson["result_set"]["error"] = "";
    resultSetJson["result_set"]["char_set_name"] = "utf8mb4";
    resultSetJson["result_set"]["has_result"] = 1;
    resultSetJson["result_set"]["fields"] = json::array();
    resultSetJson["result_set"]["rows"] = json::array();

    json fieldJson = json::object();
    fieldJson["name"] = "col-1";
    fieldJson["org_name"] = "LSST";
    fieldJson["table"] = "table";
    fieldJson["org_table"] = "table";
    fieldJson["db"] = "db";
    fieldJson["catalog"] = "def";
    fieldJson["def"] = "";
    fieldJson["length"] = 32;
    fieldJson["max_length"] = 4;
    fieldJson["flags"] = 0;
    fieldJson["decimals"] = 0;
    fieldJson["type"] = 253;
    resultSetJson["result_set"]["fields"].push_back(fieldJson);

    fieldJson = json::object();
    fieldJson["name"] = "col-2";
    fieldJson["org_name"] = "LSST";
    fieldJson["table"] = "table";
    fieldJson["org_table"] = "table";
    fieldJson["db"] = "db";
    fieldJson["catalog"] = "def";
    fieldJson["def"] = "";
    fieldJson["length"] = 16;
    fieldJson["max_length"] = 4;
    fieldJson["flags"] = 0;
    fieldJson["decimals"] = 0;
    fieldJson["type"] = 253;
    resultSetJson["result_set"]["fields"].push_back(fieldJson);

    json rowJson = json::object();
    rowJson["cells"] = json::array({"43", "4142"});
    rowJson["nulls"] = json::array({0, 0});
    resultSetJson["result_set"]["rows"].push_back(rowJson);
    rowJson = json::object();
    rowJson["cells"] = json::array({"44", ""});
    rowJson["nulls"] = json::array({0, 1});
    resultSetJson["result_set"]["rows"].push_back(rowJson);

    resultSetsJson.push_back(resultSetJson);

    resultSetJson = json::object();
    resultSetJson["scope"] = "db.empty";
    resultSetJson["result_set"] = json::object();
    resultSetJson["result_set"]["status_ext"] = protocol::StatusExt::MYSQL_ERROR;
    resultSetJson["result_set"]["error"] = "query failed";
    resultSetJson["result_set"]["char_set_name"] = "utf8mb4";
    resultSetJson["result_set"]["has_result"] = 0;
    resultSetJson["result_set"]["fields"] = json::array();
    resultSetJson["result_set"]["rows"] = json::array();

    resultSetsJson.push_back(resultSetJson);

    result.set(resultSetsJson, lsst::qserv::http::BinaryEncodingMode::HEX);

    BOOST_REQUIRE_EQUAL(result.queryResultSet.size(), 2U);
    BOOST_CHECK_EQUAL(result.queryResultSet.count("stale"), 0U);

    auto const& tableResult = result.queryResultSet.at("db.table");
    BOOST_CHECK(tableResult.hasResult);
    BOOST_CHECK(tableResult.extendedStatus == protocol::StatusExt::NONE);
    BOOST_CHECK(tableResult.error.empty());
    BOOST_CHECK_EQUAL(tableResult.charSetName, "utf8mb4");
    BOOST_REQUIRE_EQUAL(tableResult.fields.size(), 2U);
    BOOST_CHECK_EQUAL(tableResult.fields[0].name, "col-1");
    BOOST_CHECK_EQUAL(tableResult.fields[1].name, "col-2");
    BOOST_CHECK_EQUAL(tableResult.fields[0].orgName, "LSST");
    BOOST_CHECK_EQUAL(tableResult.fields[1].orgName, "LSST");
    BOOST_CHECK_EQUAL(tableResult.fields[0].type, 253);

    BOOST_REQUIRE_EQUAL(tableResult.rows.size(), 2U);
    auto row = tableResult.rows.cbegin();
    BOOST_CHECK_EQUAL(row->cells[0], "C");   // decoded from HEX: 43
    BOOST_CHECK_EQUAL(row->cells[1], "AB");  // decoded from HEX: 4142
    BOOST_CHECK_EQUAL(row->nulls[0], 0U);
    BOOST_CHECK_EQUAL(row->nulls[1], 0U);
    ++row;
    BOOST_CHECK_EQUAL(row->cells[0], "D");  // decoded from HEX: 44
    BOOST_CHECK_EQUAL(row->cells[1], "");
    BOOST_CHECK_EQUAL(row->nulls[0], 0U);
    BOOST_CHECK_EQUAL(row->nulls[1], 1U);

    size_t visitedRows = 0;
    SqlResultSet::iterate(tableResult, [&visitedRows](database::mysql::Row const& mysqlRow) {
        string value;
        if (visitedRows == 0) {
            BOOST_CHECK(mysqlRow.get("col-1", value));
            BOOST_CHECK_EQUAL(value, "C");
            BOOST_CHECK(mysqlRow.get("col-2", value));
            BOOST_CHECK_EQUAL(value, "AB");
        } else {
            BOOST_CHECK(mysqlRow.get("col-1", value));
            BOOST_CHECK_EQUAL(value, "D");
            BOOST_CHECK(mysqlRow.isNull("col-2"));
        }
        ++visitedRows;
    });
    BOOST_CHECK_EQUAL(visitedRows, 2U);

    auto const& errorResult = result.queryResultSet.at("db.empty");
    BOOST_CHECK(!errorResult.hasResult);
    BOOST_CHECK(errorResult.extendedStatus == protocol::StatusExt::MYSQL_ERROR);
    BOOST_CHECK_EQUAL(errorResult.error, "query failed");
    BOOST_CHECK(errorResult.fields.empty());
    BOOST_CHECK(errorResult.rows.empty());

    BOOST_CHECK(result.hasErrors());
    BOOST_CHECK_EQUAL(result.firstError(), "db.empty:MYSQL_ERROR:query failed");
    BOOST_CHECK((result.allErrors() == vector<string>{"db.empty:MYSQL_ERROR:query failed"}));

    LOGS_INFO("SqlResultSet JSON initialization test ends");
}

BOOST_AUTO_TEST_SUITE_END()