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
#include "replica/DatabaseMySQLTypes.h"
#include "replica/FileUtils.h"

// Boost unit test header
#define BOOST_TEST_MODULE ConnectionParamsTest
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ConnectionParamsTest) {
    LOGS_INFO("ConnectionParamsTest test begins");

    bool const showPassword = true;

    database::mysql::ConnectionParams const defaultConstructed;
    BOOST_CHECK(defaultConstructed.host == "localhost");
    BOOST_CHECK(defaultConstructed.port == 3306);
    BOOST_CHECK(defaultConstructed.user == FileUtils::getEffectiveUser());
    BOOST_CHECK(defaultConstructed.password == "");
    BOOST_CHECK(defaultConstructed.database == "");
    BOOST_CHECK(defaultConstructed.toString() ==
                "mysql://" + FileUtils::getEffectiveUser() + ":xxxxxx@localhost:3306/");
    BOOST_CHECK(defaultConstructed.toString(showPassword) ==
                "mysql://" + FileUtils::getEffectiveUser() + ":@localhost:3306/");

    string const host = "Host-A";
    uint16_t const port = 23306;
    string const user = "qserv";
    string const password = "CHANGEME";
    string const database = "test";
    database::mysql::ConnectionParams const normallyConstructed(host, port, user, password, database);
    BOOST_CHECK(normallyConstructed.host == host);
    BOOST_CHECK(normallyConstructed.port == port);
    BOOST_CHECK(normallyConstructed.user == user);
    BOOST_CHECK(normallyConstructed.password == password);
    BOOST_CHECK(normallyConstructed.database == database);
    BOOST_CHECK(normallyConstructed.toString() ==
                "mysql://" + user + ":xxxxxx@" + host + ":" + to_string(port) + "/" + database);
    BOOST_CHECK(normallyConstructed.toString(showPassword) ==
                "mysql://" + user + ":" + password + "@" + host + ":" + to_string(port) + "/" + database);

    database::mysql::ConnectionParams const copyConstructed(normallyConstructed);
    BOOST_CHECK(copyConstructed == normallyConstructed);
    BOOST_CHECK(not(copyConstructed != normallyConstructed));
    BOOST_CHECK(normallyConstructed == copyConstructed);

    database::mysql::ConnectionParams assigned;
    assigned = normallyConstructed;
    BOOST_CHECK(assigned == normallyConstructed);

    // The minimal connection string. Only the name of a database is
    // required. The rest is filled with the default values passed into
    // the constructor. These default values will be used through the rest
    // of the parser tests.

    string const defaultHost = "Host-A";
    uint16_t const defaultPort = 23306;
    string const defaultUser = "qserv";
    string const defaultPassword = "CHANGEME";

    database::mysql::ConnectionParams parsed;
    BOOST_REQUIRE_NO_THROW({
        string const conn = "mysql://@/test";
        LOGS_INFO("ConnectionParamsTest  input:  '" << conn << "'");
        try {
            parsed = database::mysql::ConnectionParams::parse(conn, defaultHost, defaultPort, defaultUser,
                                                              defaultPassword);
        } catch (exception const& ex) {
            LOGS_INFO("ConnectionParamsTest  unexpected exception: " << ex.what());
            throw;
        }
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString() << "'");
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString(showPassword) << "'");
        BOOST_CHECK(parsed.host == defaultHost);
        BOOST_CHECK(parsed.port == defaultPort);
        BOOST_CHECK(parsed.user == defaultUser);
        BOOST_CHECK(parsed.password == defaultPassword);
        BOOST_CHECK(parsed.database == "test");
        BOOST_CHECK(parsed.toString() == "mysql://" + defaultUser + ":xxxxxx@" + defaultHost + ":" +
                                                 to_string(defaultPort) + "/test");
        BOOST_CHECK(parsed.toString(showPassword) == "mysql://" + defaultUser + ":" + defaultPassword + "@" +
                                                             defaultHost + ":" + to_string(defaultPort) +
                                                             "/test");
    });

    // Similar to the previous one except spaces added at both ends of
    // the string

    database::mysql::ConnectionParams equallyParsed;
    BOOST_REQUIRE_NO_THROW({
        string const conn = "  mysql://@/test ";
        LOGS_INFO("ConnectionParamsTest  input:  '" << conn << "'");
        try {
            equallyParsed = database::mysql::ConnectionParams::parse(conn, defaultHost, defaultPort,
                                                                     defaultUser, defaultPassword);
        } catch (exception const& ex) {
            LOGS_INFO("ConnectionParamsTest  unexpected exception: " << ex.what());
            throw;
        }
        LOGS_INFO("ConnectionParamsTest  parsed: '" << equallyParsed.toString() << "'");
        LOGS_INFO("ConnectionParamsTest  parsed: '" << equallyParsed.toString(showPassword) << "'");
        BOOST_CHECK(equallyParsed.host == defaultHost);
        BOOST_CHECK(equallyParsed.port == defaultPort);
        BOOST_CHECK(equallyParsed.user == defaultUser);
        BOOST_CHECK(equallyParsed.password == defaultPassword);
        BOOST_CHECK(equallyParsed.database == "test");
        BOOST_CHECK(equallyParsed.toString() == "mysql://" + defaultUser + ":xxxxxx@" + defaultHost + ":" +
                                                        to_string(defaultPort) + "/test");
        BOOST_CHECK(equallyParsed.toString(showPassword) == "mysql://" + defaultUser + ":" + defaultPassword +
                                                                    "@" + defaultHost + ":" +
                                                                    to_string(defaultPort) + "/test");
    });
    BOOST_CHECK(equallyParsed == parsed);

    // Parsing a connection string with missing components

    BOOST_REQUIRE_NO_THROW({
        // Missing password
        string const conn = "  mysql://qsreplica@Host-B:13306/test ";
        LOGS_INFO("ConnectionParamsTest  input:  '" << conn << "'");
        database::mysql::ConnectionParams parsed;
        try {
            parsed = database::mysql::ConnectionParams::parse(conn, defaultHost, defaultPort, defaultUser,
                                                              defaultPassword);
        } catch (exception const& ex) {
            LOGS_INFO("ConnectionParamsTest  unexpected exception: " << ex.what());
            throw;
        }
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString() << "'");
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString(showPassword) << "'");
        BOOST_CHECK(parsed.host == "Host-B");
        BOOST_CHECK(parsed.port == 13306);
        BOOST_CHECK(parsed.user == "qsreplica");
        BOOST_CHECK(parsed.password == defaultPassword);
        BOOST_CHECK(parsed.database == "test");
        BOOST_CHECK(parsed.toString() == "mysql://qsreplica:xxxxxx@Host-B:13306/test");
        BOOST_CHECK(parsed.toString(showPassword) ==
                    "mysql://qsreplica:" + defaultPassword + "@Host-B:13306/test");
    });
    BOOST_REQUIRE_NO_THROW({
        // Missing port
        string const conn = "  mysql://qsreplica:CHANGEMETOO@Host-B/test ";
        LOGS_INFO("ConnectionParamsTest  input:  '" << conn << "'");
        database::mysql::ConnectionParams parsed;
        try {
            parsed = database::mysql::ConnectionParams::parse(conn, defaultHost, defaultPort, defaultUser,
                                                              defaultPassword);
        } catch (exception const& ex) {
            LOGS_INFO("ConnectionParamsTest  unexpected exception: " << ex.what());
            throw;
        }
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString() << "'");
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString(showPassword) << "'");
        BOOST_CHECK(parsed.host == "Host-B");
        BOOST_CHECK(parsed.port == defaultPort);
        BOOST_CHECK(parsed.user == "qsreplica");
        BOOST_CHECK(parsed.password == "CHANGEMETOO");
        BOOST_CHECK(parsed.database == "test");
        BOOST_CHECK(parsed.toString() ==
                    "mysql://qsreplica:xxxxxx@Host-B:" + to_string(defaultPort) + "/test");
        BOOST_CHECK(parsed.toString(showPassword) ==
                    "mysql://qsreplica:CHANGEMETOO@Host-B:" + to_string(defaultPort) + "/test");
    });
    BOOST_REQUIRE_NO_THROW({
        // Missing user
        string const conn = "  mysql://:CHANGEMETOO@Host-B:13306/test ";
        LOGS_INFO("ConnectionParamsTest  input:  '" << conn << "'");
        database::mysql::ConnectionParams parsed;
        try {
            parsed = database::mysql::ConnectionParams::parse(conn, defaultHost, defaultPort, defaultUser,
                                                              defaultPassword);
        } catch (exception const& ex) {
            LOGS_INFO("ConnectionParamsTest  unexpected exception: " << ex.what());
            throw;
        }
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString() << "'");
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString(showPassword) << "'");
        BOOST_CHECK(parsed.host == "Host-B");
        BOOST_CHECK(parsed.port == 13306);
        BOOST_CHECK(parsed.user == defaultUser);
        BOOST_CHECK(parsed.password == "CHANGEMETOO");
        BOOST_CHECK(parsed.database == "test");
        BOOST_CHECK(parsed.toString() == "mysql://" + defaultUser + ":xxxxxx@Host-B:13306/test");
        BOOST_CHECK(parsed.toString(showPassword) ==
                    "mysql://" + defaultUser + ":CHANGEMETOO@Host-B:13306/test");
    });
    BOOST_REQUIRE_NO_THROW({
        // Missing user & password
        string const conn = "  mysql://@Host-B:13306/test ";
        LOGS_INFO("ConnectionParamsTest  input:  '" << conn << "'");
        database::mysql::ConnectionParams parsed;
        try {
            parsed = database::mysql::ConnectionParams::parse(conn, defaultHost, defaultPort, defaultUser,
                                                              defaultPassword);
        } catch (exception const& ex) {
            LOGS_INFO("ConnectionParamsTest  unexpected exception: " << ex.what());
            throw;
        }
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString() << "'");
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString(showPassword) << "'");
        BOOST_CHECK(parsed.host == "Host-B");
        BOOST_CHECK(parsed.port == 13306);
        BOOST_CHECK(parsed.user == defaultUser);
        BOOST_CHECK(parsed.password == defaultPassword);
        BOOST_CHECK(parsed.database == "test");
        BOOST_CHECK(parsed.toString() == "mysql://" + defaultUser + ":xxxxxx@Host-B:13306/test");
        BOOST_CHECK(parsed.toString(showPassword) ==
                    "mysql://" + defaultUser + ":" + defaultPassword + "@Host-B:13306/test");
    });

    // Parsing a connection string with all components provided

    BOOST_REQUIRE_NO_THROW({
        string const conn = "  mysql://qsreplica:CHANGEMETOO@Host-B:13306/test ";
        LOGS_INFO("ConnectionParamsTest  input:  '" << conn << "'");
        database::mysql::ConnectionParams parsed;
        try {
            parsed = database::mysql::ConnectionParams::parse(conn, defaultHost, defaultPort, defaultUser,
                                                              defaultPassword);
        } catch (exception const& ex) {
            LOGS_INFO("ConnectionParamsTest  unexpected exception: " << ex.what());
            throw;
        }
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString() << "'");
        LOGS_INFO("ConnectionParamsTest  parsed: '" << parsed.toString(showPassword) << "'");
        BOOST_CHECK(parsed.host == "Host-B");
        BOOST_CHECK(parsed.port == 13306);
        BOOST_CHECK(parsed.user == "qsreplica");
        BOOST_CHECK(parsed.password == "CHANGEMETOO");
        BOOST_CHECK(parsed.database == "test");
        BOOST_CHECK(parsed.toString() == "mysql://qsreplica:xxxxxx@Host-B:13306/test");
        BOOST_CHECK(parsed.toString(showPassword) == "mysql://qsreplica:CHANGEMETOO@Host-B:13306/test");
    });

    // Test exception throwing if the database name is missing in
    // a connection string. Note that exceptions are intercepted and
    // thrown again to improve the error reporting (what causes
    // the exceptions).

    BOOST_CHECK_THROW(
            {
                string const conn = "mysql://@";
                LOGS_INFO("ConnectionParamsTest  input:  '" << conn << "'");
                database::mysql::ConnectionParams parsed;
                try {
                    parsed = database::mysql::ConnectionParams::parse(conn, defaultHost, defaultPort,
                                                                      defaultUser, defaultPassword);
                } catch (exception const& ex) {
                    LOGS_INFO("ConnectionParamsTest  expected exception: " << ex.what());
                    throw;
                }
            },
            invalid_argument);

    LOGS_INFO("ConnectionParamsTest test ends");
}

BOOST_AUTO_TEST_SUITE_END()
