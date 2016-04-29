// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @file
  *
  * @brief Unit test for the MySql implementation of the Common State System Interface.
  *
  *
  * This is a unittest for the KvInterfaceImplMySql class, geared for testing remote server connections.
  *
  * The test requires ~/.lsst/KvInterfaceImplMySql-testRemote.txt config file with the following:
  * [mysql]
  * user     = <username>
  * passwd = <passwd> # this is optional
  * host     = <host>
  * port     = <port>
  *
  * It is sufficient if the user has normal privileges.
  *
  * @Author Nathan Pease, SLAC
  *
  */

// System headers
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>

// Boost headers
#include <boost/algorithm/string/replace.hpp>
#include "boost/property_tree/ini_parser.hpp"
#include "boost/property_tree/ptree.hpp"

// Boost unit test header
#define BOOST_TEST_MODULE Css_1
#include "boost/test/included/unit_test.hpp"

// Qserv headers
#include "css/CssError.h"
#include "css/KvInterfaceImplMySql.h"
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"

using lsst::qserv::css::KvInterfaceImplMySql;
using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::sql::SqlConnection;
using lsst::qserv::sql::SqlErrorObject;

namespace {


// todo this is copied directly from testQMeta.cc. Perhaps it could be made generic and put in a shared location?
struct TestDBGuard {
    TestDBGuard()
    : connected(false) {
        boost::property_tree::ptree pt;
        std::string iniFileLoc = std::getenv("HOME") + std::string("/.lsst/KvInterfaceImplMySql-testRemote.txt");
        boost::property_tree::ini_parser::read_ini(iniFileLoc, pt);
        std::cout << "iniFileLoc:" << iniFileLoc << std::endl;
        sqlConfig.hostname = pt.get<std::string>("mysql.host");
        sqlConfig.port = pt.get<unsigned int>("mysql.port");
        sqlConfig.username = pt.get<std::string>("mysql.user");
        bool gotPassword(false);
        try {
            sqlConfig.password = pt.get<std::string>("mysql.passwd");
            gotPassword = true;
        } catch (boost::property_tree::ptree_bad_path& ex) {
        } catch (boost::property_tree::ptree_bad_data& ex) {
        }

        if (not gotPassword) {
            std::cout << "enter password:" << std::flush;
            std::cin >> sqlConfig.password;
        }

        sqlConfig.dbName = "testCSSZ012sdrt";

        std::ifstream schemaFile("admin/templates/configuration/tmp/configure/sql/CssData.sql");

        // read whole file into buffer
        std::string buffer;
        std::getline(schemaFile, buffer, '\0');

        // replace production schema name with test schema
        boost::replace_all(buffer, "qservCssData", sqlConfig.dbName);

        // need config without database name
        MySqlConfig sqlConfigLocal = sqlConfig;
        sqlConfigLocal.dbName = "";
        std::cout << "config:" << sqlConfigLocal.toString() << std::endl;
        SqlConnection sqlConn(sqlConfigLocal);

        SqlErrorObject errObj;
        sqlConn.runQuery(buffer, errObj);
        if (not errObj.isSet()) {
           connected = true;
        }
    }

    ~TestDBGuard() {
        SqlConnection sqlConn(sqlConfig);
        SqlErrorObject errObj;
        sqlConn.dropDb(sqlConfig.dbName, errObj);
    }

    MySqlConfig sqlConfig;
    bool connected;
};

} // namespace

struct PerTestFixture
{
    PerTestFixture() {
        kvInterface = std::make_shared<KvInterfaceImplMySql>(testDB.sqlConfig);
        sqlConn = std::make_shared<SqlConnection>(testDB.sqlConfig);
    }

    bool isConnected() const { return testDB.connected; }

    static TestDBGuard testDB;
    std::shared_ptr<SqlConnection> sqlConn;
    std::shared_ptr<KvInterfaceImplMySql> kvInterface;
};

TestDBGuard PerTestFixture::testDB;


#define CHECK_CONNECTION() if (not isConnected()) { BOOST_WARN_MESSAGE(false, "Not connected, can not run test case."); return; }


BOOST_FIXTURE_TEST_SUITE(SqlKVInterfaceConnectionTestSuite, PerTestFixture)

BOOST_AUTO_TEST_CASE(EmptyDBGet) {
    CHECK_CONNECTION();

    std::string key;
    BOOST_CHECK_THROW(kvInterface->get("/"), lsst::qserv::css::NoSuchKey);
    BOOST_CHECK_THROW(kvInterface->get("/Get"), lsst::qserv::css::NoSuchKey);
    BOOST_CHECK_THROW(kvInterface->getChildren("/"), lsst::qserv::css::NoSuchKey);
}

BOOST_AUTO_TEST_CASE(CreateAndGetKV) {
    CHECK_CONNECTION();

    std::string key;
    BOOST_CHECK_NO_THROW(key = kvInterface->create("/CreateAndGetKV/testKey", "testValue"));
    BOOST_CHECK_EQUAL(key, "/CreateAndGetKV/testKey");
    BOOST_CHECK_EQUAL(kvInterface->get("/CreateAndGetKV/testKey"), "testValue");
}


BOOST_AUTO_TEST_CASE(CreateUnique) {
    CHECK_CONNECTION();

    std::string pfx = "/CreateAndGetKV/uniqueKey_";
    std::string key;

    BOOST_CHECK_NO_THROW(key = kvInterface->create(pfx, "uniqueValue1", true));
    BOOST_CHECK_EQUAL(key, pfx+"0000000001");
    BOOST_CHECK_EQUAL(kvInterface->get(key), "uniqueValue1");

    // try to confuse it by adding non-numeric keys
    BOOST_CHECK_NO_THROW(key = kvInterface->create(pfx+"01234567ab", ""));
    BOOST_CHECK_NO_THROW(key = kvInterface->create(pfx+"abcdefghij", ""));

    for (int i = 0; i != 10; ++ i) {
        std::ostringstream str;
        str << pfx << std::setfill('0') << std::setw(10) << i+2;
        BOOST_CHECK_NO_THROW(key = kvInterface->create(pfx, "", true));
        BOOST_CHECK_EQUAL(key, str.str());
    }

    // this should reset unique to higher value
    BOOST_CHECK_NO_THROW(key = kvInterface->create(pfx+"0000001234", ""));
    BOOST_CHECK_NO_THROW(key = kvInterface->create(pfx, "", true));
    BOOST_CHECK_EQUAL(key, pfx+"0000001235");
}


BOOST_AUTO_TEST_CASE(GetRootChildren) {
    std::vector<std::string> children;
    children = kvInterface->getChildren("/");
    BOOST_REQUIRE(children.size() == 1U);
    BOOST_CHECK_EQUAL(children[0], "CreateAndGetKV");
}

BOOST_AUTO_TEST_CASE(SetAndGetChildren) {
    CHECK_CONNECTION();

    // create the required parent object:
    kvInterface->create("/SetAndGetChildren", "");
    // then create children to use in test:
    kvInterface->create("/SetAndGetChildren/child0", "abc");
    kvInterface->create("/SetAndGetChildren/child1", "123");
    kvInterface->create("/SetAndGetChildren/child2", "!@#");
    std::vector<std::string> children;
    BOOST_REQUIRE_NO_THROW(children = kvInterface->getChildren("/SetAndGetChildren"));
    BOOST_REQUIRE(children.size() == 3U);
    std::sort(children.begin(), children.end());
    BOOST_CHECK_EQUAL(children[0], "child0");
    BOOST_CHECK_EQUAL(children[1], "child1");
    BOOST_CHECK_EQUAL(children[2], "child2");
}


BOOST_AUTO_TEST_CASE(Get) {
    CHECK_CONNECTION();

    BOOST_CHECK_THROW(kvInterface->get("/Get"), lsst::qserv::css::NoSuchKey);
    BOOST_CHECK_EQUAL(kvInterface->get("/Get", "my default value"), "my default value");
}


BOOST_AUTO_TEST_CASE(GetChildrenForParentThatDoesNotExist) {
    CHECK_CONNECTION();

    std::vector<std::string> children;
    BOOST_CHECK_THROW(children = kvInterface->getChildren("/GetChildrenForParentThatDoesNotExist"), lsst::qserv::css::NoSuchKey);
    BOOST_CHECK_EQUAL(children.size(), 0U);
}


BOOST_AUTO_TEST_CASE(CreateDuplicateKV) {
    CHECK_CONNECTION();

    BOOST_REQUIRE_NO_THROW(kvInterface->create("/CreateDuplicateKV", "a value"));
    // verify that adding a key a second time does not throw
    BOOST_CHECK_THROW(kvInterface->create("/CreateDuplicateKV", "another value"), lsst::qserv::css::KeyExistsError);
}


BOOST_AUTO_TEST_CASE(Exists) {
    CHECK_CONNECTION();

    // new key, should not exist:
    BOOST_REQUIRE_EQUAL(kvInterface->exists("/Exists"), false);
    // adding the key should work:
    BOOST_REQUIRE_NO_THROW(kvInterface->create("/Exists", "new value"));
    // now the key should exist:
    BOOST_REQUIRE_EQUAL(kvInterface->exists("/Exists"), true);
}


BOOST_AUTO_TEST_CASE(Delete) {
    CHECK_CONNECTION();

    BOOST_REQUIRE_NO_THROW(kvInterface->create("/Delete", "a value"));
    BOOST_REQUIRE_NO_THROW(kvInterface->deleteKey("/Delete"));
    BOOST_REQUIRE_THROW(kvInterface->deleteKey("/Delete"), lsst::qserv::css::NoSuchKey);
}


BOOST_AUTO_TEST_CASE(RecursiveAddAndDelete) {
    CHECK_CONNECTION();

    // note that 'child' gets added automatically
    BOOST_REQUIRE_NO_THROW(kvInterface->create("/RecursiveDelete/child/a", "a"));
    BOOST_REQUIRE_NO_THROW(kvInterface->create("/RecursiveDelete/child/b", "b"));
    BOOST_REQUIRE_NO_THROW(kvInterface->set("/RecursiveDelete", "root"));
    BOOST_CHECK_EQUAL(kvInterface->get("/RecursiveDelete"), "root");
    BOOST_CHECK_EQUAL(kvInterface->exists("/RecursiveDelete/child"), true);
    BOOST_CHECK_EQUAL(kvInterface->get("/RecursiveDelete/child/a"), "a");
    BOOST_CHECK_EQUAL(kvInterface->get("/RecursiveDelete/child/b"), "b");

    BOOST_REQUIRE_NO_THROW(kvInterface->deleteKey("/RecursiveDelete/child/a"));
    BOOST_CHECK_EQUAL(kvInterface->exists("/RecursiveDelete"), true);
    BOOST_CHECK_EQUAL(kvInterface->exists("/RecursiveDelete/child"), true);
    BOOST_CHECK_EQUAL(kvInterface->get("/RecursiveDelete/child/b"), "b");

    BOOST_REQUIRE_NO_THROW(kvInterface->deleteKey("/RecursiveDelete"));
    BOOST_CHECK_EQUAL(kvInterface->exists("/RecursiveDelete"), false);
    BOOST_CHECK_EQUAL(kvInterface->exists("/RecursiveDelete/child"), false);
    BOOST_CHECK_EQUAL(kvInterface->exists("/RecursiveDelete/child/a"), false);
    BOOST_CHECK_EQUAL(kvInterface->exists("/RecursiveDelete/child/b"), false);
}


BOOST_AUTO_TEST_CASE(Set) {
    CHECK_CONNECTION();

    // call set with a key that does not exist (it should get added)
    BOOST_REQUIRE_NO_THROW(kvInterface->set("/Set", "nowItExists"));
    // verify the key was added:
    BOOST_CHECK_EQUAL(kvInterface->get("/Set"), "nowItExists");
    // set the key to a new value
    BOOST_REQUIRE_NO_THROW(kvInterface->set("/Set", "toANewValue"));
    // verify the change:
    BOOST_CHECK_EQUAL(kvInterface->get("/Set"), "toANewValue");
}


BOOST_AUTO_TEST_CASE(SetRecursive) {
    CHECK_CONNECTION();

    BOOST_REQUIRE_NO_THROW(kvInterface->set("/SetRecursive/a/long/key", "a value"));
    BOOST_CHECK_EQUAL(kvInterface->exists("/SetRecursive"), true);
    BOOST_CHECK_EQUAL(kvInterface->exists("/SetRecursive/a"), true);
    BOOST_CHECK_EQUAL(kvInterface->exists("/SetRecursive/a/long"), true);
    BOOST_CHECK_EQUAL(kvInterface->get("/SetRecursive/a/long/key"), "a value");
}


BOOST_AUTO_TEST_CASE(KeyTooLong) {
    CHECK_CONNECTION();

    std::string tooLongKey = std::string("/") + std::string(9999, 'x');
    BOOST_CHECK_THROW(kvInterface->set(tooLongKey, "to value"), lsst::qserv::css::CssError);
    BOOST_CHECK_EQUAL(kvInterface->exists(tooLongKey), false);
}


BOOST_AUTO_TEST_CASE(InvalidSql) {
    CHECK_CONNECTION();

    BOOST_REQUIRE_NO_THROW(kvInterface->create("/Robert'); DROP TABLE kvData;--", "ha ha sucker"));
}

BOOST_AUTO_TEST_SUITE_END()
