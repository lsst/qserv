// -*- LSST-C++ -*-

/*
 * See COPYRIGHT file at the top of the source tree
 */

/**
  * @brief Unit test for the CssAccess class.
  */


// System headers
#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// Third-party headers

// Qserv headers
#include "css/constants.h"
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "css/EmptyChunks.h"
#include "css/KvInterfaceImplMem.h"

// Boost unit test header
#define BOOST_TEST_MODULE TestCssAccess
#include "boost/test/included/unit_test.hpp"

using namespace std;

namespace lsst {
namespace qserv {
namespace css {

shared_ptr<KvInterface> initKVI() {
    vector<pair<string, string>> kv;
    kv.push_back(make_pair("/", ""));
    kv.push_back(make_pair("/css_meta", ""));
    kv.push_back(make_pair("/css_meta/version", to_string(CssAccess::cssVersion())));

    kv.push_back(make_pair("/PARTITIONING", ""));
    string p = "/PARTITIONING/_0000000001";
    kv.push_back(make_pair(p, ""));
    kv.push_back(make_pair(p+"/nStripes", "60"));
    kv.push_back(make_pair(p+"/nSubStripes", "18"));
    kv.push_back(make_pair(p+"/overlap", "0.025"));

    kv.push_back(make_pair("/DBS", ""));
    kv.push_back(make_pair("/DBS/dbA", KEY_STATUS_READY));
    kv.push_back(make_pair("/DBS/dbA/partitioningId", "0000000001"));
    kv.push_back(make_pair("/DBS/dbB", "Bdb"));
    kv.push_back(make_pair("/DBS/dbC", KEY_STATUS_IGNORE));
    p = "/DBS/dbA/TABLES";
    kv.push_back(make_pair(p, ""));
    kv.push_back(make_pair(p + "/Object", KEY_STATUS_READY));
    kv.push_back(make_pair(p + "/Object/partitioning", ""));
    kv.push_back(make_pair(p + "/Object/partitioning/lonColName", "ra_PS"));
    kv.push_back(make_pair(p + "/Object/partitioning/latColName", "decl_PS"));
    kv.push_back(make_pair(p + "/Object/partitioning/subChunks", "1"));
    kv.push_back(make_pair(p + "/Object/partitioning/dirColName","objectId"));
    kv.push_back(make_pair(p + "/Source", KEY_STATUS_READY));
    kv.push_back(make_pair(p + "/Source/partitioning", ""));
    kv.push_back(make_pair(p + "/Source/partitioning/lonColName", "ra"));
    kv.push_back(make_pair(p + "/Source/partitioning/latColName", "decl"));
    kv.push_back(make_pair(p + "/Source/partitioning/subChunks", "0"));
    kv.push_back(make_pair(p + "/FSource", KEY_STATUS_READY));
    kv.push_back(make_pair(p + "/FSource/partitioning", ""));
    kv.push_back(make_pair(p + "/FSource/partitioning/lonColName", "ra"));
    kv.push_back(make_pair(p + "/FSource/partitioning/latColName", "decl"));
    kv.push_back(make_pair(p + "/FSource/partitioning/subChunks", "0"));
    kv.push_back(make_pair(p + "/Exposure", KEY_STATUS_READY));
    kv.push_back(make_pair(p + "/Exposure/schema", "(I INT)"));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS", ""));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/1234", ""));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/1234/REPLICAS", ""));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/1234/REPLICAS/0000000001", ""));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/1234/REPLICAS/0000000001/.packed.json", R"({"nodeName": "worker1"})"));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/1234/REPLICAS/0000000002", ""));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/1234/REPLICAS/0000000002/.packed.json", R"({"nodeName": "worker2"})"));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/5678", ""));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/5678/REPLICAS", ""));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/5678/REPLICAS/0000000001", ""));
    kv.push_back(make_pair(p + "/Exposure/CHUNKS/5678/REPLICAS/0000000001/.packed.json", R"({"nodeName": "worker1"})"));

    p = "/DBS/dbB/TABLES";
    kv.push_back(make_pair(p, ""));
    kv.push_back(make_pair(p + "/Exposure", KEY_STATUS_READY));
    kv.push_back(make_pair(p + "/Exposure/.packed.json", R"X({"schema": "(FLOAT X)"})X"));
    kv.push_back(make_pair(p + "/MyObject", KEY_STATUS_READY));
    kv.push_back(make_pair(p + "/MyObject/partitioning", ""));
    kv.push_back(make_pair(p + "/MyObject/partitioning/lonColName", "ra_PS"));
    kv.push_back(make_pair(p + "/MyObject/partitioning/latColName", "decl_PS"));
    kv.push_back(make_pair(p + "/MyObject/partitioning/subChunks", "1"));
    kv.push_back(make_pair(p + "/MyObject/partitioning/dirDb","dbA"));
    kv.push_back(make_pair(p + "/MyObject/partitioning/dirTable","Object"));
    kv.push_back(make_pair(p + "/MyObject/partitioning/dirColName","objectId"));
    kv.push_back(make_pair(p + "/DeletedTable", "NOT_READY"));

    p = "/DBS/dbC/TABLES";
    kv.push_back(make_pair(p, ""));
    kv.push_back(make_pair(p + "/RefMatch", KEY_STATUS_READY));
    kv.push_back(make_pair(p + "/RefMatch/match", ""));
    kv.push_back(make_pair(p + "/RefMatch/match/dirTable1", "Object"));
    kv.push_back(make_pair(p + "/RefMatch/match/dirColName1", "objectId"));
    kv.push_back(make_pair(p + "/RefMatch/match/dirTable2", "Source"));
    kv.push_back(make_pair(p + "/RefMatch/match/dirColName2", "sourceId"));
    kv.push_back(make_pair(p + "/RefMatch/match/flagColName", "flag"));
    kv.push_back(make_pair(p + "/RefMatch2", KEY_STATUS_READY));
    kv.push_back(make_pair(p + "/RefMatch2/match", ""));
    kv.push_back(make_pair(p + "/RefMatch2/match/.packed.json",
                           R"({"dirTable1": "Object", "dirColName1": "objectId", "dirTable2": "Source", "dirColName2": "sourceId", "flagColName": "flag"})"));
    kv.push_back(make_pair(p + "/TempTable1", KEY_STATUS_IGNORE));
    kv.push_back(make_pair(p + "/TempTable2", "PENDING_CREATE:12345"));

    p = "/NODES";
    kv.push_back(make_pair(p, ""));
    kv.push_back(make_pair(p + "/node1", "ACTIVE"));
    kv.push_back(make_pair(p + "/node2", "INACTIVE"));
    kv.push_back(make_pair(p + "/node2/.packed.json", R"({"type": "worker", "host": "worker2", "port": 5012})"));
    kv.push_back(make_pair(p + "/node3", "ACTIVE"));
    kv.push_back(make_pair(p + "/node3/type", "worker"));
    kv.push_back(make_pair(p + "/node3/host", "worker3"));
    kv.push_back(make_pair(p + "/node3/port", "5012"));

    // copy all stuff to KVI
    auto kvI = make_shared<KvInterfaceImplMem>();
    for (auto& pair: kv) {
        kvI->create(pair.first, pair.second);
    }

    return kvI;
}

// Test fixure that instantiates a CSS with pre-loaded data
class CssAccessFixture: public CssAccess {
public:
    CssAccessFixture() : CssAccess(initKVI(), make_shared<EmptyChunks>()) {}

    ~CssAccessFixture() {}
};

BOOST_FIXTURE_TEST_SUITE(CssAccessTestSuite, CssAccessFixture)

BOOST_AUTO_TEST_CASE(testJsonParser) {
    map<string, string> jmap;

    // empty string is OK
    jmap = _unpackJson("", "");
    BOOST_CHECK(jmap.empty());

    // empty object
    jmap = _unpackJson("", "{}");
    BOOST_CHECK(jmap.empty());

    // non-empty object
    jmap = _unpackJson("", R"({"a": "string", "b": 1, "c": 1.234, "d": ""})");
    BOOST_CHECK_EQUAL(jmap.size(), 4U);
    BOOST_CHECK_EQUAL(jmap["a"], "string");
    BOOST_CHECK_EQUAL(jmap["b"], "1");
    BOOST_CHECK_EQUAL(jmap["c"], "1.234");
    BOOST_CHECK_EQUAL(jmap["d"], "");

    // non-empty object with non-trivial sub-objects
    jmap = _unpackJson("", R"({"a": "string", "b": {"b1": 1}, "c": [{}]})");
    BOOST_CHECK_EQUAL(jmap.size(), 1U);
    BOOST_CHECK_EQUAL(jmap["a"], "string");

    // non-empty object with non-trivial but empty sub-objects, empty
    // sub-objects ({} or []) are treated by parser as empty strings
    jmap = _unpackJson("", R"({"a": "string", "b": {}, "c": []})");
    BOOST_CHECK_EQUAL(jmap.size(), 3U);
    BOOST_CHECK_EQUAL(jmap["a"], "string");
    BOOST_CHECK_EQUAL(jmap["b"], "");
    BOOST_CHECK_EQUAL(jmap["c"], "");

    // some standard "constants"
    jmap = _unpackJson("", R"({"a": null, "b": true, "c": false})");
    BOOST_CHECK_EQUAL(jmap.size(), 3U);
    BOOST_CHECK_EQUAL(jmap["a"], "null");
    BOOST_CHECK_EQUAL(jmap["b"], "true");
    BOOST_CHECK_EQUAL(jmap["c"], "false");
}

BOOST_AUTO_TEST_CASE(testGetDbNames) {
    auto names = getDbNames();
    BOOST_CHECK_EQUAL(names.size(), 3U);
    BOOST_CHECK(count(names.begin(), names.end(), "dbA"));
    BOOST_CHECK(count(names.begin(), names.end(), "dbB"));
    BOOST_CHECK(count(names.begin(), names.end(), "dbC"));
}

BOOST_AUTO_TEST_CASE(testGetDbStatus) {
    auto statMap = getDbStatus();
    BOOST_CHECK_EQUAL(statMap.size(), 3U);
    BOOST_CHECK_EQUAL(statMap.count("dbA"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("dbB"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("dbC"), 1U);
    BOOST_CHECK_EQUAL(statMap["dbA"], KEY_STATUS_READY);
    BOOST_CHECK_EQUAL(statMap["dbB"], "Bdb");
    BOOST_CHECK_EQUAL(statMap["dbC"], KEY_STATUS_IGNORE);
}

BOOST_AUTO_TEST_CASE(testSetDbStatus) {
    setDbStatus("dbA", "DEAD");
    setDbStatus("dbB", KEY_STATUS_READY);
    setDbStatus("dbC", "");
    auto statMap = getDbStatus();
    BOOST_CHECK_EQUAL(statMap.size(), 3U);
    BOOST_CHECK_EQUAL(statMap.count("dbA"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("dbB"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("dbC"), 1U);
    BOOST_CHECK_EQUAL(statMap["dbA"], "DEAD");
    BOOST_CHECK_EQUAL(statMap["dbB"], KEY_STATUS_READY);
    BOOST_CHECK_EQUAL(statMap["dbC"], "");
}

BOOST_AUTO_TEST_CASE(testContainsDb) {
    BOOST_CHECK(containsDb("dbA"));
    BOOST_CHECK(containsDb("dbB"));
    BOOST_CHECK(containsDb("dbC"));
    BOOST_CHECK(not containsDb("db"));
}

BOOST_AUTO_TEST_CASE(testGetDbStriping) {
    StripingParams params;
    params = getDbStriping("dbA");
    BOOST_CHECK_EQUAL(params.stripes, 60);
    BOOST_CHECK_EQUAL(params.subStripes, 18);
    BOOST_CHECK_EQUAL(params.partitioningId, 1);
    BOOST_CHECK_EQUAL(params.overlap, 0.025);

    params = getDbStriping("dbB");
    BOOST_CHECK_EQUAL(params.stripes, 0);
    BOOST_CHECK_EQUAL(params.subStripes, 0);
    BOOST_CHECK_EQUAL(params.partitioningId, 0);
    BOOST_CHECK_EQUAL(params.overlap, 0.0);

    BOOST_CHECK_THROW(getDbStriping("dbX"), NoSuchDb);
}

BOOST_AUTO_TEST_CASE(testCreateDb) {
    StripingParams params1;
    createDb("dbNew1", params1, "L2", "UNRELEASED");
    params1 = getDbStriping("dbNew1");
    BOOST_CHECK_EQUAL(params1.stripes, 0);
    BOOST_CHECK_EQUAL(params1.subStripes, 0);
    BOOST_CHECK_EQUAL(params1.partitioningId, 0);
    BOOST_CHECK_EQUAL(params1.overlap, 0);

    StripingParams params2{50, 25, 0, 0.01};
    createDb("dbNew2", params2, "L2", "RELEASED");
    params2 = getDbStriping("dbNew2");
    BOOST_CHECK_EQUAL(params2.stripes, 50);
    BOOST_CHECK_EQUAL(params2.subStripes, 25);
    BOOST_CHECK(params2.partitioningId != 0);
    BOOST_CHECK_EQUAL(params2.overlap, 0.01);

    createDbLike("dbNew1Like", "dbNew1");
    params1 = getDbStriping("dbNew1Like");
    BOOST_CHECK_EQUAL(params1.stripes, 0);
    BOOST_CHECK_EQUAL(params1.subStripes, 0);
    BOOST_CHECK_EQUAL(params1.partitioningId, 0);
    BOOST_CHECK_EQUAL(params1.overlap, 0);

    createDbLike("dbNew2Like", "dbNew2");
    StripingParams params2like = getDbStriping("dbNew2Like");
    BOOST_CHECK_EQUAL(params2like.stripes, 50);
    BOOST_CHECK_EQUAL(params2like.subStripes, 25);
    BOOST_CHECK_EQUAL(params2like.partitioningId, params2.partitioningId);
    BOOST_CHECK_EQUAL(params2like.overlap, 0.01);
}

BOOST_AUTO_TEST_CASE(testDropDb) {
    StripingParams params1;
    createDb("dbNew1", params1, "L2", "UNRELEASED");
    StripingParams params2{50, 25, 0, 0.01};
    createDb("dbNew2", params2, "L2", "RELEASED");

    dropDb("dbNew1");
    dropDb("dbNew2");
    BOOST_CHECK_THROW(dropDb("dbNew2"), NoSuchDb);
    BOOST_CHECK_THROW(dropDb("dbX"), NoSuchDb);
}

BOOST_AUTO_TEST_CASE(testGetTableNames) {
    auto tables = getTableNames("dbA");
    sort(tables.begin(), tables.end());
    vector<string> expectA{"Exposure", "FSource", "Object", "Source"};
    BOOST_CHECK(tables == expectA);

    tables = getTableNames("dbB");
    sort(tables.begin(), tables.end());
    vector<string> expectB1{"Exposure", "MyObject"};
    BOOST_CHECK(tables == expectB1);

    tables = getTableNames("dbB", false);
    sort(tables.begin(), tables.end());
    vector<string> expectB2{"DeletedTable", "Exposure", "MyObject"};
    BOOST_CHECK(tables == expectB2);

    BOOST_CHECK_THROW(tables = getTableNames("dbX"), NoSuchDb);
}

BOOST_AUTO_TEST_CASE(testGetTableStatus) {
    auto statMap = getTableStatus("dbC");
    BOOST_CHECK_EQUAL(statMap.size(), 4U);
    BOOST_CHECK_EQUAL(statMap.count("RefMatch"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("RefMatch2"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("TempTable1"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("TempTable2"), 1U);
    BOOST_CHECK_EQUAL(statMap["RefMatch"], KEY_STATUS_READY);
    BOOST_CHECK_EQUAL(statMap["RefMatch2"], KEY_STATUS_READY);
    BOOST_CHECK_EQUAL(statMap["TempTable1"], KEY_STATUS_IGNORE);
    BOOST_CHECK_EQUAL(statMap["TempTable2"], "PENDING_CREATE:12345");
}

BOOST_AUTO_TEST_CASE(testSetTableStatus) {
    setTableStatus("dbC", "RefMatch", "");
    setTableStatus("dbC", "RefMatch2", "NOT_THERE");
    setTableStatus("dbC", "TempTable1", KEY_STATUS_READY);
    setTableStatus("dbC", "TempTable2", KEY_STATUS_IGNORE);
    auto statMap = getTableStatus("dbC");
    BOOST_CHECK_EQUAL(statMap.size(), 4U);
    BOOST_CHECK_EQUAL(statMap.count("RefMatch"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("RefMatch2"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("TempTable1"), 1U);
    BOOST_CHECK_EQUAL(statMap.count("TempTable2"), 1U);
    BOOST_CHECK_EQUAL(statMap["RefMatch"], "");
    BOOST_CHECK_EQUAL(statMap["RefMatch2"], "NOT_THERE");
    BOOST_CHECK_EQUAL(statMap["TempTable1"], KEY_STATUS_READY);
    BOOST_CHECK_EQUAL(statMap["TempTable2"], KEY_STATUS_IGNORE);
}

BOOST_AUTO_TEST_CASE(testContainsTable) {
    BOOST_CHECK(containsTable("dbA", "Exposure"));
    BOOST_CHECK(not containsTable("dbA", "ExposureNotThere"));

    BOOST_CHECK(containsTable("dbB", "Exposure"));
    BOOST_CHECK(not containsTable("dbB", "DeletedTable"));
    BOOST_CHECK(not containsTable("dbB", "DeletedTable", true));
    BOOST_CHECK(containsTable("dbB", "DeletedTable", false));

    BOOST_CHECK_THROW(containsTable("dbX", "any"), NoSuchDb);
}

BOOST_AUTO_TEST_CASE(testGetTableSchema) {
    BOOST_CHECK_EQUAL(getTableSchema("dbA", "Exposure"), "(I INT)");
    BOOST_CHECK_EQUAL(getTableSchema("dbA", "Object"), "");
    BOOST_CHECK_THROW(getTableSchema("dbA", "NotATable"), NoSuchTable);

    BOOST_CHECK_EQUAL(getTableSchema("dbB", "Exposure"), "(FLOAT X)");
}

BOOST_AUTO_TEST_CASE(testGetMatchTableParams) {
    MatchTableParams params;
    params = getMatchTableParams("dbA", "Exposure");
    BOOST_CHECK(params.dirTable1.empty());
    BOOST_CHECK(params.dirColName1.empty());
    BOOST_CHECK(params.dirTable2.empty());
    BOOST_CHECK(params.dirColName2.empty());
    BOOST_CHECK(params.flagColName.empty());

    // unpacked params
    params = getMatchTableParams("dbC", "RefMatch");
    BOOST_CHECK_EQUAL(params.dirTable1, "Object");
    BOOST_CHECK_EQUAL(params.dirColName1, "objectId");
    BOOST_CHECK_EQUAL(params.dirTable2, "Source");
    BOOST_CHECK_EQUAL(params.dirColName2, "sourceId");
    BOOST_CHECK_EQUAL(params.flagColName, "flag");

    // packed params
    params = getMatchTableParams("dbC", "RefMatch2");
    BOOST_CHECK_EQUAL(params.dirTable1, "Object");
    BOOST_CHECK_EQUAL(params.dirColName1, "objectId");
    BOOST_CHECK_EQUAL(params.dirTable2, "Source");
    BOOST_CHECK_EQUAL(params.dirColName2, "sourceId");
    BOOST_CHECK_EQUAL(params.flagColName, "flag");

    BOOST_CHECK_THROW(params = getMatchTableParams("dbC", "NoRefMatch"), NoSuchTable);
}

BOOST_AUTO_TEST_CASE(testGetPartTableParams) {
    PartTableParams params;
    params = getPartTableParams("dbA", "Exposure");
    BOOST_CHECK(params.dirDb.empty());
    BOOST_CHECK(params.dirTable.empty());
    BOOST_CHECK(params.dirColName.empty());
    BOOST_CHECK(params.latColName.empty());
    BOOST_CHECK(params.lonColName.empty());
    BOOST_CHECK_EQUAL(params.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.subChunks, false);

    params = getPartTableParams("dbB", "MyObject");
    BOOST_CHECK_EQUAL(params.dirDb, "dbA");
    BOOST_CHECK_EQUAL(params.dirTable, "Object");
    BOOST_CHECK_EQUAL(params.dirColName, "objectId");
    BOOST_CHECK_EQUAL(params.latColName, "decl_PS");
    BOOST_CHECK_EQUAL(params.lonColName, "ra_PS");
    BOOST_CHECK_EQUAL(params.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.subChunks, true);

    BOOST_CHECK_THROW(params = getPartTableParams("dbC", "NoRefMatch"), NoSuchTable);
}

BOOST_AUTO_TEST_CASE(testGetTableParams) {
    TableParams params;
    params = getTableParams("dbA", "Exposure");
    BOOST_CHECK(params.match.dirTable1.empty());
    BOOST_CHECK(params.match.dirColName1.empty());
    BOOST_CHECK(params.match.dirTable2.empty());
    BOOST_CHECK(params.match.dirColName2.empty());
    BOOST_CHECK(params.match.flagColName.empty());
    BOOST_CHECK(params.partitioning.dirDb.empty());
    BOOST_CHECK(params.partitioning.dirTable.empty());
    BOOST_CHECK(params.partitioning.dirColName.empty());
    BOOST_CHECK(params.partitioning.latColName.empty());
    BOOST_CHECK(params.partitioning.lonColName.empty());
    BOOST_CHECK_EQUAL(params.partitioning.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.partitioning.subChunks, false);

    // partitioning params
    params = getTableParams("dbB", "MyObject");
    BOOST_CHECK(params.match.dirTable1.empty());
    BOOST_CHECK(params.match.dirColName1.empty());
    BOOST_CHECK(params.match.dirTable2.empty());
    BOOST_CHECK(params.match.dirColName2.empty());
    BOOST_CHECK(params.match.flagColName.empty());
    BOOST_CHECK_EQUAL(params.partitioning.dirDb, "dbA");
    BOOST_CHECK_EQUAL(params.partitioning.dirTable, "Object");
    BOOST_CHECK_EQUAL(params.partitioning.dirColName, "objectId");
    BOOST_CHECK_EQUAL(params.partitioning.latColName, "decl_PS");
    BOOST_CHECK_EQUAL(params.partitioning.lonColName, "ra_PS");
    BOOST_CHECK_EQUAL(params.partitioning.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.partitioning.subChunks, true);

    // unpacked match params
    params = getTableParams("dbC", "RefMatch");
    BOOST_CHECK_EQUAL(params.match.dirTable1, "Object");
    BOOST_CHECK_EQUAL(params.match.dirColName1, "objectId");
    BOOST_CHECK_EQUAL(params.match.dirTable2, "Source");
    BOOST_CHECK_EQUAL(params.match.dirColName2, "sourceId");
    BOOST_CHECK_EQUAL(params.match.flagColName, "flag");
    BOOST_CHECK(params.partitioning.dirDb.empty());
    BOOST_CHECK(params.partitioning.dirTable.empty());
    BOOST_CHECK(params.partitioning.dirColName.empty());
    BOOST_CHECK(params.partitioning.latColName.empty());
    BOOST_CHECK(params.partitioning.lonColName.empty());
    BOOST_CHECK_EQUAL(params.partitioning.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.partitioning.subChunks, false);

    // packed match params
    params = getTableParams("dbC", "RefMatch2");
    BOOST_CHECK_EQUAL(params.match.dirTable1, "Object");
    BOOST_CHECK_EQUAL(params.match.dirColName1, "objectId");
    BOOST_CHECK_EQUAL(params.match.dirTable2, "Source");
    BOOST_CHECK_EQUAL(params.match.dirColName2, "sourceId");
    BOOST_CHECK_EQUAL(params.match.flagColName, "flag");
    BOOST_CHECK(params.partitioning.dirDb.empty());
    BOOST_CHECK(params.partitioning.dirTable.empty());
    BOOST_CHECK(params.partitioning.dirColName.empty());
    BOOST_CHECK(params.partitioning.latColName.empty());
    BOOST_CHECK(params.partitioning.lonColName.empty());
    BOOST_CHECK_EQUAL(params.partitioning.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.partitioning.subChunks, false);

    BOOST_CHECK_THROW(params = getTableParams("dbC", "NoRefMatch"), NoSuchTable);
}

BOOST_AUTO_TEST_CASE(testCreateTable) {
    PartTableParams params0;
    createTable("dbA", "NewTable", "(INT I)", params0);
    BOOST_CHECK(containsTable("dbA", "NewTable"));

    BOOST_CHECK_EQUAL(getTableSchema("dbA", "NewTable"), "(INT I)");

    TableParams params;
    params = getTableParams("dbA", "NewTable");
    BOOST_CHECK(params.match.dirTable1.empty());
    BOOST_CHECK(params.match.dirColName1.empty());
    BOOST_CHECK(params.match.dirTable2.empty());
    BOOST_CHECK(params.match.dirColName2.empty());
    BOOST_CHECK(params.match.flagColName.empty());
    BOOST_CHECK(params.partitioning.dirDb.empty());
    BOOST_CHECK(params.partitioning.dirTable.empty());
    BOOST_CHECK(params.partitioning.dirColName.empty());
    BOOST_CHECK(params.partitioning.latColName.empty());
    BOOST_CHECK(params.partitioning.lonColName.empty());
    BOOST_CHECK_EQUAL(params.partitioning.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.partitioning.subChunks, false);

    BOOST_CHECK_THROW(createTable("dbA", "NewTable", "(INT I)", params0), TableExists);

    PartTableParams params1{"dbA", "SomeTable", "dirColName", "latColName", "lonColName", 0.012, true, true};
    createTable("dbA", "NewTable2", "(INT J)", params1);
    BOOST_CHECK(containsTable("dbA", "NewTable2"));

    BOOST_CHECK_EQUAL(getTableSchema("dbA", "NewTable2"), "(INT J)");

    params = getTableParams("dbA", "NewTable2");
    BOOST_CHECK(params.match.dirTable1.empty());
    BOOST_CHECK(params.match.dirColName1.empty());
    BOOST_CHECK(params.match.dirTable2.empty());
    BOOST_CHECK(params.match.dirColName2.empty());
    BOOST_CHECK(params.match.flagColName.empty());
    BOOST_CHECK_EQUAL(params.partitioning.dirDb, "dbA");
    BOOST_CHECK_EQUAL(params.partitioning.dirTable, "SomeTable");
    BOOST_CHECK_EQUAL(params.partitioning.dirColName, "dirColName");
    BOOST_CHECK_EQUAL(params.partitioning.latColName, "latColName");
    BOOST_CHECK_EQUAL(params.partitioning.lonColName, "lonColName");
    BOOST_CHECK_EQUAL(params.partitioning.overlap, 0.012);
    BOOST_CHECK_EQUAL(params.partitioning.subChunks, true);
}

BOOST_AUTO_TEST_CASE(testCreateMatchTable) {
    MatchTableParams params0;
    createMatchTable("dbA", "MatchTable", "(INT I)", params0);
    BOOST_CHECK(containsTable("dbA", "MatchTable"));

    BOOST_CHECK_EQUAL(getTableSchema("dbA", "MatchTable"), "(INT I)");

    TableParams params;
    params = getTableParams("dbA", "MatchTable");
    BOOST_CHECK(params.match.dirTable1.empty());
    BOOST_CHECK(params.match.dirColName1.empty());
    BOOST_CHECK(params.match.dirTable2.empty());
    BOOST_CHECK(params.match.dirColName2.empty());
    BOOST_CHECK(params.match.flagColName.empty());
    BOOST_CHECK(params.partitioning.dirDb.empty());
    BOOST_CHECK(params.partitioning.dirTable.empty());
    BOOST_CHECK(params.partitioning.dirColName.empty());
    BOOST_CHECK(params.partitioning.latColName.empty());
    BOOST_CHECK(params.partitioning.lonColName.empty());
    BOOST_CHECK_EQUAL(params.partitioning.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.partitioning.subChunks, false);

    BOOST_CHECK_THROW(createMatchTable("dbA", "MatchTable", "(INT I)", params0), TableExists);

    MatchTableParams params1{"dirTable1", "dirCol1", "dirTable2", "dirCol2", "flagCol"};
    createMatchTable("dbA", "MatchTable2", "(INT X)", params1);
    BOOST_CHECK(containsTable("dbA", "MatchTable2"));

    BOOST_CHECK_EQUAL(getTableSchema("dbA", "MatchTable2"), "(INT X)");

    params = getTableParams("dbA", "MatchTable2");
    BOOST_CHECK_EQUAL(params.match.dirTable1, "dirTable1");
    BOOST_CHECK_EQUAL(params.match.dirColName1, "dirCol1");
    BOOST_CHECK_EQUAL(params.match.dirTable2, "dirTable2");
    BOOST_CHECK_EQUAL(params.match.dirColName2, "dirCol2");
    BOOST_CHECK_EQUAL(params.match.flagColName, "flagCol");
    BOOST_CHECK(params.partitioning.dirDb.empty());
    BOOST_CHECK(params.partitioning.dirTable.empty());
    BOOST_CHECK(params.partitioning.dirColName.empty());
    BOOST_CHECK(params.partitioning.latColName.empty());
    BOOST_CHECK(params.partitioning.lonColName.empty());
    BOOST_CHECK_EQUAL(params.partitioning.overlap, 0.0);
    BOOST_CHECK_EQUAL(params.partitioning.subChunks, false);
}

BOOST_AUTO_TEST_CASE(testDropTable) {
    PartTableParams params0;
    createTable("dbA", "NewTable", "(INT I)", params0);
    BOOST_CHECK(containsTable("dbA", "NewTable"));

    dropTable("dbA", "NewTable");
    BOOST_CHECK_THROW(dropTable("dbA", "NewTable"), NoSuchTable);

    BOOST_CHECK_THROW(dropTable("dbA", "NeverExisted"), NoSuchTable);
    BOOST_CHECK_THROW(dropTable("WrongDb", "NeverExisted"), NoSuchTable);
}

BOOST_AUTO_TEST_CASE(testGetNodeNames) {
    auto names = getNodeNames();
    std::sort(names.begin(), names.end());
    std::vector<std::string> expected{"node1", "node2", "node3"};
    BOOST_CHECK(names == expected);
}

BOOST_AUTO_TEST_CASE(testGetNodeParams) {
    NodeParams params;

    params = getNodeParams("node1");
    BOOST_CHECK_EQUAL(params.type, "");
    BOOST_CHECK_EQUAL(params.host, "");
    BOOST_CHECK_EQUAL(params.port, 0);
    BOOST_CHECK_EQUAL(params.state, NODE_STATE_ACTIVE);
    BOOST_CHECK(params.isActive());

    params = getNodeParams("node2");
    BOOST_CHECK_EQUAL(params.type, "worker");
    BOOST_CHECK_EQUAL(params.host, "worker2");
    BOOST_CHECK_EQUAL(params.port, 5012);
    BOOST_CHECK_EQUAL(params.state, NODE_STATE_INACTIVE);
    BOOST_CHECK(not params.isActive());

    params = getNodeParams("node3");
    BOOST_CHECK_EQUAL(params.type, "worker");
    BOOST_CHECK_EQUAL(params.host, "worker3");
    BOOST_CHECK_EQUAL(params.port, 5012);
    BOOST_CHECK_EQUAL(params.state, NODE_STATE_ACTIVE);
    BOOST_CHECK(params.isActive());

    BOOST_CHECK_THROW(getNodeParams("UnknownNode"), NoSuchNode);
}

BOOST_AUTO_TEST_CASE(testGetAllNodeParams) {
    auto parMap = getAllNodeParams();
    BOOST_CHECK_EQUAL(parMap.size(), 3U);

    NodeParams params;

    params = parMap["node1"];
    BOOST_CHECK_EQUAL(params.type, "");
    BOOST_CHECK_EQUAL(params.host, "");
    BOOST_CHECK_EQUAL(params.port, 0);
    BOOST_CHECK_EQUAL(params.state, NODE_STATE_ACTIVE);
    BOOST_CHECK(params.isActive());

    params = parMap["node2"];
    BOOST_CHECK_EQUAL(params.type, "worker");
    BOOST_CHECK_EQUAL(params.host, "worker2");
    BOOST_CHECK_EQUAL(params.port, 5012);
    BOOST_CHECK_EQUAL(params.state, NODE_STATE_INACTIVE);
    BOOST_CHECK(not params.isActive());

    params = parMap["node3"];
    BOOST_CHECK_EQUAL(params.type, "worker");
    BOOST_CHECK_EQUAL(params.host, "worker3");
    BOOST_CHECK_EQUAL(params.port, 5012);
    BOOST_CHECK_EQUAL(params.state, NODE_STATE_ACTIVE);
    BOOST_CHECK(params.isActive());
}

BOOST_AUTO_TEST_CASE(testAddNode) {
    NodeParams params("worker", "worker100", 5012, "SICK");
    addNode("newnode", params);

    auto names = getNodeNames();
    std::sort(names.begin(), names.end());
    std::vector<std::string> expected{"newnode", "node1", "node2", "node3"};
    BOOST_CHECK(names == expected);

    params = getNodeParams("newnode");
    BOOST_CHECK_EQUAL(params.type, "worker");
    BOOST_CHECK_EQUAL(params.host, "worker100");
    BOOST_CHECK_EQUAL(params.port, 5012);
    BOOST_CHECK_EQUAL(params.state, "SICK");
    BOOST_CHECK(not params.isActive());
}

BOOST_AUTO_TEST_CASE(testSetNodeState) {
    setNodeState("node2", NODE_STATE_ACTIVE);

    auto params = getNodeParams("node2");
    BOOST_CHECK_EQUAL(params.state, NODE_STATE_ACTIVE);
    BOOST_CHECK(params.isActive());
}

BOOST_AUTO_TEST_CASE(testDeleteNode) {
    deleteNode("node1");
    deleteNode("node2");
    deleteNode("node3");

    auto names = getNodeNames();
    BOOST_CHECK(names.empty());

    BOOST_CHECK_THROW(deleteNode("node1"), NoSuchNode);
    BOOST_CHECK_THROW(deleteNode("nodeX"), NoSuchNode);
}

BOOST_AUTO_TEST_CASE(testGetChunks) {
    std::map<int, std::vector<std::string>> chunks;
    chunks = getChunks("dbA", "Exposure");
    BOOST_CHECK_EQUAL(chunks.size(), 2U);
    std::vector<std::string> expected1234{"worker1", "worker2"};
    BOOST_CHECK(chunks[1234] == expected1234);
    std::vector<std::string> expected5678{"worker1"};
    BOOST_CHECK(chunks[5678] == expected5678);

    chunks = getChunks("dbA", "Object");
    BOOST_CHECK(chunks.empty());

    BOOST_CHECK_THROW(chunks = getChunks("dbA", "NonTable"), NoSuchTable);
}

BOOST_AUTO_TEST_CASE(testAddChunk) {
    addChunk("dbB", "MyObject", 1000, {"worker1", "worker2"});
    addChunk("dbB", "MyObject", 2000, {"worker3"});

    std::map<int, std::vector<std::string>> chunks;
    chunks = getChunks("dbB", "MyObject");
    BOOST_CHECK_EQUAL(chunks.size(), 2U);
    std::vector<std::string> expected1000{"worker1", "worker2"};
    BOOST_CHECK(chunks[1000] == expected1000);
    std::vector<std::string> expected2000{"worker3"};
    BOOST_CHECK(chunks[2000] == expected2000);
}

BOOST_AUTO_TEST_SUITE_END()

// inline test data for test cases below
char const* testData = "\
/\t\\N\n\
/css_meta\t\\N\n\
/css_meta/version\t1\n\
/DBS\t\\N\n\
/DBS/LSST\tLSST\n\
";

// Test quite for CssAccess factory methods
BOOST_AUTO_TEST_SUITE(CssAccessFactoryTestSuite)

BOOST_AUTO_TEST_CASE(testDataString) {

    auto css1 = CssAccess::createFromData("", "");
    auto css2 = CssAccess::createFromData(testData, "");
    auto names = css2->getDbNames();
    BOOST_CHECK_EQUAL(names.size(), 1U);
    BOOST_CHECK_EQUAL(names[0], "LSST");
}

BOOST_AUTO_TEST_CASE(testConfigMap) {
    typedef std::map<std::string, std::string> StringMap;

    // test with missing required keyword
    BOOST_CHECK_THROW(auto css = CssAccess::createFromConfig(StringMap(), ""), ConfigError);
    {
        // test with incorrect keyword value
        // this test will fail when we have CSS implemented using monkeys
        StringMap config = {std::make_pair("technology", "monkeys")};
        BOOST_CHECK_THROW(auto css = CssAccess::createFromConfig(config, ""), ConfigError);

    }
    {
        // test with empty initial data
        StringMap config = {std::make_pair("technology", "mem")};
        auto css = CssAccess::createFromConfig(config, "");
    }
    {
        // test with initial data from string
        StringMap config = {
            std::make_pair("technology", "mem"),
            std::make_pair("data", testData),
        };
        auto css = CssAccess::createFromConfig(config, "");
        auto names = css->getDbNames();
        BOOST_CHECK_EQUAL(names.size(), 1U);
    }
    {
        // test bad file name
        StringMap config = {
            std::make_pair("technology", "mem"),
            std::make_pair("file", "/~~~"),
        };
        BOOST_CHECK_THROW(auto css = CssAccess::createFromConfig(config, ""), ConfigError);
    }
    {
        // test badly-formatted port number for mysql
        StringMap config = {
            std::make_pair("technology", "mysql"),
            std::make_pair("port", "X"),
        };
        BOOST_CHECK_THROW(auto css = CssAccess::createFromConfig(config, ""), ConfigError);

        config["port"] = "12bad";
        BOOST_CHECK_THROW(auto css = CssAccess::createFromConfig(config, ""), ConfigError);

        config["port"] = "0xdead";
        BOOST_CHECK_THROW(auto css = CssAccess::createFromConfig(config, ""), ConfigError);
    }
}

BOOST_AUTO_TEST_CASE(testReadOnly) {

    // read-write instance
    auto css = CssAccess::createFromData(testData, "");
    StripingParams params1;
    css->createDb("dbNew1", params1, "L2", "UNRELEASED");

    // read-only instance
    css = CssAccess::createFromData(testData, "", true);
    BOOST_CHECK_THROW(css->createDb("dbNew1", params1, "L2", "UNRELEASED"), ReadonlyCss);

    // same read-only from config map
    typedef std::map<std::string, std::string> StringMap;
    StringMap config = {
        std::make_pair("technology", "mem"),
        std::make_pair("data", testData),
    };
    css = CssAccess::createFromConfig(config, "", true);
    BOOST_CHECK_THROW(css->createDb("dbNew1", params1, "L2", "UNRELEASED"), ReadonlyCss);
}

BOOST_AUTO_TEST_CASE(testCssVersion) {

    // version mismatch
    testData = "/\t\\N\n/css_meta\t\\N\n/css_meta/version\t1000000";
    BOOST_CHECK_THROW(CssAccess::createFromData(testData, ""), VersionMismatchError);
}

BOOST_AUTO_TEST_SUITE_END()

}}} // namespace lsst::qserv::css
