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
  * @brief test SemanticMap
  */

// System headers
#include <algorithm>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"

// Boost unit test header
#define BOOST_TEST_MODULE Configuration
#include "boost/test/included/unit_test.hpp"

using namespace std;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ConfigurationTest) {

    LOGS_INFO("Configuration test begins");

    map<string, string> kvMap = {
        {"common.workers",                    "worker-A worker-B worker-C"},
        {"common.database_families",          "production test"},
        {"common.databases",                  "db1 db2 db3 db4 db5 db6"},
        {"common.request_buf_size_bytes",     "8192"},
        {"common.request_retry_interval_sec", "1"},
        {"controller.num_threads",            "2"},
        {"controller.http_server_port",       "8080"},
        {"controller.http_server_threads",    "3"},
        {"controller.request_timeout_sec",    "100"},
        {"controller.job_timeout_sec",        "200"},
        {"controller.job_heartbeat_sec",      "300"},
        {"controller.empty_chunks_dir",       "/qserv/data/qserv"},
        {"database.technology",               "mysql"},
        {"database.host",                     "mysql.lsst.org"},
        {"database.port",                     "13306"},
        {"database.user",                     "qsreplica"},
        {"database.password",                 "changeme"},
        {"database.name",                     "qservReplica"},
        {"database.services_pool_size",       "2"},
        {"xrootd.auto_notify",                "0"},
        {"xrootd.host",                       "xrootd.lsst.org"},
        {"xrootd.port",                       "1104"},
        {"xrootd.request_timeout_sec",        "400"},
        {"worker.technology",                 "POSIX"},
        {"worker.num_svc_processing_threads", "4"},
        {"worker.num_fs_processing_threads",  "5"},
        {"worker.fs_buf_size_bytes",          "1024"},
        {"worker.num_loader_processing_threads", "6"},
        {"worker.num_exporter_processing_threads", "7"},
        {"worker.svc_port",                   "51000"},
        {"worker.fs_port",                    "52000"},
        {"worker.data_dir",                   "/data/{worker}"},
        {"worker.db_port",                    "3306"},
        {"worker.db_user",                    "root"},
        {"worker.loader_port",                "53000"},
        {"worker.loader_tmp_dir",             "/tmp/{worker}"},
        {"worker.exporter_port",              "54000"},
        {"worker.exporter_tmp_dir",           "/tmp/{worker}"},

        {"worker:worker-A.is_enabled",   "1"},
        {"worker:worker-A.is_read_only", "0"},
        {"worker:worker-A.svc_host",     "host-A"},
        {"worker:worker-A.svc_port",     "51001"},
        {"worker:worker-A.fs_host",      "host-A"},
        {"worker:worker-A.fs_port",      "52001"},
        {"worker:worker-A.data_dir",     "/data/A"},
        {"worker:worker-A.db_host",      "host-A"},
        {"worker:worker-A.db_port",      "53306"},
        {"worker:worker-A.db_user",      "qsmaster"},
        {"worker:worker-A.loader_host",  "host-A"},
        {"worker:worker-A.loader_port",  "53002"},
        {"worker:worker-A.loader_tmp_dir", "/tmp/A"},
        {"worker:worker-A.exporter_host",  "host-A"},
        {"worker:worker-A.exporter_port",  "53003"},
        {"worker:worker-A.exporter_tmp_dir", "/tmp/export/A"},

        {"worker:worker-B.is_enabled",   "1"},
        {"worker:worker-B.is_read_only", "1"},
        {"worker:worker-B.svc_host",     "host-B"},
        {"worker:worker-B.fs_host",      "host-B"},
        {"worker:worker-B.data_dir",     "/data/B"},
        {"worker:worker-B.db_host",      "host-B"},
        {"worker:worker-B.loader_host",  "host-B"},
        {"worker:worker-B.exporter_host",  "host-B"},

        {"worker:worker-C.is_enabled",   "0"},
        {"worker:worker-C.is_read_only", "0"},
        {"worker:worker-C.svc_host",     "host-C"},
        {"worker:worker-C.fs_host",      "host-C"},
        {"worker:worker-C.db_host",      "host-C"},
        {"worker:worker-C.loader_host",  "host-C"},
        {"worker:worker-C.exporter_host",  "host-C"},

        {"database_family:production.min_replication_level", "10"},
        {"database_family:production.num_stripes",           "11"},
        {"database_family:production.num_sub_stripes",       "12"},

        {"database_family:test.min_replication_level", "13"},
        {"database_family:test.num_stripes",           "14"},
        {"database_family:test.num_sub_stripes",       "15"},

        {"database:db1.family",             "production"},
        {"database:db1.partitioned_tables", "Table11"},
        {"database:db1.regular_tables",     "MetaTable11"},
        {"database:db1.is_published",       "1"},
        {"database:db1.director_table",     "Table11"},
        {"database:db1.director_table_key", "id1"},
        {"database:db1.chunk_id_key",       "chunkId1"},
        {"database:db1.sub_chunk_id_key",   "subChunkId1"},
        {"table:db1.Table11.latitude_key",  "decl11"},
        {"table:db1.Table11.longitude_key", "ra11"},

        {"database:db2.family",             "production"},
        {"database:db2.partitioned_tables", "Table21 Table22"},
        {"database:db2.regular_tables",     "MetaTable21 MetaTable22"},
        {"database:db2.is_published",       "1"},
        {"database:db2.director_table",     "Table21"},
        {"database:db2.director_table_key", "id2"},
        {"database:db2.chunk_id_key",       "chunkId2"},
        {"database:db2.sub_chunk_id_key",   "subChunkId2"},
        {"table:db2.Table21.latitude_key",  "decl21"},
        {"table:db2.Table21.longitude_key", "ra21"},
        {"table:db2.Table22.latitude_key",  "decl22"},
        {"table:db2.Table22.longitude_key", "ra22"},

        {"database:db3.family",             "production"},
        {"database:db3.partitioned_tables", "Table31 Table32 Table33"},
        {"database:db3.regular_tables",     "MetaTable31 MetaTable32 MetaTable33"},
        {"database:db3.is_published",       "1"},
        {"database:db3.director_table",     "Table31"},
        {"database:db3.director_table_key", "id3"},
        {"database:db3.chunk_id_key",       "chunkId3"},
        {"database:db3.sub_chunk_id_key",   "subChunkId3"},
        {"table:db3.Table31.latitude_key",  "decl31"},
        {"table:db3.Table31.longitude_key", "ra31"},
        {"table:db3.Table32.latitude_key",  "decl32"},
        {"table:db3.Table32.longitude_key", "ra32"},
        {"table:db3.Table33.latitude_key",  "decl33"},
        {"table:db3.Table33.longitude_key", "ra33"},

        {"database:db4.family",             "test"},
        {"database:db4.partitioned_tables", "Table41 Table42"},
        {"database:db4.regular_tables",     ""},
        {"database:db4.is_published",       "1"},
        {"database:db4.director_table",     "Table41"},
        {"database:db4.director_table_key", "id4"},
        {"database:db4.chunk_id_key",       "chunkId4"},
        {"database:db4.sub_chunk_id_key",   "subChunkId4"},
        {"table:db4.Table41.latitude_key",  "decl41"},
        {"table:db4.Table41.longitude_key", "ra41"},
        {"table:db4.Table42.latitude_key",  "decl42"},
        {"table:db4.Table42.longitude_key", "ra42"},

        {"database:db5.family",             "test"},
        {"database:db5.partitioned_tables", "Table51"},
        {"database:db5.regular_tables",     ""},
        {"database:db5.is_published",       "1"},
        {"database:db5.director_table",     "Table51"},
        {"database:db5.director_table_key", "id5"},
        {"database:db5.chunk_id_key",       "chunkId5"},
        {"database:db5.sub_chunk_id_key",   "subChunkId5"},
        {"table:db5.Table51.latitude_key",  "decl51"},
        {"table:db5.Table51.longitude_key", "ra51"},

        {"database:db6.family",             "test"},
        {"database:db6.partitioned_tables", "Table61"},
        {"database:db6.regular_tables",     "MetaTable61"},  
        {"database:db6.is_published",       "0"},
        {"database:db6.director_table",     "Table61"},
        {"database:db6.director_table_key", "id6"},
        {"database:db6.chunk_id_key",       "chunkId6"},
        {"database:db6.sub_chunk_id_key",   "subChunkId6"},
        {"table:db6.Table61.latitude_key",  "decl61"},
        {"table:db6.Table61.longitude_key", "ra61"}
    };

    Configuration::Ptr config;

    BOOST_REQUIRE_NO_THROW({
        config = Configuration::load(kvMap);
        BOOST_CHECK(config != nullptr);
        BOOST_CHECK(config->configUrl() == "map:");

        LOGS_INFO(config->asString());

        // Test default assumptions for optional parameters of the method
        vector<string> workers1 = config->workers();
        sort(workers1.begin(), workers1.end());
        BOOST_CHECK(workers1.size() == 1);
        BOOST_CHECK(workers1 == vector<string>({"worker-A"}));

        // Test explicit selectors
        bool isEnabled  = true;
        bool isReadOnly = false;

        vector<string> workers2 = config->workers(isEnabled, isReadOnly);
        sort(workers2.begin(), workers2.end());
        BOOST_CHECK(workers2.size() == 1);
        BOOST_CHECK(workers2 == workers1);

        // Fetch names of all the read-only workers
        isEnabled  = true;
        isReadOnly = true;

        vector<string> workers3 = config->workers(isEnabled, isReadOnly);
        sort(workers3.begin(), workers3.end());
        BOOST_CHECK(workers3.size() == 1);
        BOOST_CHECK(workers3 == vector<string>({"worker-B"}));

        // Fetch names of all the disabled workers
        isEnabled  = false;

        vector<string> workers4 = config->workers(isEnabled);
        sort(workers4.begin(), workers4.end());
        BOOST_CHECK(workers4.size() == 1);
        BOOST_CHECK(workers4 == vector<string>({"worker-C"}));
    });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK(config->requestBufferSizeBytes() == 8192);
        BOOST_CHECK(config->retryTimeoutSec()        == 1);


        BOOST_CHECK(config->controllerThreads()           == 2);
        BOOST_CHECK(config->controllerHttpPort()          == 8080);
        BOOST_CHECK(config->controllerHttpThreads()       == 3);
        BOOST_CHECK(config->controllerRequestTimeoutSec() == 100);
        BOOST_CHECK(config->controllerEmptyChunksDir()    == "/qserv/data/qserv");
        BOOST_CHECK(config->jobTimeoutSec()               == 200);
        BOOST_CHECK(config->jobHeartbeatTimeoutSec()      == 300);

        BOOST_CHECK(config->xrootdAutoNotify() == 0);
        BOOST_CHECK(config->xrootdHost()       == "xrootd.lsst.org");
        BOOST_CHECK(config->xrootdPort()       == 1104);
        BOOST_CHECK(config->xrootdTimeoutSec() == 400);

        BOOST_CHECK(config->databaseTechnology()       == "mysql");
        BOOST_CHECK(config->databaseHost()             == "mysql.lsst.org");
        BOOST_CHECK(config->databasePort()             == 13306);
        BOOST_CHECK(config->databaseUser()             == "qsreplica");
        BOOST_CHECK(config->databasePassword()         == "changeme");
        BOOST_CHECK(config->databaseName()             == "qservReplica");
        BOOST_CHECK(config->databaseServicesPoolSize() == 2);
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> families = config->databaseFamilies();
        sort(families.begin(), families.end());
        BOOST_CHECK(families.size() == 2);
        BOOST_CHECK(families == vector<string>({"production", "test"}));

        for (auto&& name: families) {
            BOOST_CHECK(config->isKnownDatabaseFamily(name));
        }
    });
    BOOST_REQUIRE_NO_THROW({
        DatabaseFamilyInfo const production = config->databaseFamilyInfo("production");
        BOOST_CHECK(production.name == "production");
        BOOST_CHECK(production.replicationLevel == 10);
        BOOST_CHECK(production.numStripes       == 11);
        BOOST_CHECK(production.numSubStripes    == 12);
    });
    BOOST_REQUIRE_NO_THROW({ 
        DatabaseFamilyInfo const test = config->databaseFamilyInfo("test");
        BOOST_CHECK(test.name == "test");
        BOOST_CHECK(test.replicationLevel == 13);
        BOOST_CHECK(test.numStripes       == 14);
        BOOST_CHECK(test.numSubStripes    == 15);
    });
    BOOST_REQUIRE_NO_THROW({
        BOOST_CHECK(config->replicationLevel("production") == 10);
        BOOST_CHECK(config->replicationLevel("test")       == 13);
    });
    BOOST_REQUIRE_NO_THROW({
        DatabaseFamilyInfo newFamily;
        newFamily.name = "new";
        newFamily.replicationLevel = 300;
        newFamily.numStripes = 301;
        newFamily.numSubStripes = 302;

        DatabaseFamilyInfo const newFamilyAdded = config->addDatabaseFamily(newFamily);
        BOOST_CHECK(config->isKnownDatabaseFamily("new"));

        BOOST_CHECK(newFamilyAdded.name == "new");
        BOOST_CHECK(newFamilyAdded.replicationLevel == 300);
        BOOST_CHECK(newFamilyAdded.numStripes       == 301);
        BOOST_CHECK(newFamilyAdded.numSubStripes    == 302);
    });
    BOOST_REQUIRE_NO_THROW({
        config->deleteDatabaseFamily("new");
        BOOST_CHECK(not config->isKnownDatabaseFamily("new"));
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> databases1 = config->databases();
        sort(databases1.begin(), databases1.end());
        BOOST_CHECK(databases1.size() == 5);
        BOOST_CHECK(databases1 == vector<string>({"db1", "db2", "db3", "db4", "db5"}));
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> databases2 = config->databases("production");
        sort(databases2.begin(), databases2.end());
        BOOST_CHECK(databases2.size() == 3);
        BOOST_CHECK(databases2 == vector<string>({"db1", "db2", "db3"}));
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> databases3 = config->databases("test");
        sort(databases3.begin(), databases3.end());
        BOOST_CHECK(databases3.size() == 2);
        BOOST_CHECK(databases3 == vector<string>({"db4", "db5"}));
    });
    BOOST_REQUIRE_NO_THROW({
        bool allDatabases = false;
        bool isPublished = true;
        vector<string> databases4 = config->databases("test", allDatabases, isPublished);
        sort(databases4.begin(), databases4.end());
        BOOST_CHECK(databases4.size() == 2);
        BOOST_CHECK(databases4 == vector<string>({"db4", "db5"}));

        isPublished = false;
        vector<string> databases5 = config->databases("test", allDatabases, isPublished);
        sort(databases5.begin(), databases5.end());
        BOOST_CHECK(databases5.size() == 1);
        BOOST_CHECK(databases5 == vector<string>({"db6"}));

        allDatabases = true;
        vector<string> databases6 = config->databases("test", allDatabases);
        sort(databases6.begin(), databases6.end());
        BOOST_CHECK(databases6.size() == 3);
        BOOST_CHECK(databases6 == vector<string>({"db4", "db5", "db6"}));

        isPublished = true;
        vector<string> databases7 = config->databases("test", allDatabases, isPublished);
        sort(databases7.begin(), databases7.end());
        BOOST_CHECK(databases7.size() == 3);
        BOOST_CHECK(databases7 == vector<string>({"db4", "db5", "db6"}));

        isPublished = false;
        vector<string> databases8 = config->databases("test", allDatabases, isPublished);
        sort(databases8.begin(), databases8.end());
        BOOST_CHECK(databases8.size() == 3);
        BOOST_CHECK(databases8 == vector<string>({"db4", "db5", "db6"}));

        for (auto&& name: vector<string>({"db1", "db2", "db3", "db4", "db5", "db6"})) {
            BOOST_CHECK(config->isKnownDatabase(name));  
        }
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> tables;

        DatabaseInfo const db1info = config->databaseInfo("db1");
        BOOST_CHECK(db1info.name   == "db1");
        BOOST_CHECK(db1info.family == "production");
        BOOST_CHECK(db1info.isPublished == true);
        BOOST_CHECK(db1info.directorTable == "Table11");
        BOOST_CHECK(db1info.directorTableKey == "id1");
        BOOST_CHECK(db1info.chunkIdColName == "chunkId1");
        BOOST_CHECK(db1info.subChunkIdColName == "subChunkId1");

        tables = db1info.partitionedTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 1);
        BOOST_CHECK(tables == vector<string>({"Table11"}));
        BOOST_CHECK(db1info.isPartitioned("Table11"));

        tables = db1info.regularTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 1);
        BOOST_CHECK(tables == vector<string>({"MetaTable11"}));
        BOOST_CHECK(not db1info.isPartitioned("MetaTable11"));
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> tables;

        DatabaseInfo const db2info = config->databaseInfo("db2");
        BOOST_CHECK(db2info.name   == "db2");
        BOOST_CHECK(db2info.family == "production");
        BOOST_CHECK(db2info.isPublished == true);
        BOOST_CHECK(db2info.directorTable == "Table21");
        BOOST_CHECK(db2info.directorTableKey == "id2");
        BOOST_CHECK(db2info.chunkIdColName == "chunkId2");
        BOOST_CHECK(db2info.subChunkIdColName == "subChunkId2");

        tables = db2info.partitionedTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 2);
        BOOST_CHECK(tables == vector<string>({"Table21", "Table22"}));

        tables = db2info.regularTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 2);
        BOOST_CHECK(tables == vector<string>({"MetaTable21", "MetaTable22"}));
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> tables;

        DatabaseInfo const db3info = config->databaseInfo("db3");
        BOOST_CHECK(db3info.name   == "db3");
        BOOST_CHECK(db3info.family == "production");
        BOOST_CHECK(db3info.isPublished == true);
        BOOST_CHECK(db3info.directorTable == "Table31");
        BOOST_CHECK(db3info.directorTableKey == "id3");
        BOOST_CHECK(db3info.chunkIdColName == "chunkId3");
        BOOST_CHECK(db3info.subChunkIdColName == "subChunkId3");

        tables = db3info.partitionedTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 3);
        BOOST_CHECK(tables == vector<string>({"Table31", "Table32", "Table33"}));

        tables = db3info.regularTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 3);
        BOOST_CHECK(tables == vector<string>({"MetaTable31", "MetaTable32", "MetaTable33"}));
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> tables;

        DatabaseInfo const db4info = config->databaseInfo("db4");
        BOOST_CHECK(db4info.name   == "db4");
        BOOST_CHECK(db4info.family == "test");
        BOOST_CHECK(db4info.isPublished == true);
        BOOST_CHECK(db4info.directorTable == "Table41");
        BOOST_CHECK(db4info.directorTableKey == "id4");
        BOOST_CHECK(db4info.chunkIdColName == "chunkId4");
        BOOST_CHECK(db4info.subChunkIdColName == "subChunkId4");

        tables = db4info.partitionedTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 2);
        BOOST_CHECK(tables == vector<string>({"Table41", "Table42"}));

        tables = db4info.regularTables;
        BOOST_CHECK(tables.size() == 0);
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> tables;

        DatabaseInfo const db5info = config->databaseInfo("db5");
        BOOST_CHECK(db5info.name   == "db5");
        BOOST_CHECK(db5info.family == "test");
        BOOST_CHECK(db5info.isPublished == true);
        BOOST_CHECK(db5info.directorTable == "Table51");
        BOOST_CHECK(db5info.directorTableKey == "id5");
        BOOST_CHECK(db5info.chunkIdColName == "chunkId5");
        BOOST_CHECK(db5info.subChunkIdColName == "subChunkId5");

        tables = db5info.partitionedTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 1);
        BOOST_CHECK(tables == vector<string>({"Table51"}));

        tables = db5info.regularTables;
        BOOST_CHECK(tables.size() == 0);
    });
    BOOST_REQUIRE_NO_THROW({
        vector<string> tables;

        DatabaseInfo const db6info = config->databaseInfo("db6");
        BOOST_CHECK(db6info.name   == "db6");
        BOOST_CHECK(db6info.family == "test");
        BOOST_CHECK(db6info.isPublished == false);
        BOOST_CHECK(db6info.directorTable == "Table61");
        BOOST_CHECK(db6info.directorTableKey == "id6");
        BOOST_CHECK(db6info.chunkIdColName == "chunkId6");
        BOOST_CHECK(db6info.subChunkIdColName == "subChunkId6");

        tables = db6info.partitionedTables;
        sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 1);
        BOOST_CHECK(tables == vector<string>({"Table61"}));

        tables = db6info.regularTables;
        BOOST_CHECK(tables.size() == 1);
        BOOST_CHECK(tables == vector<string>({"MetaTable61"}));
    });
    BOOST_REQUIRE_NO_THROW({
        DatabaseInfo info;
        info.name = "new";
        info.family = "test";
  
        DatabaseInfo const newInfo = config->addDatabase(info);
        BOOST_CHECK(newInfo.name   == "new");
        BOOST_CHECK(newInfo.family == "test");
        BOOST_CHECK(newInfo.isPublished == false);
        BOOST_CHECK(newInfo.partitionedTables.size() == 0);
        BOOST_CHECK(newInfo.regularTables.size() == 0);
        BOOST_CHECK(newInfo.directorTable == "");
        BOOST_CHECK(newInfo.directorTableKey == "");
        BOOST_CHECK(newInfo.chunkIdColName == "");
        BOOST_CHECK(newInfo.subChunkIdColName == "");

        BOOST_CHECK_THROW(config->addDatabase(newInfo), invalid_argument);
    });
    {
        DatabaseInfo info;
        info.name = "";
        BOOST_CHECK_THROW(config->addDatabase(info), invalid_argument);

        info.name   = "another";
        info.family = "";
        BOOST_CHECK_THROW(config->addDatabase(info), invalid_argument);

        info.family = "unknown";
        BOOST_CHECK_THROW(config->addDatabase(info), invalid_argument);
    }
    {
        DatabaseInfo info;
        BOOST_CHECK_THROW(info.isPartitioned("NonExistingTable"), invalid_argument);
    }
    {
        SqlColDef const emptyColdef;
        BOOST_CHECK(emptyColdef.name.empty());
        BOOST_CHECK(emptyColdef.type.empty());

        SqlColDef const coldef("itsName", "itsType");
        BOOST_CHECK(coldef.name == "itsName");
        BOOST_CHECK(coldef.type == "itsType");

        SqlColDef const copiedColdef(coldef);
        BOOST_CHECK(copiedColdef.name == "itsName");
        BOOST_CHECK(copiedColdef.type == "itsType");

        SqlColDef const assignedColdef = coldef;
        BOOST_CHECK(assignedColdef.name == "itsName");
        BOOST_CHECK(assignedColdef.type == "itsType");
    }
    {
        list<SqlColDef> coldefs;
        coldefs.emplace_back("chunkIdT1", "INT");
        coldefs.emplace_back("subChunkIdT1", "INT");
        bool const isPartitioned = true;
        bool const isDirectorTable = false;
        string const directorTableKey;
        DatabaseInfo info;
        BOOST_REQUIRE_NO_THROW({
            info = config->addTable("new", "T1", isPartitioned, coldefs, isDirectorTable, directorTableKey, "chunkIdT1", "subChunkIdT1");
        });
        BOOST_CHECK(info.partitionedTables.size() == 1 and
                    info.partitionedTables[0] == "T1");
    }
    BOOST_CHECK_THROW(config->addTable("new", "T1", true), invalid_argument);
    {
        list<SqlColDef> coldefs;
        coldefs.emplace_back("idT2", "VARCHAR(255)");
        coldefs.emplace_back("chunkIdT2", "INT");
        coldefs.emplace_back("subChunkIdT2", "INT");
        coldefs.emplace_back("declT2", "DOUBLE");
        coldefs.emplace_back("raT2", "DOUBLE");
        bool const isPartitioned = true;
        bool const isDirectorTable = true;
        string const directorTableKey = "idT2";
        DatabaseInfo info;
        BOOST_REQUIRE_NO_THROW({
            info = config->addTable("new", "T2", isPartitioned, coldefs, isDirectorTable, directorTableKey,
                    "chunkIdT2", "subChunkIdT2", "declT2", "raT2");
        });
        BOOST_CHECK(info.partitionedTables.size() == 2);
    }
    BOOST_CHECK_THROW(config->addTable("new", "T2", true), invalid_argument);
    BOOST_REQUIRE_NO_THROW({
        DatabaseInfo const info = config->addTable("new", "T3", false);
        BOOST_CHECK(info.regularTables.size() == 1 and
                    info.regularTables[0] == "T3");
    });
    BOOST_CHECK_THROW(config->addTable("new", "T3", false), invalid_argument);
    BOOST_REQUIRE_NO_THROW({
        config->deleteTable("new", "T3");
    });
    BOOST_REQUIRE_NO_THROW({
        DatabaseInfo const info = config->publishDatabase("new");
        BOOST_CHECK(info.name == "new");
        BOOST_CHECK(info.family == "test");
        BOOST_CHECK(info.isPublished == true);
        BOOST_CHECK(info.partitionedTables.size() == 2);
        BOOST_CHECK(info.regularTables.size() == 0);
    });
    // Adding tables to the database after it's published isn't allowed
    BOOST_CHECK_THROW(config->addTable("new", "T4", true), invalid_argument);
    BOOST_REQUIRE_NO_THROW({
        config->deleteTable("new", "T1");
        config->deleteTable("new", "T2");
        config->deleteDatabase("new");
    });

    BOOST_CHECK_THROW(config->deleteDatabase("new"), invalid_argument);
    BOOST_REQUIRE_NO_THROW({
        for (auto&& name: vector<string>({"worker-A", "worker-B", "worker-C"})) {
            BOOST_CHECK(config->isKnownWorker(name));
        }
  
        WorkerInfo const workerA = config->workerInfo("worker-A");
        BOOST_CHECK(workerA.name =="worker-A");
        BOOST_CHECK(workerA.isEnabled);
        BOOST_CHECK(not workerA.isReadOnly);
        BOOST_CHECK(workerA.svcHost == "host-A");
        BOOST_CHECK(workerA.svcPort == 51001);
        BOOST_CHECK(workerA.fsHost  == "host-A");
        BOOST_CHECK(workerA.fsPort  == 52001);
        BOOST_CHECK(workerA.dataDir == "/data/A");
        BOOST_CHECK(workerA.dbHost  == "host-A");
        BOOST_CHECK(workerA.dbPort  == 53306);
        BOOST_CHECK(workerA.dbUser  == "qsmaster");
        BOOST_CHECK(workerA.loaderHost   == "host-A");
        BOOST_CHECK(workerA.loaderPort   == 53002);
        BOOST_CHECK(workerA.loaderTmpDir == "/tmp/A");
        BOOST_CHECK(workerA.exporterHost   == "host-A");
        BOOST_CHECK(workerA.exporterPort   == 53003);
        BOOST_CHECK(workerA.exporterTmpDir == "/tmp/export/A");

        WorkerInfo const workerB = config->workerInfo("worker-B");
        BOOST_CHECK(workerB.name =="worker-B");
        BOOST_CHECK(workerB.isEnabled);
        BOOST_CHECK(workerB.isReadOnly);
        BOOST_CHECK(workerB.svcHost == "host-B");
        BOOST_CHECK(workerB.svcPort == 51000);
        BOOST_CHECK(workerB.fsHost  == "host-B");
        BOOST_CHECK(workerB.fsPort  == 52000);
        BOOST_CHECK(workerB.dataDir == "/data/B");
        BOOST_CHECK(workerB.dbHost  == "host-B");
        BOOST_CHECK(workerB.dbPort  == 3306);
        BOOST_CHECK(workerB.dbUser  == "root");
        BOOST_CHECK(workerB.loaderHost   == "host-B");
        BOOST_CHECK(workerB.loaderPort   == 53000);
        BOOST_CHECK(workerB.loaderTmpDir == "/tmp/worker-B");
        BOOST_CHECK(workerB.exporterHost   == "host-B");
        BOOST_CHECK(workerB.exporterPort   == 54000);
        BOOST_CHECK(workerB.exporterTmpDir == "/tmp/worker-B");

        WorkerInfo const workerC = config->workerInfo("worker-C");
        BOOST_CHECK(workerC.name =="worker-C");
        BOOST_CHECK(not workerC.isEnabled);
        BOOST_CHECK(workerC.svcHost == "host-C");
        BOOST_CHECK(workerC.svcPort == 51000);
        BOOST_CHECK(workerC.fsHost  == "host-C");
        BOOST_CHECK(workerC.fsPort  == 52000);
        BOOST_CHECK(workerC.dataDir == "/data/worker-C");
        BOOST_CHECK(workerC.dbHost  == "host-C");
        BOOST_CHECK(workerC.dbPort  == 3306);
        BOOST_CHECK(workerC.dbUser  == "root");
        BOOST_CHECK(workerC.loaderHost   == "host-C");
        BOOST_CHECK(workerC.loaderPort   == 53000);
        BOOST_CHECK(workerC.loaderTmpDir == "/tmp/worker-C");
        BOOST_CHECK(workerC.exporterHost   == "host-C");
        BOOST_CHECK(workerC.exporterPort   == 54000);
        BOOST_CHECK(workerC.exporterTmpDir == "/tmp/worker-C");

        // Adding a new worker with well formed and unique parameters

        WorkerInfo workerD;
        workerD.name       = "worker-D";
        workerD.isEnabled  = true;
        workerD.isReadOnly = true;
        workerD.svcHost    = "host-D";
        workerD.svcPort    = 51001;
        workerD.fsHost     = "host-D";
        workerD.fsPort     = 52001;
        workerD.dataDir    = "/data/D";
        workerD.dbHost     = "host-D";
        workerD.dbPort     = 13306;
        workerD.dbUser     = "default";
        workerD.loaderHost   = "host-D";
        workerD.loaderPort   = 52002;
        workerD.loaderTmpDir = "/tmp/D";
        workerD.exporterHost   = "host-D";
        workerD.exporterPort   = 52003;
        workerD.exporterTmpDir = "/tmp/D";

        config->addWorker(workerD);
        BOOST_CHECK_THROW(config->addWorker(workerD), invalid_argument);

        workerD = config->workerInfo("worker-D");
        BOOST_CHECK(workerD.name =="worker-D");
        BOOST_CHECK(workerD.isEnabled);
        BOOST_CHECK(workerD.isReadOnly);
        BOOST_CHECK(workerD.svcHost == "host-D");
        BOOST_CHECK(workerD.svcPort == 51001);
        BOOST_CHECK(workerD.fsHost  == "host-D");
        BOOST_CHECK(workerD.fsPort  == 52001);
        BOOST_CHECK(workerD.dataDir == "/data/D");
        BOOST_CHECK(workerD.dbHost  == "host-D");
        BOOST_CHECK(workerD.dbPort  == 13306);
        BOOST_CHECK(workerD.dbUser  == "default");
        BOOST_CHECK(workerD.loaderHost   == "host-D");
        BOOST_CHECK(workerD.loaderPort   == 52002);
        BOOST_CHECK(workerD.loaderTmpDir == "/tmp/D");
        BOOST_CHECK(workerD.exporterHost   == "host-D");
        BOOST_CHECK(workerD.exporterPort   == 52003);
        BOOST_CHECK(workerD.exporterTmpDir == "/tmp/D");

        // Adding a new worker with parameters conflicting with the ones of
        // some existing worker

        WorkerInfo workerE = workerD;
        workerE.name = "worker-E";
        BOOST_CHECK_THROW(config->addWorker(workerE), invalid_argument);

        config->deleteWorker("worker-C");
        BOOST_CHECK(not config->isKnownWorker("worker-C"));
        BOOST_CHECK_THROW(config->deleteWorker("worker-C"), invalid_argument);

        WorkerInfo const disabledWorker = config->disableWorker("worker-B");
        BOOST_CHECK(disabledWorker.name == "worker-B");
        BOOST_CHECK(not disabledWorker.isEnabled);

        WorkerInfo const enabledWorker = config->disableWorker("worker-B", false);
        BOOST_CHECK(enabledWorker.name == "worker-B");
        BOOST_CHECK(enabledWorker.isEnabled);

        WorkerInfo const readOnlyWorker = config->setWorkerReadOnly("worker-B");
        BOOST_CHECK(readOnlyWorker.name == "worker-B");
        BOOST_CHECK(readOnlyWorker.isReadOnly);

        WorkerInfo const readWriteWorker = config->setWorkerReadOnly("worker-B", false);
        BOOST_CHECK(readWriteWorker.name == "worker-B");
        BOOST_CHECK(not readWriteWorker.isReadOnly);

        BOOST_CHECK(config->setWorkerSvcHost("worker-A", "host-A1").svcHost == "host-A1");
        BOOST_CHECK(config->setWorkerSvcPort("worker-A", 1).svcPort == 1);

        BOOST_CHECK(config->setWorkerFsHost("worker-A", "host-A1").fsHost == "host-A1");
        BOOST_CHECK(config->setWorkerFsPort("worker-A", 2).fsPort == 2);

        BOOST_CHECK(config->setWorkerDataDir("worker-A", "/test").dataDir == "/test");

        BOOST_CHECK(config->workerTechnology()           == "POSIX");
        BOOST_CHECK(config->workerNumProcessingThreads() == 4);
        BOOST_CHECK(config->fsNumProcessingThreads()     == 5);
        BOOST_CHECK(config->workerFsBufferSizeBytes()    == 1024);
        BOOST_CHECK(config->loaderNumProcessingThreads() == 6);
        BOOST_CHECK(config->exporterNumProcessingThreads() == 7);

        config->setRequestBufferSizeBytes(8193);
        BOOST_CHECK(config->requestBufferSizeBytes() == 8193);

        config->setRetryTimeoutSec(2);
        BOOST_CHECK(config->retryTimeoutSec() == 2);

        config->setRetryTimeoutSec(2);
        BOOST_CHECK(config->retryTimeoutSec() == 2);

        config->setControllerThreads(3);
        BOOST_CHECK(config->controllerThreads() == 3);

        config->setControllerHttpPort(8081);
        BOOST_CHECK(config->controllerHttpPort() == 8081);

        config->setControllerHttpThreads(4);
        BOOST_CHECK(config->controllerHttpThreads() == 4);

        config->setControllerRequestTimeoutSec(101);
        BOOST_CHECK(config->controllerRequestTimeoutSec() == 101);

        config->setJobTimeoutSec(201);
        BOOST_CHECK(config->jobTimeoutSec() == 201);

        config->setJobHeartbeatTimeoutSec(301);
        BOOST_CHECK(config->jobHeartbeatTimeoutSec() == 301);

        config->setJobHeartbeatTimeoutSec(0);
        BOOST_CHECK(config->jobHeartbeatTimeoutSec() == 0);

        config->setXrootdAutoNotify(true);
        BOOST_CHECK(config->xrootdAutoNotify());

        config->setXrootdAutoNotify(false);
        BOOST_CHECK(not config->xrootdAutoNotify());

        config->setXrootdHost("localhost");
        BOOST_CHECK(config->xrootdHost() == "localhost");

        BOOST_CHECK_THROW(config->setXrootdHost(""), invalid_argument);

        config->setXrootdPort(1105);
        BOOST_CHECK(config->xrootdPort() == 1105);

        config->setXrootdTimeoutSec(401);
        BOOST_CHECK(config->xrootdTimeoutSec() == 401);

        config->setDatabaseServicesPoolSize(3);
        BOOST_CHECK(config->databaseServicesPoolSize() == 3);

        BOOST_CHECK_THROW(config->setDatabaseServicesPoolSize(0), invalid_argument);

        config->setWorkerTechnology("FS");
        BOOST_CHECK(config->workerTechnology() == "FS");

        config->setWorkerNumProcessingThreads(5);
        BOOST_CHECK(config->workerNumProcessingThreads() == 5);

        config->setFsNumProcessingThreads(6);
        BOOST_CHECK(config->fsNumProcessingThreads() == 6);

        config->setWorkerFsBufferSizeBytes(1025);
        BOOST_CHECK(config->workerFsBufferSizeBytes() == 1025);

        config->setLoaderNumProcessingThreads(7);
        BOOST_CHECK(config->loaderNumProcessingThreads() == 7);

        config->setExporterNumProcessingThreads(8);
        BOOST_CHECK(config->exporterNumProcessingThreads() == 8);
    });

    BOOST_CHECK_THROW(kvMap.at("non-existing-key"), out_of_range);

    LOGS_INFO("Configuration test ends");
}

BOOST_AUTO_TEST_SUITE_END()
