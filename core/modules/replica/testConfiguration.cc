// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/Configuration.h"

// Boost unit test header
#define BOOST_TEST_MODULE Configuration
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;
using namespace lsst::qserv::replica;

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ConfigurationTest) {

    LOGS_INFO("Configuration test begins");

    std::map<std::string,std::string> kvMap = {
        {"common.workers",                    "worker-A worker-B worker-C"},
        {"common.database_families",          "production test"},
        {"common.databases",                  "db1 db2 db3 db4 db5"},
        {"common.request_buf_size_bytes",     "8192"},
        {"common.request_retry_interval_sec", "1"},
        {"common.database_technology",        "mysql"},
        {"common.database_host",              "mysql.lsst.org"},
        {"common.database_port",              "13306"},
        {"common.database_user",              "qsreplica"},
        {"common.database_password",          "changeme"},
        {"common.database_name",              "qservReplica"},
        {"controller.num_threads",            "2"},
        {"controller.http_server_port",       "8080"},
        {"controller.http_server_threads",    "3"},
        {"controller.request_timeout_sec",    "100"},
        {"controller.job_timeout_sec",        "200"},
        {"controller.job_heartbeat_sec",      "300"},
        {"xrootd.auto_notify",                "0"},
        {"xrootd.host",                       "xrootd.lsst.org"},
        {"xrootd.port",                       "1104"},
        {"xrootd.request_timeout_sec",        "400"},
        {"worker.technology",                 "POSIX"},
        {"worker.num_svc_processing_threads", "4"},
        {"worker.num_fs_processing_threads",  "5"},
        {"worker.fs_buf_size_bytes",          "1024"},
        {"worker.svc_port",                   "51000"},
        {"worker.fs_port",                    "52000"},
        {"worker.data_dir",                   "/tmp/{worker}"},

        {"worker:worker-A.is_enabled",   "1"},
        {"worker:worker-A.is_read_only", "0"},
        {"worker:worker-A.svc_host",     "host-A"},
        {"worker:worker-A.svc_port",     "51001"},
        {"worker:worker-A.fs_host",      "host-A"},
        {"worker:worker-A.fs_port",      "52001"},
        {"worker:worker-A.data_dir",     "/data/A"},

        {"worker:worker-B.is_enabled",   "1"},
        {"worker:worker-B.is_read_only", "1"},
        {"worker:worker-B.svc_host",     "host-B"},
     // {"worker:worker-B.svc_port",     "51002"},      // assuming default
        {"worker:worker-B.fs_host",      "host-B"},
     // {"worker:worker-B.fs_port",      "52002"},      // assuming default
        {"worker:worker-B.data_dir",     "/data/B"},

        {"worker:worker-C.is_enabled",   "0"},
        {"worker:worker-C.is_read_only", "0"},
        {"worker:worker-C.svc_host",     "host-C"},
     // {"worker:worker-C.svc_port",     "51003"},      // assuming default
        {"worker:worker-C.fs_host",      "host-C"},
     // {"worker:worker-C.fs_port",      "52003"},      // assuming default
     // {"worker:worker-C.data_dir",     "/data/C"},    // assuming default

        {"database_family:production.min_replication_level", "10"},
        {"database_family:production.num_stripes",           "11"},
        {"database_family:production.num_sub_stripes",       "12"},

        {"database_family:test.min_replication_level", "13"},
        {"database_family:test.num_stripes",           "14"},
        {"database_family:test.num_sub_stripes",       "15"},

        {"database:db1.family",             "production"},
        {"database:db1.partitioned_tables", "Table11"},
        {"database:db1.regular_tables",     "MetaTable11"},

        {"database:db2.family",             "production"},
        {"database:db2.partitioned_tables", "Table21 Table22"},
        {"database:db2.regular_tables",     "MetaTable21 MetaTable22"},

        {"database:db3.family",             "production"},
        {"database:db3.partitioned_tables", "Table31 Table32 Table33"},
        {"database:db3.regular_tables",     "MetaTable31 MetaTable32 MetaTable33"},

        {"database:db4.family",             "test"},
        {"database:db4.partitioned_tables", "Table41 Table42"},
        {"database:db4.regular_tables",     ""},

        {"database:db5.family",             "test"},
        {"database:db5.partitioned_tables", "Table51"},
        {"database:db5.regular_tables",     ""}
    };

    Configuration::Ptr config;

    BOOST_REQUIRE_NO_THROW({

        config = Configuration::load(kvMap);
        BOOST_CHECK(config != nullptr);
        BOOST_CHECK(config->configUrl() == "map:");

       // ------------------------------------------------------------------------
       // -- Common configuration parameters of both the controller and workers --
       // ------------------------------------------------------------------------

        // Test default assumptions for optional parameters of the method

        std::vector<std::string> workers1 = config->workers();
        std::sort(workers1.begin(), workers1.end());
        BOOST_CHECK(workers1.size() == 1);
        BOOST_CHECK(workers1 == std::vector<std::string>({"worker-A"}));

        bool isEnabled  = true;
        bool isReadOnly = false;

        std::vector<std::string> workers2 = config->workers(isEnabled, isReadOnly);
        std::sort(workers2.begin(), workers2.end());
        BOOST_CHECK(workers2.size() == 1);
        BOOST_CHECK(workers2 == workers1);

        // Fetch names of all the read-only workers

        isEnabled  = true;
        isReadOnly = true;

        std::vector<std::string> workers3 = config->workers(isEnabled, isReadOnly);
        std::sort(workers3.begin(), workers3.end());
        BOOST_CHECK(workers3.size() == 1);
        BOOST_CHECK(workers3 == std::vector<std::string>({"worker-B"}));

        // Fetch names of all the disabled workers

        isEnabled  = false;

        std::vector<std::string> workers4 = config->workers(isEnabled);
        std::sort(workers4.begin(), workers4.end());
        BOOST_CHECK(workers4.size() == 1);
        BOOST_CHECK(workers4 == std::vector<std::string>({"worker-C"}));

        // Protocol parameters

        BOOST_CHECK(config->requestBufferSizeBytes() == 8192);
        BOOST_CHECK(config->retryTimeoutSec()        == 1);

        // --------------------------------------------------------
        // -- Configuration parameters of the controller service --
        // --------------------------------------------------------

        BOOST_CHECK(config->controllerThreads()           == 2);
        BOOST_CHECK(config->controllerHttpPort()          == 8080);
        BOOST_CHECK(config->controllerHttpThreads()       == 3);
        BOOST_CHECK(config->controllerRequestTimeoutSec() == 100);
        BOOST_CHECK(config->jobTimeoutSec()               == 200);
        BOOST_CHECK(config->jobHeartbeatTimeoutSec()      == 300);

        // --------------------------------------------------------
        // -- Qserv Worker Management Services  (via XRootD/SSI) --
        // --------------------------------------------------------

        BOOST_CHECK(config->xrootdAutoNotify() == 0);
        BOOST_CHECK(config->xrootdHost()       == "xrootd.lsst.org");
        BOOST_CHECK(config->xrootdPort()       == 1104);
        BOOST_CHECK(config->xrootdTimeoutSec() == 400);

        // -----------------------------------------------------------
        // -- Configuration parameters related to database services --
        // -----------------------------------------------------------

        BOOST_CHECK(config->databaseTechnology() == "mysql");
        BOOST_CHECK(config->databaseHost()       == "mysql.lsst.org");
        BOOST_CHECK(config->databasePort()       == 13306);
        BOOST_CHECK(config->databaseUser()       == "qsreplica");
        BOOST_CHECK(config->databasePassword()   == "changeme");
        BOOST_CHECK(config->databaseName()       == "qservReplica");

        // ---------------------------------------------------
        // -- Configuration parameters related to databases --
        // ---------------------------------------------------
    
        // Database families

        std::vector<std::string> families = config->databaseFamilies();
        std::sort(families.begin(), families.end());
        BOOST_CHECK(families.size() == 2);
        BOOST_CHECK(families == std::vector<std::string>({"production", "test"}));

        for (auto&& name: families) {
            BOOST_CHECK(config->isKnownDatabaseFamily(name));
        }

        DatabaseFamilyInfo const production = config->databaseFamilyInfo("production");
        BOOST_CHECK(production.name == "production");
        BOOST_CHECK(production.replicationLevel == 10);
        BOOST_CHECK(production.numStripes       == 11);
        BOOST_CHECK(production.numSubStripes    == 12);
 
        DatabaseFamilyInfo const test = config->databaseFamilyInfo("test");
        BOOST_CHECK(test.name == "test");
        BOOST_CHECK(test.replicationLevel == 13);
        BOOST_CHECK(test.numStripes       == 14);
        BOOST_CHECK(test.numSubStripes    == 15);
   
        BOOST_CHECK(config->replicationLevel("production") == 10);
        BOOST_CHECK(config->replicationLevel("test")       == 13);
    
        // Databases

        std::vector<std::string> databases1 = config->databases();
        std::sort(databases1.begin(), databases1.end());
        BOOST_CHECK(databases1.size() == 5);
        BOOST_CHECK(databases1 == std::vector<std::string>({"db1","db2","db3","db4","db5"}));

        std::vector<std::string> databases2 = config->databases("production");
        std::sort(databases2.begin(), databases2.end());
        BOOST_CHECK(databases2.size() == 3);
        BOOST_CHECK(databases2 == std::vector<std::string>({"db1","db2","db3"}));

        std::vector<std::string> databases3 = config->databases("test");
        std::sort(databases3.begin(), databases3.end());
        BOOST_CHECK(databases3.size() == 2);
        BOOST_CHECK(databases3 == std::vector<std::string>({"db4","db5"}));
    
        for (auto&& name: std::vector<std::string>({"db1","db2","db3","db4","db5"})) {
            BOOST_CHECK(config->isKnownDatabase(name));  
        }

        std::vector<std::string> tables;

        DatabaseInfo const db1info = config->databaseInfo("db1");
        BOOST_CHECK(db1info.name   == "db1");
        BOOST_CHECK(db1info.family == "production");

        tables = db1info.partitionedTables;
        std::sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 1);
        BOOST_CHECK(tables == std::vector<std::string>({"Table11"}));

        tables = db1info.regularTables;
        std::sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 1);
        BOOST_CHECK(tables == std::vector<std::string>({"MetaTable11"}));

        DatabaseInfo const db2info = config->databaseInfo("db2");
        BOOST_CHECK(db2info.name   == "db2");
        BOOST_CHECK(db2info.family == "production");

        tables = db2info.partitionedTables;
        std::sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 2);
        BOOST_CHECK(tables == std::vector<std::string>({"Table21","Table22"}));

        tables = db2info.regularTables;
        std::sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 2);
        BOOST_CHECK(tables == std::vector<std::string>({"MetaTable21","MetaTable22"}));

        DatabaseInfo const db3info = config->databaseInfo("db3");
        BOOST_CHECK(db3info.name   == "db3");
        BOOST_CHECK(db3info.family == "production");

        tables = db3info.partitionedTables;
        std::sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 3);
        BOOST_CHECK(tables == std::vector<std::string>({"Table31","Table32","Table33"}));

        tables = db3info.regularTables;
        std::sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 3);
        BOOST_CHECK(tables == std::vector<std::string>({"MetaTable31","MetaTable32","MetaTable33"}));

        DatabaseInfo const db4info = config->databaseInfo("db4");
        BOOST_CHECK(db4info.name   == "db4");
        BOOST_CHECK(db4info.family == "test");

        tables = db4info.partitionedTables;
        std::sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 2);
        BOOST_CHECK(tables == std::vector<std::string>({"Table41","Table42"}));

        tables = db4info.regularTables;
        BOOST_CHECK(tables.size() == 0);

        DatabaseInfo const db5info = config->databaseInfo("db5");
        BOOST_CHECK(db5info.name   == "db5");
        BOOST_CHECK(db5info.family == "test");

        tables = db5info.partitionedTables;
        std::sort(tables.begin(), tables.end());
        BOOST_CHECK(tables.size() == 1);
        BOOST_CHECK(tables == std::vector<std::string>({"Table51"}));

        tables = db5info.regularTables;
        BOOST_CHECK(tables.size() == 0);

        // -----------------------------------------------------
        // -- Configuration parameters of the worker services --
        // -----------------------------------------------------
    
        for (auto&& name: std::vector<std::string>({"worker-A","worker-B","worker-C"})) {
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

        WorkerInfo const workerB = config->workerInfo("worker-B");
        BOOST_CHECK(workerB.name =="worker-B");
        BOOST_CHECK(workerB.isEnabled);
        BOOST_CHECK(workerB.isReadOnly);
        BOOST_CHECK(workerB.svcHost == "host-B");
        BOOST_CHECK(workerB.svcPort == 51000);
        BOOST_CHECK(workerB.fsHost  == "host-B");
        BOOST_CHECK(workerB.fsPort  == 52000);
        BOOST_CHECK(workerB.dataDir == "/data/B");

        WorkerInfo const workerC = config->workerInfo("worker-C");
        BOOST_CHECK(workerC.name =="worker-C");
        BOOST_CHECK(not workerC.isEnabled);
        BOOST_CHECK(workerC.svcHost == "host-C");
        BOOST_CHECK(workerC.svcPort == 51000);
        BOOST_CHECK(workerC.fsHost  == "host-C");
        BOOST_CHECK(workerC.fsPort  == 52000);
        BOOST_CHECK(workerC.dataDir == "/tmp/worker-C");

        WorkerInfo const disabledWorker = config->disableWorker("worker-B");
        BOOST_CHECK(disabledWorker.name == "worker-B");
        BOOST_CHECK(not disabledWorker.isEnabled);

        config->deleteWorker("worker-C");
        BOOST_CHECK(not config->isKnownWorker("worker-C"));

        BOOST_CHECK(config->workerTechnology()           == "POSIX");
        BOOST_CHECK(config->workerNumProcessingThreads() == 4);
        BOOST_CHECK(config->fsNumProcessingThreads()     == 5);
        BOOST_CHECK(config->workerFsBufferSizeBytes()    == 1024);


        BOOST_CHECK(config->setWorkerSvcPort("worker-A", 1).svcPort == 1);

        BOOST_CHECK(config->setWorkerFsPort("worker-A", 2).fsPort == 2);
    });

    BOOST_CHECK_THROW(kvMap.at("non-existing-key"), std::out_of_range);

    LOGS_INFO("Configuration test ends");
}

BOOST_AUTO_TEST_SUITE_END()
