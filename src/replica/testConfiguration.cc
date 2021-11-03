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
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "replica/Common.h"
#include "replica/ConfigTestData.h"
#include "replica/Configuration.h"

// Boost unit test header
#define BOOST_TEST_MODULE Configuration
#include <boost/test/unit_test.hpp>

using namespace std;
namespace test = boost::test_tools;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {
    /// The configuration is shared by all tests.
    Configuration::Ptr config;
}


BOOST_AUTO_TEST_SUITE(Suite)


BOOST_AUTO_TEST_CASE(ConfigurationInitTestJSON) {
    LOGS_INFO("Testing JSON initialization");

    BOOST_REQUIRE_NO_THROW(config = Configuration::load(ConfigTestData::data()));
    BOOST_CHECK(config != nullptr);
    BOOST_CHECK(config->configUrl().empty());
    string const configJsonStr = config->toJson().dump();
    BOOST_CHECK(!configJsonStr.empty());
}


BOOST_AUTO_TEST_CASE(ConfigurationTestDir) {
    LOGS_INFO("Testing directory functions");

    set<string> categories;
    for (auto&& itr: config->parameters()) {
        categories.insert(itr.first);
    }
    BOOST_CHECK(config->parameters() == ConfigTestData::parameters());
}


BOOST_AUTO_TEST_CASE(ConfigurationTestReadingGeneralParameters) {
    LOGS_INFO("Testing reading general parameters");

    // Fetching values of general parameters.
    BOOST_CHECK(config->get<size_t>("common", "request_buf_size_bytes") == 8192);
    BOOST_CHECK(config->get<unsigned int>("common", "request_retry_interval_sec") == 1);

    BOOST_CHECK(config->get<size_t>("controller", "num_threads") == 2);
    BOOST_CHECK(config->get<uint16_t>("controller", "http_server_port") == 8080);
    BOOST_CHECK(config->get<unsigned int>("controller", "http_max_listen_conn") == 256);
    BOOST_CHECK(config->get<size_t>("controller", "http_server_threads") == 3);
    BOOST_CHECK(config->get<unsigned int>("controller", "request_timeout_sec") == 100);
    BOOST_CHECK(config->get<string>("controller", "empty_chunks_dir") == "/qserv/data/qserv");
    BOOST_CHECK(config->get<unsigned int>("controller", "job_timeout_sec") == 200);
    BOOST_CHECK(config->get<unsigned int>("controller", "job_heartbeat_sec") == 300);

    BOOST_CHECK(config->get<unsigned int>("xrootd", "auto_notify") == 0);
    BOOST_CHECK(config->get<string>("xrootd", "host") == "localhost");
    BOOST_CHECK(config->get<uint16_t>("xrootd", "port") == 1104);
    BOOST_CHECK(config->get<unsigned int>("xrootd", "request_timeout_sec") == 400);

    BOOST_CHECK(config->get<string>("database", "host") == "localhost");
    BOOST_CHECK(config->get<uint16_t>("database", "port") == 13306);
    BOOST_CHECK(config->get<string>("database", "user") == "qsreplica");
    BOOST_CHECK(config->get<string>("database", "password") == "changeme");
    BOOST_CHECK(config->get<string>("database", "name") == "qservReplica");

    BOOST_CHECK(config->get<string>("database", "qserv_master_user") == "qsmaster");
    BOOST_CHECK(config->qservCzarDbUrl() == "mysql://qsreplica@localhost:3306/qservMeta");
    BOOST_CHECK(config->qservWorkerDbUrl() == "mysql://qsreplica@localhost:3306/qservw_worker");

    BOOST_CHECK(config->get<size_t>("database", "services_pool_size") == 2);

    BOOST_CHECK(config->get<string>("worker", "technology") == "POSIX");
    BOOST_CHECK(config->get<size_t>("worker", "num_svc_processing_threads") == 4);
    BOOST_CHECK(config->get<size_t>("worker", "num_fs_processing_threads") == 5);
    BOOST_CHECK(config->get<size_t>("worker", "fs_buf_size_bytes") == 1024);
    BOOST_CHECK(config->get<size_t>("worker", "num_loader_processing_threads") == 6);
    BOOST_CHECK(config->get<size_t>("worker", "num_exporter_processing_threads") == 7);
    BOOST_CHECK(config->get<size_t>("worker", "num_http_loader_processing_threads") == 8);
    BOOST_CHECK(config->get<size_t>("worker", "num_async_loader_processing_threads") == 9);
    BOOST_CHECK(config->get<size_t>("worker", "async_loader_auto_resume") == 0);
    BOOST_CHECK(config->get<size_t>("worker", "async_loader_cleanup_on_resume") == 0);
    BOOST_CHECK(config->get<unsigned int>("worker", "http_max_listen_conn") == 512);
}


BOOST_AUTO_TEST_CASE(ConfigurationTestModifyingGeneralParameters) {
    LOGS_INFO("Testing modifying general parameters");
 
    BOOST_CHECK_THROW(config->set<size_t>("common", "request_buf_size_bytes", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("common", "request_buf_size_bytes", 8193));
    BOOST_CHECK(config->get<size_t>("common", "request_buf_size_bytes") == 8193);

    BOOST_CHECK_THROW(config->set<unsigned int>("common", "request_retry_interval_sec", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("common", "request_retry_interval_sec", 2));
    BOOST_CHECK(config->get<unsigned int>("common", "request_retry_interval_sec") == 2);

    BOOST_CHECK_THROW(config->set<size_t>("controller", "num_threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("controller", "num_threads", 3));
    BOOST_CHECK(config->get<size_t>("controller", "num_threads") == 3);

    BOOST_CHECK_THROW(config->set<uint16_t>("controller", "http_server_port", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<uint16_t>("controller", "http_server_port", 8081));
    BOOST_CHECK(config->get<uint16_t>("controller", "http_server_port") == 8081);

    BOOST_CHECK_THROW(config->set<unsigned int>("controller", "http_max_listen_conn", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "http_max_listen_conn", 1024));
    BOOST_CHECK(config->get<unsigned int>("controller", "http_max_listen_conn") == 1024);

    BOOST_CHECK_THROW(config->set<size_t>("controller", "http_server_threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("controller", "http_server_threads", 4));
    BOOST_CHECK(config->get<size_t>("controller", "http_server_threads") == 4);

    BOOST_CHECK_THROW(config->set<unsigned int>("controller", "request_timeout_sec", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "request_timeout_sec", 101));
    BOOST_CHECK(config->get<unsigned int>("controller", "request_timeout_sec") == 101);

    BOOST_CHECK_THROW(config->set<unsigned int>("controller", "job_timeout_sec", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "job_timeout_sec", 201));
    BOOST_CHECK(config->get<unsigned int>("controller", "job_timeout_sec") == 201);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "job_heartbeat_sec", 301));
    BOOST_CHECK(config->get<unsigned int>("controller", "job_heartbeat_sec") == 301);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "job_heartbeat_sec", 0));
    BOOST_CHECK(config->get<unsigned int>("controller", "job_heartbeat_sec") == 0);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "auto_notify", 1));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "auto_notify") != 0);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "auto_notify", 0));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "auto_notify") == 0);

    BOOST_CHECK_THROW(config->set<string>("xrootd", "host", ""), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<string>("xrootd", "host", "localhost"));
    BOOST_CHECK(config->get<string>("xrootd", "host") == "localhost");

    BOOST_CHECK_THROW(config->set<uint16_t>("xrootd", "port", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<uint16_t>("xrootd", "port", 1105));
    BOOST_CHECK(config->get<uint16_t>("xrootd", "port") == 1105);

    BOOST_CHECK_THROW(config->set<unsigned int>("xrootd", "request_timeout_sec", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "request_timeout_sec", 401));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "request_timeout_sec") == 401);

    BOOST_CHECK_THROW(config->set<size_t>("database", "services_pool_size", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("database", "services_pool_size", 3));
    BOOST_CHECK(config->get<size_t>("database", "services_pool_size") == 3);

    BOOST_CHECK_THROW(config->set<string>("worker", "technology", ""), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<string>("worker", "technology", "FS"));
    BOOST_CHECK(config->get<string>("worker", "technology") == "FS");

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num_svc_processing_threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num_svc_processing_threads", 5));
    BOOST_CHECK(config->get<size_t>("worker", "num_svc_processing_threads") == 5);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num_fs_processing_threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num_fs_processing_threads", 6));
    BOOST_CHECK(config->get<size_t>("worker", "num_fs_processing_threads") == 6);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "fs_buf_size_bytes", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "fs_buf_size_bytes", 1025));
    BOOST_CHECK(config->get<size_t>("worker", "fs_buf_size_bytes") == 1025);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num_loader_processing_threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num_loader_processing_threads", 7));
    BOOST_CHECK(config->get<size_t>("worker", "num_loader_processing_threads") == 7);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num_exporter_processing_threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num_exporter_processing_threads", 8));
    BOOST_CHECK(config->get<size_t>("worker", "num_exporter_processing_threads") == 8);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num_http_loader_processing_threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num_http_loader_processing_threads", 9));
    BOOST_CHECK(config->get<size_t>("worker", "num_http_loader_processing_threads") == 9);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num_async_loader_processing_threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num_async_loader_processing_threads", 10));
    BOOST_CHECK(config->get<size_t>("worker", "num_async_loader_processing_threads") == 10);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "async_loader_auto_resume", 1));
    BOOST_CHECK(config->get<unsigned int>("worker", "async_loader_auto_resume") != 0);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "async_loader_auto_resume", 0));
    BOOST_CHECK(config->get<unsigned int>("worker", "async_loader_auto_resume") == 0);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "async_loader_cleanup_on_resume", 1));
    BOOST_CHECK(config->get<unsigned int>("worker", "async_loader_cleanup_on_resume") != 0);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "async_loader_cleanup_on_resume", 0));
    BOOST_CHECK(config->get<unsigned int>("worker", "async_loader_cleanup_on_resume") == 0);

    BOOST_CHECK_THROW(config->set<unsigned int>("worker", "http_max_listen_conn", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "http_max_listen_conn", 2048));
    BOOST_CHECK(config->get<unsigned int>("worker", "http_max_listen_conn") == 2048);
}


BOOST_AUTO_TEST_CASE(ConfigurationTestWorkers) {
    LOGS_INFO("Testing worker services");

    // Default assumptions for optional parameters of the workers selector.
    vector<string> workers1;
    BOOST_REQUIRE_NO_THROW(workers1 = config->workers());
    BOOST_CHECK(workers1.size() == 1);
    BOOST_CHECK(workers1 == vector<string>({"worker-A"}));

    // Explicit values of the worker selectors.
    bool isEnabled  = true;
    bool isReadOnly = false;
    vector<string> workers2;
    BOOST_REQUIRE_NO_THROW(workers2 = config->workers(isEnabled, isReadOnly));
    sort(workers2.begin(), workers2.end());
    BOOST_CHECK(workers2.size() == 1);
    BOOST_CHECK(workers2 == workers1);
 
    // Fetch names of all the read-only workers.
    isEnabled  = true;
    isReadOnly = true;
    vector<string> workers3;
    BOOST_REQUIRE_NO_THROW(workers3 = config->workers(isEnabled, isReadOnly));
    sort(workers3.begin(), workers3.end());
    BOOST_CHECK(workers3.size() == 1);
    BOOST_CHECK(workers3 == vector<string>({"worker-B"}));
 
    // Fetch names of all the disabled workers.
    isEnabled  = false;
    vector<string> workers4;
    BOOST_REQUIRE_NO_THROW(workers4 = config->workers(isEnabled));
    sort(workers4.begin(), workers4.end());
    BOOST_CHECK(workers4.size() == 1);
    BOOST_CHECK(workers4 == vector<string>({"worker-C"}));

    for (auto&& name: vector<string>({"worker-A", "worker-B", "worker-C"})) {
        BOOST_CHECK(config->isKnownWorker(name));
    }
}


BOOST_AUTO_TEST_CASE(ConfigurationTestWorkerParameters) {
    LOGS_INFO("Testing worker parameters");

    WorkerInfo workerA;
    BOOST_REQUIRE_NO_THROW(workerA = config->workerInfo("worker-A"));
    BOOST_CHECK(workerA.name =="worker-A");
    BOOST_CHECK(workerA.isEnabled);
    BOOST_CHECK(!workerA.isReadOnly);
    BOOST_CHECK(workerA.svcHost == "host-A");
    BOOST_CHECK(workerA.svcPort == 51001);
    BOOST_CHECK(workerA.fsHost == "host-A");
    BOOST_CHECK(workerA.fsPort == 52001);
    BOOST_CHECK(workerA.dataDir == "/data/A");
    BOOST_CHECK(workerA.loaderHost == "host-A");
    BOOST_CHECK(workerA.loaderPort == 53002);
    BOOST_CHECK(workerA.loaderTmpDir == "/tmp/A");
    BOOST_CHECK(workerA.exporterHost == "host-A");
    BOOST_CHECK(workerA.exporterPort == 53003);
    BOOST_CHECK(workerA.exporterTmpDir == "/tmp/export/A");
    BOOST_CHECK(workerA.httpLoaderHost == "host-A");
    BOOST_CHECK(workerA.httpLoaderPort == 53004);
    BOOST_CHECK(workerA.httpLoaderTmpDir == "/tmp/http/A");

    WorkerInfo workerB;
    BOOST_REQUIRE_NO_THROW(workerB = config->workerInfo("worker-B"));
    BOOST_CHECK(workerB.name =="worker-B");
    BOOST_CHECK(workerB.isEnabled);
    BOOST_CHECK(workerB.isReadOnly);
    BOOST_CHECK(workerB.svcHost == "host-B");
    BOOST_CHECK(workerB.svcPort == 51000);
    BOOST_CHECK(workerB.fsHost == "host-B");
    BOOST_CHECK(workerB.fsPort == 52000);
    BOOST_CHECK(workerB.dataDir == "/data/B");
    BOOST_CHECK(workerB.loaderHost == "host-B");
    BOOST_CHECK(workerB.loaderPort == 53000);
    BOOST_CHECK(workerB.loaderTmpDir == "/tmp");
    BOOST_CHECK(workerB.exporterHost == "host-B");
    BOOST_CHECK(workerB.exporterPort == 54000);
    BOOST_CHECK(workerB.exporterTmpDir == "/tmp");
    BOOST_CHECK(workerB.httpLoaderHost == "host-B");
    BOOST_CHECK(workerB.httpLoaderPort == 55000);
    BOOST_CHECK(workerB.httpLoaderTmpDir == "/tmp");

    WorkerInfo workerC;
    BOOST_REQUIRE_NO_THROW(workerC = config->workerInfo("worker-C"));
    BOOST_CHECK(workerC.name =="worker-C");
    BOOST_CHECK(!workerC.isEnabled);
    BOOST_CHECK(workerC.svcHost == "host-C");
    BOOST_CHECK(workerC.svcPort == 51000);
    BOOST_CHECK(workerC.fsHost == "host-C");
    BOOST_CHECK(workerC.fsPort == 52000);
    BOOST_CHECK(workerC.dataDir == "/data");
    BOOST_CHECK(workerC.loaderHost  == "host-C");
    BOOST_CHECK(workerC.loaderPort == 53000);
    BOOST_CHECK(workerC.loaderTmpDir == "/tmp");
    BOOST_CHECK(workerC.exporterHost == "host-C");
    BOOST_CHECK(workerC.exporterPort == 54000);
    BOOST_CHECK(workerC.exporterTmpDir == "/tmp");
    BOOST_CHECK(workerC.httpLoaderHost == "host-C");
    BOOST_CHECK(workerC.httpLoaderPort == 55000);
    BOOST_CHECK(workerC.httpLoaderTmpDir == "/tmp");

    // Adding a new worker with well formed and unique parameters.
    WorkerInfo workerD;
    workerD.name = "worker-D";
    workerD.isEnabled = true;
    workerD.isReadOnly = true;
    workerD.svcHost = "host-D";
    workerD.svcPort = 51001;
    workerD.fsHost = "host-D";
    workerD.fsPort = 52001;
    workerD.dataDir = "/data/D";
    workerD.loaderHost = "host-D";
    workerD.loaderPort = 52002;
    workerD.loaderTmpDir = "/tmp/D";
    workerD.exporterHost = "host-D";
    workerD.exporterPort = 52003;
    workerD.exporterTmpDir = "/tmp/D";
    workerD.httpLoaderHost = "host-D";
    workerD.httpLoaderPort = 52004;
    workerD.httpLoaderTmpDir = "/tmp/http/D";
    BOOST_REQUIRE_NO_THROW(config->addWorker(workerD));
    BOOST_CHECK_THROW(config->addWorker(workerD), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(workerD = config->workerInfo("worker-D"));
    BOOST_CHECK(workerD.name =="worker-D");
    BOOST_CHECK(workerD.isEnabled);
    BOOST_CHECK(workerD.isReadOnly);
    BOOST_CHECK(workerD.svcHost == "host-D");
    BOOST_CHECK(workerD.svcPort == 51001);
    BOOST_CHECK(workerD.fsHost == "host-D");
    BOOST_CHECK(workerD.fsPort == 52001);
    BOOST_CHECK(workerD.dataDir == "/data/D");
    BOOST_CHECK(workerD.loaderHost == "host-D");
    BOOST_CHECK(workerD.loaderPort == 52002);
    BOOST_CHECK(workerD.loaderTmpDir == "/tmp/D");
    BOOST_CHECK(workerD.exporterHost == "host-D");
    BOOST_CHECK(workerD.exporterPort == 52003);
    BOOST_CHECK(workerD.exporterTmpDir == "/tmp/D");
    BOOST_CHECK(workerD.httpLoaderHost == "host-D");
    BOOST_CHECK(workerD.httpLoaderPort == 52004);
    BOOST_CHECK(workerD.httpLoaderTmpDir == "/tmp/http/D");

    // Adding a new worker with parameters conflicting with the ones of
    // some existing worker.
    WorkerInfo workerE = workerD;
    workerE.name = "worker-E";
    BOOST_CHECK_THROW(config->addWorker(workerE), std::invalid_argument);

    // Adding a new worker with incomplete set of specs. The only required
    // attributes are the name of the worker and the name of a host where
    // the replication service of the worker would run. The host names where
    // other services would run will be set to be the same as the one of
    // replication serice's. The remaining unspecified attributes will be
    // pulled from the corresponding defaults for workers.
    WorkerInfo workerF;
    workerF.name = "worker-F";
    workerF.svcHost = "host-F";
    WorkerInfo addedWorkerF;
    BOOST_REQUIRE_NO_THROW(addedWorkerF = config->addWorker(workerF));
    BOOST_CHECK(addedWorkerF.name == workerF.name);
    BOOST_CHECK(addedWorkerF.isEnabled == workerF.isEnabled);
    BOOST_CHECK(addedWorkerF.isReadOnly == workerF.isReadOnly);
    BOOST_CHECK(addedWorkerF.svcHost == workerF.svcHost);
    BOOST_CHECK(addedWorkerF.svcPort == config->get<uint16_t>("worker_defaults", "svc_port"));
    BOOST_CHECK(addedWorkerF.fsHost == workerF.svcHost);
    BOOST_CHECK(addedWorkerF.fsPort == config->get<uint16_t>("worker_defaults", "fs_port"));
    BOOST_CHECK(addedWorkerF.dataDir == config->get<string>("worker_defaults", "data_dir"));
    BOOST_CHECK(addedWorkerF.loaderHost == workerF.svcHost);
    BOOST_CHECK(addedWorkerF.loaderPort == config->get<uint16_t>("worker_defaults", "loader_port"));
    BOOST_CHECK(addedWorkerF.loaderTmpDir == config->get<string>("worker_defaults", "loader_tmp_dir"));
    BOOST_CHECK(addedWorkerF.exporterHost == workerF.svcHost);
    BOOST_CHECK(addedWorkerF.exporterPort == config->get<uint16_t>("worker_defaults", "exporter_port"));
    BOOST_CHECK(addedWorkerF.exporterTmpDir == config->get<string>("worker_defaults", "exporter_tmp_dir"));
    BOOST_CHECK(addedWorkerF.httpLoaderHost == workerF.svcHost);
    BOOST_CHECK(addedWorkerF.httpLoaderPort == config->get<uint16_t>("worker_defaults", "http_loader_port"));
    BOOST_CHECK(addedWorkerF.httpLoaderTmpDir == config->get<string>("worker_defaults", "http_loader_tmp_dir"));

    // Deleting workers.
    BOOST_REQUIRE_NO_THROW(config->deleteWorker("worker-C"));
    BOOST_CHECK(!config->isKnownWorker("worker-C"));
    BOOST_CHECK_THROW(config->deleteWorker("worker-C"), std::invalid_argument);

    // Updating worker's status.
    WorkerInfo disabledWorker;
    BOOST_REQUIRE_NO_THROW(disabledWorker = config->workerInfo("worker-B"));
    disabledWorker.isEnabled = false;
    BOOST_REQUIRE_NO_THROW(disabledWorker = config->updateWorker(disabledWorker));
    BOOST_CHECK(disabledWorker.name == "worker-B");
    BOOST_CHECK(!disabledWorker.isEnabled);

    WorkerInfo enabledWorker;
    BOOST_REQUIRE_NO_THROW(enabledWorker = config->workerInfo("worker-B"));
    enabledWorker.isEnabled = true;
    BOOST_REQUIRE_NO_THROW(enabledWorker = config->updateWorker(enabledWorker));
    BOOST_CHECK(enabledWorker.name == "worker-B");
    BOOST_CHECK(enabledWorker.isEnabled);

    BOOST_REQUIRE_NO_THROW(disabledWorker = config->disableWorker("worker-B"));
    BOOST_CHECK(disabledWorker.name == "worker-B");
    BOOST_CHECK(!disabledWorker.isEnabled);

    WorkerInfo readOnlyWorker;
    BOOST_REQUIRE_NO_THROW(readOnlyWorker = config->workerInfo("worker-B"));
    readOnlyWorker.isReadOnly = true;
    BOOST_REQUIRE_NO_THROW(readOnlyWorker = config->updateWorker(readOnlyWorker));
    BOOST_CHECK(readOnlyWorker.name == "worker-B");
    BOOST_CHECK(readOnlyWorker.isReadOnly);

    WorkerInfo readWriteWorker;
    BOOST_REQUIRE_NO_THROW(readWriteWorker = config->workerInfo("worker-B"));
    readWriteWorker.isReadOnly = false;
    BOOST_REQUIRE_NO_THROW(readWriteWorker = config->updateWorker(readWriteWorker));
    BOOST_CHECK(readWriteWorker.name == "worker-B");
    BOOST_CHECK(!readWriteWorker.isReadOnly);

    WorkerInfo updatedWorker;
    BOOST_REQUIRE_NO_THROW(updatedWorker = config->workerInfo("worker-A"));
    updatedWorker.svcHost = "host-A1";
    updatedWorker.svcPort = 1;
    updatedWorker.fsHost = "host-A1";
    updatedWorker.fsPort = 2;
    updatedWorker.dataDir = "/test";
    updatedWorker.loaderHost = "host-A1";
    updatedWorker.loaderPort = 4;
    updatedWorker.loaderTmpDir = "/tmp/A1";
    updatedWorker.exporterHost = "host-A1";
    updatedWorker.exporterPort = 5;
    updatedWorker.exporterTmpDir = "/tmp/A1";
    updatedWorker.httpLoaderHost = "host-A1";
    updatedWorker.httpLoaderPort = 6;
    updatedWorker.httpLoaderTmpDir = "/tmp/A1";
    BOOST_REQUIRE_NO_THROW(updatedWorker = config->updateWorker(updatedWorker));
    BOOST_CHECK(updatedWorker.svcHost == "host-A1");
    BOOST_CHECK(updatedWorker.svcPort == 1);
    BOOST_CHECK(updatedWorker.fsHost == "host-A1");
    BOOST_CHECK(updatedWorker.fsPort == 2);
    BOOST_CHECK(updatedWorker.dataDir == "/test");
    BOOST_CHECK(updatedWorker.loaderHost == "host-A1");
    BOOST_CHECK(updatedWorker.loaderPort == 4);
    BOOST_CHECK(updatedWorker.loaderTmpDir == "/tmp/A1");
    BOOST_CHECK(updatedWorker.exporterHost == "host-A1");
    BOOST_CHECK(updatedWorker.exporterPort == 5);
    BOOST_CHECK(updatedWorker.exporterTmpDir == "/tmp/A1");
    BOOST_CHECK(updatedWorker.httpLoaderHost == "host-A1");
    BOOST_CHECK(updatedWorker.httpLoaderPort == 6);
    BOOST_CHECK(updatedWorker.httpLoaderTmpDir == "/tmp/A1");
}


BOOST_AUTO_TEST_CASE(ConfigurationTestFamilies) {
    LOGS_INFO("Testing database families");

    // Selecting and probing database families.
    vector<string> families;
    BOOST_REQUIRE_NO_THROW(families = config->databaseFamilies());
    sort(families.begin(), families.end());
    BOOST_CHECK(families.size() == 2);
    BOOST_CHECK(families == vector<string>({"production", "test"}));
    for (auto&& name: families) {
        BOOST_CHECK(config->isKnownDatabaseFamily(name));
    }
    DatabaseFamilyInfo production;
    BOOST_REQUIRE_NO_THROW(production = config->databaseFamilyInfo("production"));
    BOOST_CHECK(production.name == "production");
    BOOST_CHECK(production.replicationLevel == 10);
    BOOST_CHECK(production.numStripes == 11);
    BOOST_CHECK(production.numSubStripes == 12);
    BOOST_CHECK(abs(production.overlap - 0.01667) <= numeric_limits<double>::epsilon());
    DatabaseFamilyInfo test;
    BOOST_REQUIRE_NO_THROW(test = config->databaseFamilyInfo("test"));
    BOOST_CHECK(test.name == "test");
    BOOST_CHECK(test.replicationLevel == 13);
    BOOST_CHECK(test.numStripes == 14);
    BOOST_CHECK(test.numSubStripes == 15);
    BOOST_CHECK(abs(test.overlap - 0.001) <= numeric_limits<double>::epsilon());
    BOOST_CHECK(config->replicationLevel("production") == 10);
    BOOST_CHECK(config->replicationLevel("test") == 13);

    // Adding new families.
    DatabaseFamilyInfo newFamily;
    newFamily.name = "new";
    newFamily.replicationLevel = 300;
    newFamily.numStripes = 301;
    newFamily.numSubStripes = 302;
    newFamily.overlap = 0.001;
    DatabaseFamilyInfo newFamilyAdded;
    BOOST_CHECK(!config->isKnownDatabaseFamily("new"));
    BOOST_REQUIRE_NO_THROW(newFamilyAdded = config->addDatabaseFamily(newFamily));
    BOOST_CHECK(config->isKnownDatabaseFamily("new"));
    BOOST_CHECK(newFamilyAdded.name == "new");
    BOOST_CHECK(newFamilyAdded.replicationLevel == 300);
    BOOST_CHECK(newFamilyAdded.numStripes == 301);
    BOOST_CHECK(newFamilyAdded.numSubStripes == 302);
    BOOST_CHECK(abs(newFamilyAdded.overlap - 0.001) <= numeric_limits<double>::epsilon());

    // Deleting existing families,
    BOOST_REQUIRE_NO_THROW(config->deleteDatabaseFamily("new"));
    BOOST_CHECK(!config->isKnownDatabaseFamily("new"));

    // Deleting non-existing families.
    BOOST_REQUIRE_THROW(config->deleteDatabaseFamily(""), std::invalid_argument);
    BOOST_REQUIRE_THROW(config->deleteDatabaseFamily("non-existing"), std::invalid_argument);

}


BOOST_AUTO_TEST_CASE(ConfigurationTestReadingDatabases) {
    LOGS_INFO("Testing reading databases");

    // Database selectors. 
    vector<string> databases1;
    BOOST_REQUIRE_NO_THROW(databases1 = config->databases());
    sort(databases1.begin(), databases1.end());
    BOOST_CHECK(databases1.size() == 5);
    BOOST_CHECK(databases1 == vector<string>({"db1", "db2", "db3", "db4", "db5"}));
    vector<string> databases2;
    BOOST_REQUIRE_NO_THROW(databases2 = config->databases("production"));
    sort(databases2.begin(), databases2.end());
    BOOST_CHECK(databases2.size() == 3);
    BOOST_CHECK(databases2 == vector<string>({"db1", "db2", "db3"}));
    vector<string> databases3;
    BOOST_REQUIRE_NO_THROW(databases3 = config->databases("test"));
    sort(databases3.begin(), databases3.end());
    BOOST_CHECK(databases3.size() == 2);
    BOOST_CHECK(databases3 == vector<string>({"db4", "db5"}));
    bool allDatabases = false;
    bool isPublished = true;
    vector<string> databases4;
    BOOST_REQUIRE_NO_THROW(databases4 = config->databases("test", allDatabases, isPublished));
    sort(databases4.begin(), databases4.end());
    BOOST_CHECK(databases4.size() == 2);
    BOOST_CHECK(databases4 == vector<string>({"db4", "db5"}));
    isPublished = false;
    vector<string> databases5;
    BOOST_REQUIRE_NO_THROW(databases5 = config->databases("test", allDatabases, isPublished));
    sort(databases5.begin(), databases5.end());
    BOOST_CHECK(databases5.size() == 1);
    BOOST_CHECK(databases5 == vector<string>({"db6"}));
    allDatabases = true;
    vector<string> databases6;
    BOOST_REQUIRE_NO_THROW(databases6 = config->databases("test", allDatabases));
    sort(databases6.begin(), databases6.end());
    BOOST_CHECK(databases6.size() == 3);
    BOOST_CHECK(databases6 == vector<string>({"db4", "db5", "db6"}));
    isPublished = true;
    vector<string> databases7;
    BOOST_REQUIRE_NO_THROW(databases7 = config->databases("test", allDatabases, isPublished));
    sort(databases7.begin(), databases7.end());
    BOOST_CHECK(databases7.size() == 3);
    BOOST_CHECK(databases7 == vector<string>({"db4", "db5", "db6"}));
    isPublished = false;
    vector<string> databases8;
    BOOST_REQUIRE_NO_THROW(databases8 = config->databases("test", allDatabases, isPublished));
    sort(databases8.begin(), databases8.end());
    BOOST_CHECK(databases8.size() == 3);
    BOOST_CHECK(databases8 == vector<string>({"db4", "db5", "db6"}));
    for (auto&& name: vector<string>({"db1", "db2", "db3", "db4", "db5", "db6"})) {
        BOOST_CHECK(config->isKnownDatabase(name));  
    }
}


BOOST_AUTO_TEST_CASE(ConfigurationTestReadingTables) {
    LOGS_INFO("Testing reading tables");

    vector<string> tables;
    DatabaseInfo db1info;
    BOOST_REQUIRE_NO_THROW(db1info = config->databaseInfo("db1"));
    BOOST_CHECK(db1info.name == "db1");
    BOOST_CHECK(db1info.family == "production");
    BOOST_CHECK(db1info.isPublished == true);
    BOOST_CHECK(db1info.directorTable.count("Table11") != 0);
    BOOST_CHECK(db1info.directorTable.at("Table11").empty());
    BOOST_CHECK(db1info.directorTableKey.count("Table11") != 0);
    BOOST_CHECK(db1info.directorTableKey.at("Table11") == "id11");
    BOOST_CHECK(db1info.latitudeColName.count("Table11") != 0);
    BOOST_CHECK(db1info.latitudeColName.at("Table11") == "decl11");
    BOOST_CHECK(db1info.longitudeColName.count("Table11") != 0);
    BOOST_CHECK(db1info.longitudeColName.at("Table11") == "ra11");

    tables = db1info.partitionedTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"Table11"}));
    BOOST_CHECK(db1info.isPartitioned("Table11"));
    BOOST_CHECK(db1info.isDirector("Table11"));

    tables = db1info.directorTables();
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"Table11"}));

    tables = db1info.regularTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"MetaTable11"}));
    BOOST_CHECK(!db1info.isPartitioned("MetaTable11"));
    BOOST_CHECK(!db1info.isDirector("MetaTable11"));
    BOOST_CHECK(!db1info.directorTable.count("MetaTable11"));
    BOOST_CHECK(!db1info.directorTableKey.count("MetaTable11"));
    BOOST_CHECK(!db1info.latitudeColName.count("MetaTable11"));
    BOOST_CHECK(!db1info.longitudeColName.count("MetaTable11"));

    DatabaseInfo db2info;
    BOOST_REQUIRE_NO_THROW(db2info = config->databaseInfo("db2"));
    BOOST_CHECK(db2info.name == "db2");
    BOOST_CHECK(db2info.family == "production");
    BOOST_CHECK(db2info.isPublished == true);
    BOOST_CHECK(db2info.isDirector("Table21"));
    BOOST_CHECK(db2info.directorTable.count("Table21") != 0);
    BOOST_CHECK(db2info.directorTable.at("Table21").empty());
    BOOST_CHECK(db2info.directorTableKey.count("Table21") != 0);
    BOOST_CHECK(db2info.directorTableKey.at("Table21") == "id21");
    BOOST_CHECK(db2info.latitudeColName.count("Table21") != 0);
    BOOST_CHECK(db2info.latitudeColName.at("Table21") == "decl21");
    BOOST_CHECK(db2info.longitudeColName.count("Table21") != 0);
    BOOST_CHECK(db2info.longitudeColName.at("Table21") == "ra21");
    BOOST_CHECK(!db2info.isDirector("Table22"));
    BOOST_CHECK(db2info.directorTable.count("Table22") != 0);
    BOOST_CHECK(db2info.directorTable.at("Table22") == "Table21");
    BOOST_CHECK(db2info.directorTableKey.count("Table22") != 0);
    BOOST_CHECK(db2info.directorTableKey.at("Table22") == "id22");
    BOOST_CHECK(db2info.latitudeColName.count("Table22") != 0);
    BOOST_CHECK(db2info.latitudeColName.at("Table22") == "decl22");
    BOOST_CHECK(db2info.longitudeColName.count("Table22") != 0);
    BOOST_CHECK(db2info.longitudeColName.at("Table22") == "ra22");

    tables = db2info.partitionedTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 2);
    BOOST_CHECK(tables == vector<string>({"Table21", "Table22"}));

    tables = db2info.directorTables();
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"Table21"}));

    tables = db2info.regularTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 2);
    BOOST_CHECK(tables == vector<string>({"MetaTable21", "MetaTable22"}));

    DatabaseInfo db3info;
    BOOST_REQUIRE_NO_THROW(db3info = config->databaseInfo("db3"));
    BOOST_CHECK(db3info.name == "db3");
    BOOST_CHECK(db3info.family == "production");
    BOOST_CHECK(db3info.isPublished == true);
    BOOST_CHECK(db3info.isDirector("Table31"));
    BOOST_CHECK(db3info.directorTable.count("Table31") != 0);
    BOOST_CHECK(db3info.directorTable.at("Table31").empty());
    BOOST_CHECK(db3info.directorTableKey.count("Table31") != 0);
    BOOST_CHECK(db3info.directorTableKey.at("Table31") == "id31");
    BOOST_CHECK(db3info.latitudeColName.count("Table31") != 0);
    BOOST_CHECK(db3info.latitudeColName.at("Table31") == "decl31");
    BOOST_CHECK(db3info.longitudeColName.count("Table31") != 0);
    BOOST_CHECK(db3info.longitudeColName.at("Table31") == "ra31");
    BOOST_CHECK(!db3info.isDirector("Table32"));
    BOOST_CHECK(db3info.directorTable.count("Table32") != 0);
    BOOST_CHECK(db3info.directorTable.at("Table32") == "Table31");
    BOOST_CHECK(db3info.directorTableKey.count("Table32") != 0);
    BOOST_CHECK(db3info.directorTableKey.at("Table32") == "id32");
    BOOST_CHECK(db3info.latitudeColName.count("Table32") != 0);
    BOOST_CHECK(db3info.latitudeColName.at("Table32") == "decl32");
    BOOST_CHECK(db3info.longitudeColName.count("Table32") != 0);
    BOOST_CHECK(db3info.longitudeColName.at("Table32") == "ra32");
    BOOST_CHECK(!db3info.isDirector("Table33"));
    BOOST_CHECK(db3info.directorTable.count("Table33") != 0);
    BOOST_CHECK(db3info.directorTable.at("Table33") == "Table31");
    BOOST_CHECK(db3info.directorTableKey.count("Table33") != 0);
    BOOST_CHECK(db3info.directorTableKey.count("Table33") != 0);
    BOOST_CHECK(db3info.directorTableKey.at("Table33") == "id33");
    BOOST_CHECK(db3info.latitudeColName.count("Table33") != 0);
    BOOST_CHECK(db3info.latitudeColName.at("Table33").empty());
    BOOST_CHECK(db3info.longitudeColName.count("Table33") != 0 );
    BOOST_CHECK(db3info.longitudeColName.at("Table33").empty());

    tables = db3info.partitionedTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 3);
    BOOST_CHECK(tables == vector<string>({"Table31", "Table32", "Table33"}));

    tables = db3info.directorTables();
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"Table31"}));

    tables = db3info.regularTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 3);
    BOOST_CHECK(tables == vector<string>({"MetaTable31", "MetaTable32", "MetaTable33"}));

    DatabaseInfo db4info;
    BOOST_REQUIRE_NO_THROW(db4info = config->databaseInfo("db4"));
    BOOST_CHECK(db4info.name == "db4");
    BOOST_CHECK(db4info.family == "test");
    BOOST_CHECK(db4info.isPublished == true);

    BOOST_CHECK(db4info.isDirector("Table41"));
    BOOST_CHECK(db4info.directorTable.count("Table41") != 0);
    BOOST_CHECK(db4info.directorTable.at("Table41").empty());
    BOOST_CHECK(db4info.directorTableKey.count("Table41") != 0);
    BOOST_CHECK(db4info.directorTableKey.at("Table41") == "id41");
    BOOST_CHECK(db4info.latitudeColName.count("Table41") != 0);
    BOOST_CHECK(db4info.latitudeColName.at("Table41") == "decl41");
    BOOST_CHECK(db4info.longitudeColName.count("Table41") != 0);
    BOOST_CHECK(db4info.longitudeColName.at("Table41") == "ra41");
    BOOST_CHECK(db4info.isDirector("Table42"));
    BOOST_CHECK(db4info.directorTable.count("Table42") != 0);
    BOOST_CHECK(db4info.directorTable.at("Table42").empty());
    BOOST_CHECK(db4info.directorTableKey.count("Table42") != 0);
    BOOST_CHECK(db4info.directorTableKey.at("Table42") == "id42");
    BOOST_CHECK(db4info.latitudeColName.count("Table42") != 0);
    BOOST_CHECK(db4info.latitudeColName.at("Table42") == "decl42");
    BOOST_CHECK(db4info.longitudeColName.count("Table42") != 0);
    BOOST_CHECK(db4info.longitudeColName.at("Table42") == "ra42");

    tables = db4info.partitionedTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 2);
    BOOST_CHECK(tables == vector<string>({"Table41", "Table42"}));

    tables = db4info.directorTables();
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 2);
    BOOST_CHECK(tables == vector<string>({"Table41", "Table42"}));

    tables = db4info.regularTables;
    BOOST_CHECK(tables.size() == 0);

    DatabaseInfo db5info;
    BOOST_REQUIRE_NO_THROW(db5info = config->databaseInfo("db5"));
    BOOST_CHECK(db5info.name == "db5");
    BOOST_CHECK(db5info.family == "test");
    BOOST_CHECK(db5info.isPublished == true);
    BOOST_CHECK(db5info.isDirector("Table51"));
    BOOST_CHECK(db5info.directorTable.count("Table51") != 0);
    BOOST_CHECK(db5info.directorTable.at("Table51").empty());
    BOOST_CHECK(db5info.directorTableKey.count("Table51") != 0);
    BOOST_CHECK(db5info.directorTableKey.at("Table51") == "id51");
    BOOST_CHECK(db5info.latitudeColName.count("Table51") != 0);
    BOOST_CHECK(db5info.latitudeColName.at("Table51") == "decl51");
    BOOST_CHECK(db5info.longitudeColName.count("Table51") != 0);
    BOOST_CHECK(db5info.longitudeColName.at("Table51") == "ra51");

    tables = db5info.partitionedTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"Table51"}));

    tables = db5info.directorTables();
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"Table51"}));


    tables = db5info.regularTables;
    BOOST_CHECK(tables.size() == 0);

    DatabaseInfo db6info;
    BOOST_REQUIRE_NO_THROW(db6info = config->databaseInfo("db6"));
    BOOST_CHECK(db6info.name == "db6");
    BOOST_CHECK(db6info.family == "test");
    BOOST_CHECK(db6info.isPublished == false);
    BOOST_CHECK(db6info.isDirector("Table61"));
    BOOST_CHECK(db6info.directorTable.count("Table61") != 0);
    BOOST_CHECK(db6info.directorTable.at("Table61").empty());
    BOOST_CHECK(db6info.directorTableKey.count("Table61") != 0);
    BOOST_CHECK(db6info.directorTableKey.at("Table61") == "id61");
    BOOST_CHECK(db6info.latitudeColName.count("Table61") != 0);
    BOOST_CHECK(db6info.latitudeColName.at("Table61") == "decl61");
    BOOST_CHECK(db6info.longitudeColName.count("Table61") != 0);
    BOOST_CHECK(db6info.longitudeColName.at("Table61") == "ra61");

    tables = db6info.partitionedTables;
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"Table61"}));

    tables = db6info.directorTables();
    sort(tables.begin(), tables.end());
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"Table61"}));

    tables = db6info.regularTables;
    BOOST_CHECK(tables.size() == 1);
    BOOST_CHECK(tables == vector<string>({"MetaTable61"}));
}


BOOST_AUTO_TEST_CASE(ConfigurationTestAddingDatabases) {
    LOGS_INFO("Testing adding databases");

    // Adding new databases.
    {
        string const database = "new";
        string const family = "test";
        DatabaseInfo info;
        BOOST_REQUIRE_NO_THROW(info = config->addDatabase(database, family));
        BOOST_CHECK(info.name == database);
        BOOST_CHECK(info.family == family);
        BOOST_CHECK(info.isPublished == false);
        BOOST_CHECK(info.partitionedTables.empty());
        BOOST_CHECK(info.regularTables.empty());
        BOOST_CHECK(info.directorTable.empty());
        BOOST_CHECK(info.directorTableKey.empty());
        BOOST_CHECK(info.latitudeColName.empty());
        BOOST_CHECK(info.longitudeColName.empty());
        BOOST_CHECK(info.directorTables().empty());
        BOOST_CHECK_THROW(config->addDatabase(database, family), std::invalid_argument);
    }
    BOOST_CHECK_THROW(config->addDatabase("", ""), std::invalid_argument);
    BOOST_CHECK_THROW(config->addDatabase("", "unknown"), std::invalid_argument);
    BOOST_CHECK_THROW(config->addDatabase("another", ""), std::invalid_argument);
    BOOST_CHECK_THROW(config->addDatabase("another", "unknown"), std::invalid_argument);
    {
        DatabaseInfo info;
        BOOST_CHECK_THROW(info.isPartitioned("NonExistingTable"), std::invalid_argument);
        BOOST_CHECK_THROW(info.isDirector("NonExistingTable"), std::invalid_argument);
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
}


BOOST_AUTO_TEST_CASE(ConfigurationTestModifyingTables) {
    LOGS_INFO("Testing modifying tables");
    {
        bool const isPartitioned = true;
        bool const isDirectorTable = true;
        string const directorTable;
        string const directorTableKey = "objectId";
        string const latitudeColName = "lat";
        string const longitudeColName = "lon";
        list<SqlColDef> coldefs;
        coldefs.emplace_back(directorTableKey, "INT UNSIGNED");
        coldefs.emplace_back(latitudeColName, "DOUBLE");
        coldefs.emplace_back(longitudeColName, "DOUBLE");
        coldefs.emplace_back(lsst::qserv::SUB_CHUNK_COLUMN, "INT");
        DatabaseInfo info;
        BOOST_REQUIRE_NO_THROW(
            info = config->addTable("new", "T1", isPartitioned, coldefs,
                    isDirectorTable, directorTable, directorTableKey, latitudeColName, longitudeColName);
        );
        BOOST_CHECK(info.columns.count("T1") == 1);
        std::list<SqlColDef> columns;
        BOOST_REQUIRE_NO_THROW(
            columns = info.columns.at("T1");
        );
        BOOST_CHECK(columns.size() == 4);
        BOOST_CHECK(
            find_if(columns.cbegin(), columns.cend(), [&directorTableKey](SqlColDef const& coldef) {
                return (coldef.name == directorTableKey) && (coldef.type == "INT UNSIGNED");
            }) != columns.cend()
        );
        BOOST_CHECK(
            find_if(columns.cbegin(), columns.cend(), [&latitudeColName](SqlColDef const& coldef) {
                return (coldef.name == latitudeColName) && (coldef.type == "DOUBLE");
            }) != columns.cend()
        );
        BOOST_CHECK(
            find_if(columns.cbegin(), columns.cend(), [&longitudeColName](SqlColDef const& coldef) {
                return (coldef.name == longitudeColName) && (coldef.type == "DOUBLE");
            }) != columns.cend()
        );
        BOOST_CHECK(
            find_if(columns.cbegin(), columns.cend(), [](SqlColDef const& coldef) {
                return (coldef.name == lsst::qserv::SUB_CHUNK_COLUMN) && (coldef.type == "INT");
            }) != columns.cend()
        );
        BOOST_CHECK((info.partitionedTables.size() == 1) && (info.partitionedTables[0] == "T1"));
        BOOST_CHECK(info.directorTable.at("T1").empty());
        BOOST_CHECK(info.directorTableKey.at("T1") == directorTableKey);
        BOOST_CHECK(info.latitudeColName.at("T1") == latitudeColName);
        BOOST_CHECK(info.longitudeColName.at("T1") == longitudeColName);
        BOOST_CHECK(info.directorTables().size() == 1);
    }
    BOOST_CHECK_THROW(config->addTable("new", "T1", false), std::invalid_argument);
    {
        bool const isPartitioned = true;
        bool const isDirectorTable = false;
        string const directorTable = "T1";
        string const directorTableKey = "idT2";
        string const latitudeColName = "declT2";
        string const longitudeColName = "raT2";
        list<SqlColDef> coldefs;
        coldefs.emplace_back(directorTableKey, "INT UNSIGNED");
        coldefs.emplace_back(latitudeColName, "DOUBLE");
        coldefs.emplace_back(longitudeColName, "DOUBLE");
        DatabaseInfo info;
        BOOST_REQUIRE_NO_THROW(
            info = config->addTable("new", "T2", isPartitioned, coldefs,
                    isDirectorTable, directorTable, directorTableKey, latitudeColName, longitudeColName);
        );
        BOOST_CHECK(info.partitionedTables.size() == 2);
        BOOST_CHECK(info.directorTable.at("T2") == "T1");
        BOOST_CHECK(info.directorTableKey.count("T2") != 0);
        BOOST_CHECK(info.directorTableKey.at("T2") == directorTableKey);
        BOOST_CHECK(info.latitudeColName.count("T2") != 0);
        BOOST_CHECK(info.latitudeColName.at("T2") == latitudeColName);
        BOOST_CHECK(info.longitudeColName.count("T2") != 0);
        BOOST_CHECK(info.longitudeColName.at("T2") == longitudeColName);
        BOOST_CHECK(info.directorTables().size() == 1);
    }
    BOOST_CHECK_THROW(config->addTable("new", "T2", true), std::invalid_argument);
    {
        DatabaseInfo info;
        BOOST_REQUIRE_NO_THROW(info = config->addTable("new", "T3", false));
        BOOST_CHECK((info.regularTables.size() == 1) && (info.regularTables[0] == "T3"));
        BOOST_CHECK(info.directorTableKey.count("T3") == 0);
        BOOST_CHECK(info.latitudeColName.count("T3") == 0);
        BOOST_CHECK(info.longitudeColName.count("T3") == 0);
    }
    BOOST_CHECK_THROW(config->addTable("new", "T3", false), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->deleteTable("new", "T3"));

}


BOOST_AUTO_TEST_CASE(ConfigurationTestPublishingDatabases) {
    LOGS_INFO("Testing publishing databases");
    {
        DatabaseInfo info;
        BOOST_REQUIRE_NO_THROW(info = config->publishDatabase("new"));
        BOOST_CHECK(info.name == "new");
        BOOST_CHECK(info.family == "test");
        BOOST_CHECK(info.isPublished == true);
        BOOST_CHECK_EQUAL(info.partitionedTables.size(), 2U);
        BOOST_CHECK_EQUAL(info.regularTables.size(), 0U);
        BOOST_CHECK_THROW(info = config->publishDatabase("new"), std::logic_error);
    }

    // Adding tables to the database after it's published isn't allowed.
    BOOST_CHECK_THROW(config->addTable("new", "T4", true), std::invalid_argument);

    // Deleting director tables which may still have dependent ones is not allowed 
    BOOST_CHECK_THROW(config->deleteTable("new", "T1"), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->deleteTable("new", "T2"));
    // Now we can do this, after delting the dependent one.
    BOOST_REQUIRE_NO_THROW(config->deleteTable("new", "T1"));

    BOOST_REQUIRE_NO_THROW(config->deleteDatabase("new"));
    BOOST_CHECK_THROW(config->deleteDatabase("new"), std::invalid_argument);
}


BOOST_AUTO_TEST_CASE(ConfigurationTestDeletingFamilies) {
    LOGS_INFO("Testing deleting families");

    // Test if deleting a family would also eliminate the dependent databases
    BOOST_REQUIRE_NO_THROW(config->deleteDatabaseFamily("production"));
    BOOST_CHECK(!config->isKnownDatabaseFamily("production"));
    BOOST_CHECK(!config->isKnownDatabase("db1"));
    BOOST_CHECK(!config->isKnownDatabase("db2"));
    BOOST_CHECK(!config->isKnownDatabase("db3"));
    // Databases of the family "test" should not have been affected by the operation.
    BOOST_CHECK(config->isKnownDatabase("db4"));
    BOOST_CHECK(config->isKnownDatabase("db5"));
    BOOST_CHECK(config->isKnownDatabase("db6"));
}

BOOST_AUTO_TEST_SUITE_END()
