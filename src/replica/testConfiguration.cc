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
}  // namespace

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(ConfigurationTestStaticParameters) {
    LOGS_INFO("Testing static parameters");

    BOOST_CHECK_THROW(Configuration::setQservCzarDbUrl(""), std::invalid_argument);
    BOOST_CHECK_THROW(Configuration::setQservWorkerDbUrl(""), std::invalid_argument);

    BOOST_REQUIRE_NO_THROW(Configuration::setDatabaseAllowReconnect(true));
    BOOST_CHECK(Configuration::databaseAllowReconnect() == true);
    BOOST_REQUIRE_NO_THROW(Configuration::setDatabaseAllowReconnect(false));
    BOOST_CHECK(Configuration::databaseAllowReconnect() == false);

    BOOST_CHECK_THROW(Configuration::setDatabaseConnectTimeoutSec(0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(Configuration::setDatabaseConnectTimeoutSec(1));
    BOOST_CHECK(Configuration::databaseConnectTimeoutSec() == 1);

    BOOST_CHECK_THROW(Configuration::setDatabaseMaxReconnects(0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(Configuration::setDatabaseMaxReconnects(2));
    BOOST_CHECK(Configuration::databaseMaxReconnects() == 2);

    BOOST_CHECK_THROW(Configuration::setDatabaseTransactionTimeoutSec(0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(Configuration::setDatabaseTransactionTimeoutSec(3));
    BOOST_CHECK(Configuration::databaseTransactionTimeoutSec() == 3);

    BOOST_REQUIRE_NO_THROW(Configuration::setSchemaUpgradeWait(true));
    BOOST_CHECK(Configuration::schemaUpgradeWait() == true);
    BOOST_REQUIRE_NO_THROW(Configuration::setSchemaUpgradeWait(false));
    BOOST_CHECK(Configuration::schemaUpgradeWait() == false);

    BOOST_CHECK_THROW(Configuration::setSchemaUpgradeWaitTimeoutSec(0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(Configuration::setSchemaUpgradeWaitTimeoutSec(4));
    BOOST_CHECK(Configuration::schemaUpgradeWaitTimeoutSec() == 4);
}

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
    BOOST_CHECK(config->parameters() == ConfigTestData::parameters());
}

BOOST_AUTO_TEST_CASE(ConfigurationTestReadingGeneralParameters) {
    LOGS_INFO("Testing reading general parameters");

    // Fetching values of general parameters.
    BOOST_CHECK(config->get<size_t>("common", "request-buf-size-bytes") == 8192);
    BOOST_CHECK(config->get<unsigned int>("common", "request-retry-interval-sec") == 1);

    BOOST_CHECK(config->get<string>("registry", "host") == "127.0.0.1");
    BOOST_CHECK(config->get<uint16_t>("registry", "port") == 8081);
    BOOST_CHECK(config->get<unsigned int>("registry", "max-listen-conn") == 512);
    BOOST_CHECK(config->get<size_t>("registry", "threads") == 4);
    BOOST_CHECK(config->get<unsigned int>("registry", "heartbeat-ival-sec") == 10);

    BOOST_CHECK(config->get<size_t>("controller", "num-threads") == 2);
    BOOST_CHECK(config->get<uint16_t>("controller", "http-server-port") == 8080);
    BOOST_CHECK(config->get<unsigned int>("controller", "http-max-listen-conn") == 256);
    BOOST_CHECK(config->get<size_t>("controller", "http-server-threads") == 3);
    BOOST_CHECK(config->get<unsigned int>("controller", "request-timeout-sec") == 100);
    BOOST_CHECK(config->get<string>("controller", "empty-chunks-dir") == "/qserv/data/qserv");
    BOOST_CHECK(config->get<unsigned int>("controller", "job-timeout-sec") == 200);
    BOOST_CHECK(config->get<unsigned int>("controller", "job-heartbeat-sec") == 300);
    BOOST_CHECK(config->get<int>("controller", "worker-evict-priority-level") == 1);
    BOOST_CHECK(config->get<int>("controller", "health-monitor-priority-level") == 2);
    BOOST_CHECK(config->get<int>("controller", "ingest-priority-level") == 3);
    BOOST_CHECK(config->get<int>("controller", "catalog-management-priority-level") == 4);
    BOOST_CHECK(config->get<unsigned int>("controller", "auto-register-workers") == 1);
    BOOST_CHECK(config->get<unsigned int>("controller", "ingest-job-monitor-ival-sec") == 5);

    BOOST_CHECK(config->get<unsigned int>("xrootd", "auto-notify") == 0);
    BOOST_CHECK(config->get<string>("xrootd", "host") == "localhost");
    BOOST_CHECK(config->get<uint16_t>("xrootd", "port") == 1104);
    BOOST_CHECK(config->get<unsigned int>("xrootd", "request-timeout-sec") == 400);
    BOOST_CHECK(config->get<unsigned int>("xrootd", "allow-reconnect") == 0);
    BOOST_CHECK(config->get<unsigned int>("xrootd", "reconnect-timeout") == 500);

    BOOST_CHECK(config->get<string>("database", "host") == "localhost");
    BOOST_CHECK(config->get<uint16_t>("database", "port") == 13306);
    BOOST_CHECK(config->get<string>("database", "user") == "qsreplica");
    BOOST_CHECK(config->get<string>("database", "password") == "changeme");
    BOOST_CHECK(config->get<string>("database", "name") == "qservReplica");

    BOOST_CHECK(config->get<string>("database", "qserv-master-user") == "qsmaster");
    BOOST_CHECK(config->qservCzarDbUrl() == "mysql://qsmaster@localhost:3306/qservMeta");
    BOOST_CHECK(config->qservWorkerDbUrl() == "mysql://qsmaster@localhost:3306/qservw_worker");

    BOOST_CHECK(config->get<size_t>("database", "services-pool-size") == 2);

    BOOST_CHECK(config->get<string>("worker", "technology") == "POSIX");
    BOOST_CHECK(config->get<size_t>("worker", "num-svc-processing-threads") == 4);
    BOOST_CHECK(config->get<size_t>("worker", "num-fs-processing-threads") == 5);
    BOOST_CHECK(config->get<size_t>("worker", "fs-buf-size-bytes") == 1024);
    BOOST_CHECK(config->get<size_t>("worker", "num-loader-processing-threads") == 6);
    BOOST_CHECK(config->get<size_t>("worker", "num-exporter-processing-threads") == 7);
    BOOST_CHECK(config->get<size_t>("worker", "num-http-loader-processing-threads") == 8);
    BOOST_CHECK(config->get<size_t>("worker", "num-async-loader-processing-threads") == 9);
    BOOST_CHECK(config->get<size_t>("worker", "async-loader-auto-resume") == 0);
    BOOST_CHECK(config->get<size_t>("worker", "async-loader-cleanup-on-resume") == 0);
    BOOST_CHECK(config->get<unsigned int>("worker", "http-max-listen-conn") == 512);
}

BOOST_AUTO_TEST_CASE(ConfigurationTestModifyingGeneralParameters) {
    LOGS_INFO("Testing modifying general parameters");

    BOOST_CHECK_THROW(config->set<size_t>("common", "request-buf-size-bytes", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("common", "request-buf-size-bytes", 8193));
    BOOST_CHECK(config->get<size_t>("common", "request-buf-size-bytes") == 8193);

    BOOST_CHECK_THROW(config->set<unsigned int>("common", "request-retry-interval-sec", 0),
                      std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("common", "request-retry-interval-sec", 2));
    BOOST_CHECK(config->get<unsigned int>("common", "request-retry-interval-sec") == 2);

    BOOST_CHECK_THROW(config->set<string>("registry", "host", string()), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<string>("registry", "host", "localhost"));
    BOOST_CHECK(config->get<string>("registry", "host") == "localhost");

    BOOST_CHECK_THROW(config->set<uint16_t>("registry", "port", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<uint16_t>("registry", "port", 8083));
    BOOST_CHECK(config->get<uint16_t>("registry", "port") == 8083);

    BOOST_CHECK_THROW(config->set<unsigned int>("registry", "max-listen-conn", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("registry", "max-listen-conn", 1024));
    BOOST_CHECK(config->get<unsigned int>("registry", "max-listen-conn") == 1024);

    BOOST_CHECK_THROW(config->set<size_t>("registry", "threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("registry", "threads", 5));
    BOOST_CHECK(config->get<size_t>("registry", "threads") == 5);

    BOOST_CHECK_THROW(config->set<unsigned int>("registry", "heartbeat-ival-sec", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("registry", "heartbeat-ival-sec", 11));
    BOOST_CHECK(config->get<unsigned int>("registry", "heartbeat-ival-sec") == 11);

    BOOST_CHECK_THROW(config->set<size_t>("controller", "num-threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("controller", "num-threads", 3));
    BOOST_CHECK(config->get<size_t>("controller", "num-threads") == 3);

    BOOST_CHECK_THROW(config->set<uint16_t>("controller", "http-server-port", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<uint16_t>("controller", "http-server-port", 8081));
    BOOST_CHECK(config->get<uint16_t>("controller", "http-server-port") == 8081);

    BOOST_CHECK_THROW(config->set<unsigned int>("controller", "http-max-listen-conn", 0),
                      std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "http-max-listen-conn", 1024));
    BOOST_CHECK(config->get<unsigned int>("controller", "http-max-listen-conn") == 1024);

    BOOST_CHECK_THROW(config->set<size_t>("controller", "http-server-threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("controller", "http-server-threads", 4));
    BOOST_CHECK(config->get<size_t>("controller", "http-server-threads") == 4);

    BOOST_CHECK_THROW(config->set<unsigned int>("controller", "request-timeout-sec", 0),
                      std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "request-timeout-sec", 101));
    BOOST_CHECK(config->get<unsigned int>("controller", "request-timeout-sec") == 101);

    BOOST_CHECK_THROW(config->set<unsigned int>("controller", "job-timeout-sec", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "job-timeout-sec", 201));
    BOOST_CHECK(config->get<unsigned int>("controller", "job-timeout-sec") == 201);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "job-heartbeat-sec", 301));
    BOOST_CHECK(config->get<unsigned int>("controller", "job-heartbeat-sec") == 301);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "job-heartbeat-sec", 0));
    BOOST_CHECK(config->get<unsigned int>("controller", "job-heartbeat-sec") == 0);

    BOOST_REQUIRE_NO_THROW(config->set<int>("controller", "worker-evict-priority-level", 1));
    BOOST_CHECK(config->get<int>("controller", "worker-evict-priority-level") == 1);

    BOOST_REQUIRE_NO_THROW(config->set<int>("controller", "worker-evict-priority-level", 0));
    BOOST_CHECK(config->get<int>("controller", "worker-evict-priority-level") == 0);

    BOOST_REQUIRE_NO_THROW(config->set<int>("controller", "health-monitor-priority-level", 2));
    BOOST_CHECK(config->get<int>("controller", "health-monitor-priority-level") == 2);

    BOOST_REQUIRE_NO_THROW(config->set<int>("controller", "health-monitor-priority-level", 0));
    BOOST_CHECK(config->get<int>("controller", "health-monitor-priority-level") == 0);

    BOOST_REQUIRE_NO_THROW(config->set<int>("controller", "ingest-priority-level", 3));
    BOOST_CHECK(config->get<int>("controller", "ingest-priority-level") == 3);

    BOOST_REQUIRE_NO_THROW(config->set<int>("controller", "ingest-priority-level", 0));
    BOOST_CHECK(config->get<int>("controller", "ingest-priority-level") == 0);

    BOOST_REQUIRE_NO_THROW(config->set<int>("controller", "catalog-management-priority-level", 4));
    BOOST_CHECK(config->get<int>("controller", "catalog-management-priority-level") == 4);

    BOOST_REQUIRE_NO_THROW(config->set<int>("controller", "catalog-management-priority-level", 0));
    BOOST_CHECK(config->get<int>("controller", "catalog-management-priority-level") == 0);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "auto-register-workers", 0));
    BOOST_CHECK(config->get<unsigned int>("controller", "auto-register-workers") == 0);

    BOOST_CHECK_THROW(config->set<uint16_t>("controller", "ingest-job-monitor-ival-sec", 0),
                      std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("controller", "ingest-job-monitor-ival-sec", 6));
    BOOST_CHECK(config->get<unsigned int>("controller", "ingest-job-monitor-ival-sec") == 6);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "auto-notify", 1));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "auto-notify") != 0);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "auto-notify", 0));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "auto-notify") == 0);

    BOOST_CHECK_THROW(config->set<string>("xrootd", "host", ""), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<string>("xrootd", "host", "localhost"));
    BOOST_CHECK(config->get<string>("xrootd", "host") == "localhost");

    BOOST_CHECK_THROW(config->set<uint16_t>("xrootd", "port", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<uint16_t>("xrootd", "port", 1105));
    BOOST_CHECK(config->get<uint16_t>("xrootd", "port") == 1105);

    BOOST_CHECK_THROW(config->set<unsigned int>("xrootd", "request-timeout-sec", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "request-timeout-sec", 401));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "request-timeout-sec") == 401);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "allow-reconnect", 1));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "allow-reconnect") != 0);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "allow-reconnect", 0));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "allow-reconnect") == 0);

    BOOST_CHECK_THROW(config->set<unsigned int>("xrootd", "reconnect-timeout", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("xrootd", "reconnect-timeout", 403));
    BOOST_CHECK(config->get<unsigned int>("xrootd", "reconnect-timeout") == 403);

    BOOST_CHECK_THROW(config->set<size_t>("database", "services-pool-size", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("database", "services-pool-size", 3));
    BOOST_CHECK(config->get<size_t>("database", "services-pool-size") == 3);

    BOOST_CHECK_THROW(config->set<string>("worker", "technology", ""), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<string>("worker", "technology", "FS"));
    BOOST_CHECK(config->get<string>("worker", "technology") == "FS");

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num-svc-processing-threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num-svc-processing-threads", 5));
    BOOST_CHECK(config->get<size_t>("worker", "num-svc-processing-threads") == 5);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num-fs-processing-threads", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num-fs-processing-threads", 6));
    BOOST_CHECK(config->get<size_t>("worker", "num-fs-processing-threads") == 6);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "fs-buf-size-bytes", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "fs-buf-size-bytes", 1025));
    BOOST_CHECK(config->get<size_t>("worker", "fs-buf-size-bytes") == 1025);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num-loader-processing-threads", 0),
                      std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num-loader-processing-threads", 7));
    BOOST_CHECK(config->get<size_t>("worker", "num-loader-processing-threads") == 7);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num-exporter-processing-threads", 0),
                      std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num-exporter-processing-threads", 8));
    BOOST_CHECK(config->get<size_t>("worker", "num-exporter-processing-threads") == 8);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num-http-loader-processing-threads", 0),
                      std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num-http-loader-processing-threads", 9));
    BOOST_CHECK(config->get<size_t>("worker", "num-http-loader-processing-threads") == 9);

    BOOST_CHECK_THROW(config->set<size_t>("worker", "num-async-loader-processing-threads", 0),
                      std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<size_t>("worker", "num-async-loader-processing-threads", 10));
    BOOST_CHECK(config->get<size_t>("worker", "num-async-loader-processing-threads") == 10);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "async-loader-auto-resume", 1));
    BOOST_CHECK(config->get<unsigned int>("worker", "async-loader-auto-resume") != 0);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "async-loader-auto-resume", 0));
    BOOST_CHECK(config->get<unsigned int>("worker", "async-loader-auto-resume") == 0);

    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "async-loader-cleanup-on-resume", 1));
    BOOST_CHECK(config->get<unsigned int>("worker", "async-loader-cleanup-on-resume") != 0);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "async-loader-cleanup-on-resume", 0));
    BOOST_CHECK(config->get<unsigned int>("worker", "async-loader-cleanup-on-resume") == 0);

    BOOST_CHECK_THROW(config->set<unsigned int>("worker", "http-max-listen-conn", 0), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(config->set<unsigned int>("worker", "http-max-listen-conn", 2048));
    BOOST_CHECK(config->get<unsigned int>("worker", "http-max-listen-conn") == 2048);
}

BOOST_AUTO_TEST_CASE(ConfigurationTestWorkerOperators) {
    LOGS_INFO("Testing worker comparison operators");

    WorkerInfo w1;
    WorkerInfo w2;
    BOOST_CHECK(w1 == w2);
    BOOST_CHECK(!(w1 != w2));

    w1.name = "w1";
    w2.name = "w2";
    BOOST_CHECK(w1 != w2);
    BOOST_CHECK(!(w1 == w2));
}

BOOST_AUTO_TEST_CASE(ConfigurationTestWorkers) {
    LOGS_INFO("Testing worker services");

    // Default assumptions for optional parameters of the workers selector.
    vector<string> workers1;
    BOOST_REQUIRE_NO_THROW(workers1 = config->workers());
    BOOST_CHECK(workers1.size() == 1);
    BOOST_CHECK(workers1 == vector<string>({"worker-A"}));

    // Explicit values of the worker selectors.
    bool isEnabled = true;
    bool isReadOnly = false;
    vector<string> workers2;
    BOOST_REQUIRE_NO_THROW(workers2 = config->workers(isEnabled, isReadOnly));
    sort(workers2.begin(), workers2.end());
    BOOST_CHECK(workers2.size() == 1);
    BOOST_CHECK(workers2 == workers1);

    // Fetch names of all the read-only workers.
    isEnabled = true;
    isReadOnly = true;
    vector<string> workers3;
    BOOST_REQUIRE_NO_THROW(workers3 = config->workers(isEnabled, isReadOnly));
    sort(workers3.begin(), workers3.end());
    BOOST_CHECK(workers3.size() == 1);
    BOOST_CHECK(workers3 == vector<string>({"worker-B"}));

    // Fetch names of all the disabled workers.
    isEnabled = false;
    vector<string> workers4;
    BOOST_REQUIRE_NO_THROW(workers4 = config->workers(isEnabled));
    sort(workers4.begin(), workers4.end());
    BOOST_CHECK(workers4.size() == 1);
    BOOST_CHECK(workers4 == vector<string>({"worker-C"}));

    for (auto&& name : vector<string>({"worker-A", "worker-B", "worker-C"})) {
        BOOST_CHECK(config->isKnownWorker(name));
    }
}

BOOST_AUTO_TEST_CASE(ConfigurationTestWorkerParameters) {
    LOGS_INFO("Testing worker parameters");

    WorkerInfo workerA;
    BOOST_REQUIRE_NO_THROW(workerA = config->workerInfo("worker-A"));
    BOOST_CHECK(workerA.name == "worker-A");
    BOOST_CHECK(workerA.isEnabled);
    BOOST_CHECK(!workerA.isReadOnly);

    WorkerInfo workerB;
    BOOST_REQUIRE_NO_THROW(workerB = config->workerInfo("worker-B"));
    BOOST_CHECK(workerB.name == "worker-B");
    BOOST_CHECK(workerB.isEnabled);
    BOOST_CHECK(workerB.isReadOnly);

    WorkerInfo workerC;
    BOOST_REQUIRE_NO_THROW(workerC = config->workerInfo("worker-C"));
    BOOST_CHECK(workerC.name == "worker-C");
    BOOST_CHECK(!workerC.isEnabled);

    // Adding a new worker with well formed and unique parameters.
    WorkerInfo workerD;
    workerD.name = "worker-D";
    workerD.isEnabled = true;
    workerD.isReadOnly = true;

    BOOST_REQUIRE_NO_THROW(config->addWorker(workerD));
    BOOST_CHECK_THROW(config->addWorker(workerD), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(workerD = config->workerInfo("worker-D"));
    BOOST_CHECK(workerD.name == "worker-D");
    BOOST_CHECK(workerD.isEnabled);
    BOOST_CHECK(workerD.isReadOnly);

    // Adding a new worker with incomplete set of specs. The only required
    // attribute is the name of the worker.
    WorkerInfo workerE;
    workerE.name = "worker-E";
    WorkerInfo addedWorkerE;
    BOOST_REQUIRE_NO_THROW(addedWorkerE = config->addWorker(workerE));
    BOOST_CHECK(addedWorkerE.name == workerE.name);
    BOOST_CHECK(addedWorkerE.isEnabled == workerE.isEnabled);
    BOOST_CHECK(addedWorkerE.isReadOnly == workerE.isReadOnly);

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
    BOOST_REQUIRE_NO_THROW(updatedWorker = config->updateWorker(updatedWorker));
}

BOOST_AUTO_TEST_CASE(ConfigurationTestFamilies) {
    LOGS_INFO("Testing database families");

    // Selecting and probing database families.
    vector<string> families;
    BOOST_REQUIRE_NO_THROW(families = config->databaseFamilies());
    sort(families.begin(), families.end());
    BOOST_CHECK(families.size() == 2);
    BOOST_CHECK(families == vector<string>({"production", "test"}));
    for (auto&& name : families) {
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
    for (auto&& name : vector<string>({"db1", "db2", "db3", "db4", "db5", "db6"})) {
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
    BOOST_CHECK(db1info.createTime == 10);
    BOOST_CHECK(db1info.publishTime == 11);
    BOOST_CHECK(db1info.directorTable.count("Table11") != 0);
    BOOST_CHECK(db1info.directorTable.at("Table11").empty());
    BOOST_CHECK(db1info.directorTableKey.count("Table11") != 0);
    BOOST_CHECK(db1info.directorTableKey.at("Table11") == "id11");
    BOOST_CHECK(db1info.latitudeColName.count("Table11") != 0);
    BOOST_CHECK(db1info.latitudeColName.at("Table11") == "decl11");
    BOOST_CHECK(db1info.longitudeColName.count("Table11") != 0);
    BOOST_CHECK(db1info.longitudeColName.at("Table11") == "ra11");
    BOOST_CHECK(db1info.tableIsPublished.count("Table11") != 0);
    BOOST_CHECK(db1info.tableIsPublished.at("Table11") == true);
    BOOST_CHECK(db1info.tableCreateTime.count("Table11") != 0);
    BOOST_CHECK(db1info.tableCreateTime.at("Table11") == 110);
    BOOST_CHECK(db1info.tablePublishTime.count("Table11") != 0);
    BOOST_CHECK(db1info.tablePublishTime.at("Table11") == 111);
 
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
    BOOST_CHECK(db2info.createTime == 20);
    BOOST_CHECK(db2info.publishTime == 21);
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
    BOOST_CHECK(db3info.createTime == 30);
    BOOST_CHECK(db3info.publishTime == 31);
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
    BOOST_CHECK(db3info.longitudeColName.count("Table33") != 0);
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
    BOOST_CHECK(db4info.createTime == 40);
    BOOST_CHECK(db4info.publishTime == 41);
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
    BOOST_CHECK(db5info.createTime == 50);
    BOOST_CHECK(db5info.publishTime == 51);
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
    BOOST_CHECK(db6info.createTime == 60);
    BOOST_CHECK(db6info.publishTime == 0);
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
        BOOST_CHECK(info.createTime != 0);
        BOOST_CHECK(info.publishTime == 0);
        BOOST_CHECK(info.partitionedTables.empty());
        BOOST_CHECK(info.regularTables.empty());
        BOOST_CHECK(info.directorTable.empty());
        BOOST_CHECK(info.directorTableKey.empty());
        BOOST_CHECK(info.latitudeColName.empty());
        BOOST_CHECK(info.longitudeColName.empty());
        BOOST_CHECK(info.tableIsPublished.empty());
        BOOST_CHECK(info.tableCreateTime.empty());
        BOOST_CHECK(info.tablePublishTime.empty());
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
        BOOST_REQUIRE_NO_THROW(info = config->addTable("new", "T1", isPartitioned, coldefs, isDirectorTable,
                                                       directorTable, directorTableKey, latitudeColName,
                                                       longitudeColName););
        BOOST_CHECK(info.columns.count("T1") == 1);
        std::list<SqlColDef> columns;
        BOOST_REQUIRE_NO_THROW(columns = info.columns.at("T1"););
        BOOST_CHECK(columns.size() == 4);
        BOOST_CHECK(find_if(columns.cbegin(), columns.cend(), [&directorTableKey](SqlColDef const& coldef) {
                        return (coldef.name == directorTableKey) && (coldef.type == "INT UNSIGNED");
                    }) != columns.cend());
        BOOST_CHECK(find_if(columns.cbegin(), columns.cend(), [&latitudeColName](SqlColDef const& coldef) {
                        return (coldef.name == latitudeColName) && (coldef.type == "DOUBLE");
                    }) != columns.cend());
        BOOST_CHECK(find_if(columns.cbegin(), columns.cend(), [&longitudeColName](SqlColDef const& coldef) {
                        return (coldef.name == longitudeColName) && (coldef.type == "DOUBLE");
                    }) != columns.cend());
        BOOST_CHECK(find_if(columns.cbegin(), columns.cend(), [](SqlColDef const& coldef) {
                        return (coldef.name == lsst::qserv::SUB_CHUNK_COLUMN) && (coldef.type == "INT");
                    }) != columns.cend());
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
        BOOST_REQUIRE_NO_THROW(info = config->addTable("new", "T2", isPartitioned, coldefs, isDirectorTable,
                                                       directorTable, directorTableKey, latitudeColName,
                                                       longitudeColName););
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
