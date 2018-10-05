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

/// qserv-replica-database-test.cc is for testing the DatabaseServices API used by
/// the Replication system implementation.

// System headers
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseServices.h"
#include "replica/ReplicaInfo.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

// Command line parameters

std::string operation;
std::string configUrl = "file:replication.cfg";

bool         databaseAllowReconnect        = Configuration::databaseAllowReconnect();
unsigned int databaseConnectTimeoutSec     = Configuration::databaseConnectTimeoutSec();
unsigned int databaseMaxReconnects         = Configuration::databaseMaxReconnects();
unsigned int databaseTransactionTimeoutSec = Configuration::databaseTransactionTimeoutSec();

size_t maxReplicas = 1;

bool enabledWorkersOnly = false;

unsigned int chunk = 0;

std::string workerName;
std::string databaseName;
std::string databaseFamilyName;

std::string asString(std::time_t t) {
    std::ostringstream ss;
    ss << std::put_time(std::localtime(&t), "%F %T");
    return ss.str();
}

std::string asString(uint64_t ms) {

    // Milliseconds since Epoch
    std::chrono::time_point<std::chrono::system_clock> tp((std::chrono::milliseconds(ms)));

    // Convert to system time
    std::time_t t = std::chrono::system_clock::to_time_t(tp);

    return asString(t);
}

std::string asStringIf(uint64_t ms) { return 0 == ms ? "" : asString(ms); }

void dump(std::vector<ReplicaInfo> const& replicas) {
    for (auto&& r: replicas) {
        std::cout
            << "\n"
            << " ------------------ REPLICA ------------------\n"
            << "\n"
            << "             chunk: " << r.chunk()                              << "\n"
            << "          database: " << r.database()                           << "\n"
            << "            worker: " << r.worker()                             << "\n"
            << "            status: " << ReplicaInfo::status2string(r.status()) << "\n"
            << "        verifyTime: " << asStringIf(r.verifyTime())             << "\n"
            << " beginTransferTime: " << asStringIf(r.beginTransferTime())      << "\n"
            << "   endTransferTime: " << asStringIf(r.endTransferTime())        << "\n";
        for (auto&& f: r.fileInfo()) {
            std::cout
                << "\n"
                << "              name: " << f.name                          << "\n"
                << "              size: " << f.size                          << "\n"
                << "             mtime: " << asString(f.mtime)               << "\n"
                << "                cs: " << f.cs                            << "\n"
                << "            inSize: " << f.inSize                        << "\n"
                << " beginTransferTime: " << asStringIf(f.beginTransferTime) << "\n"
                << "   endTransferTime: " << asStringIf(f.endTransferTime)   << "\n";
        }
    }
    std::cout << std::endl;
}

void test() {

    try {

        // Change default parameters of the database connectors before
        // attemping any operations with the Replication Framework.

        Configuration::setDatabaseAllowReconnect(databaseAllowReconnect);
        Configuration::setDatabaseConnectTimeoutSec(databaseConnectTimeoutSec);
        Configuration::setDatabaseMaxReconnects(databaseMaxReconnects);
        Configuration::setDatabaseTransactionTimeoutSec(databaseTransactionTimeoutSec);

        auto provider = ServiceProvider::create(configUrl);

        if ("CONFIGURATION" == ::operation) {
            std::cout << provider->config()->asString() << std::endl;
        } else {
            std::vector<ReplicaInfo> replicas;
    
            if ("FIND_OLDEST_REPLICAS" == ::operation) {
                provider->databaseServices()->findOldestReplicas(
                    replicas,
                    maxReplicas,
                    enabledWorkersOnly
                );
            } else if ("FIND_REPLICAS" == ::operation) {
                provider->databaseServices()->findReplicas(
                    replicas,
                    chunk,
                    databaseName,
                    enabledWorkersOnly
                );
            } else if ("FIND_WORKER_REPLICAS_1" == ::operation) {
                provider->databaseServices()->findWorkerReplicas(
                    replicas,
                    workerName
                );
            } else if ("FIND_WORKER_REPLICAS_2" == ::operation) {
                provider->databaseServices()->findWorkerReplicas(
                    replicas,
                    workerName,
                    databaseName
                );
            } else if ("FIND_WORKER_REPLICAS_3" == ::operation) {
                provider->databaseServices()->findWorkerReplicas(
                    replicas,
                    chunk,
                    workerName
                );
            } else if ("FIND_WORKER_REPLICAS_4" == ::operation) {
                provider->databaseServices()->findWorkerReplicas(
                    replicas,
                    chunk,
                    workerName,
                    databaseFamilyName
                );
            }
            dump(replicas);
        }
    
    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
    }
}

} /// namespace

int main(int argc, const char* const argv[]) {

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
            argc,
            argv,
            "\n"
            "Usage:\n"
            "\n"
            "  <operation> [<parameters>] [<options>]\n"
            "\n"
            "              [--config=<url>]\n"
            "\n"
            "              [--db-allow-reconnect=<flag>]\n"
            "              [--db-reconnect-timeout=<sec>]\n"
            "              [--db-max-reconnects=<num>]\n"
            "              [--db-transaction-timeout=<sec>]\n"
            "\n"
            "Supported operations and mandatory parameters:\n"
            "\n"
            "    CONFIGURATION\n"
            "\n"
            "    FIND_OLDEST_REPLICAS   [--replicas=<num>] [--enabled-workers-only]\n"
            "\n"
            "    FIND_REPLICAS          <chunk> <database> [--enabled-workers-only]\n"
            "\n"
            "    FIND_WORKER_REPLICAS_1 <worker>\n"
            "    FIND_WORKER_REPLICAS_2 <worker> <database>\n"
            "\n"
            "    FIND_WORKER_REPLICAS_3 <chunk> <worker>\n"
            "    FIND_WORKER_REPLICAS_4 <chunk> <worker> <database-family>\n"
            "\n"
            "Parameters:\n"
            "\n"
            "    <database> \n"
            "\n"
            "      the name of a database\n"
            "\n"
            "    <database-family> \n"
            "\n"
            "      the name of a database family\n"
            "\n"
            "    <chunk> \n"
            "\n"
            "      the number of a chunk\n"
            "\n"
            "    <worker> \n"
            "\n"
            "      the name of a worker\n"
            "\n"
            "Flags and options:\n"
            "\n"
            "    --db-allow-reconnect \n"
            "\n"
            "      change the default database connecton handling node. Set 0 to disable automatic\n"
            "      reconnects. Any other number enables reconnect.\n"
            "      DEFAULT: " + std::string(::databaseAllowReconnect ? "1" : "0") + "\n"
            "\n"
            "    --db-reconnect-timeout \n"
            "\n"
            "      change the default value limiting a duration of time for making automatic\n"
            "      reconnects to a database server before failing and reporting error (if the server\n"
            "      is not up, or if it's not reachable for some reason)\n"
            "      DEFAULT: " + std::to_string(::databaseConnectTimeoutSec) + "\n"
            "\n"
            "    --db-max-reconnects\n"
            "\n"
            "      change the default value limiting a number of attempts to repeat a sequence\n"
            "      of queries due to connection losses and subsequent reconnects before to fail.\n"
            "      DEFAULT: " + std::to_string(::databaseMaxReconnects) + "\n"
            "\n"
            "    --db-transaction-timeout \n"
            "\n"
            "      change the default value limiting a duration of each attempt to execute\n"
            "      a database transaction before to fail.\n"
            "      DEFAULT: " + std::to_string(::databaseTransactionTimeoutSec) + "\n"
            "\n"
            "    --config \n"
            "\n"
            "      configuration URL (a configuration file or a set of the database\n"
            "      connection parameters"
            "      DEFAULT: '" + ::configUrl + "'\n"
            "\n"
            "    --replicas \n"
            "\n"
            "      maximum number of replicas to be returned"
            "      DEFAULT: " + std::to_string(::maxReplicas) + "\n"
            "\n"
            "    --enabled-workers-only \n"
            "\n"
            "      limit a scope of an operation to workers which are presently enabled in\n"
            "      the Replication system.\n"
            );

        ::operation = parser.parameterRestrictedBy(
            1, {"CONFIGURATION",
                "FIND_OLDEST_REPLICAS",
                "FIND_REPLICAS",
                "FIND_WORKER_REPLICAS_1",
                "FIND_WORKER_REPLICAS_2",
                "FIND_WORKER_REPLICAS_3",
                "FIND_WORKER_REPLICAS_4"}
        );
        if ("FIND_REPLICAS" == ::operation) {

            ::chunk              = parser.parameter<unsigned int>(2);
            ::databaseName       = parser.parameter<std::string>(3);

        } else if ("FIND_WORKER_REPLICAS_1" == ::operation) {

            ::workerName         = parser.parameter<std::string>(2);

        } else if ("FIND_WORKER_REPLICAS_2" == ::operation) {

            ::workerName         = parser.parameter<std::string>(2);
            ::databaseName       = parser.parameter<std::string>(3);

        } else if ("FIND_WORKER_REPLICAS_3" == ::operation) {

            ::chunk              = parser.parameter<unsigned int>(2);
            ::workerName         = parser.parameter<std::string>(3);

        } else if ("FIND_WORKER_REPLICAS_4" == ::operation) {

            ::chunk              = parser.parameter<unsigned int>(2);
            ::workerName         = parser.parameter<std::string>(3);
            ::databaseFamilyName = parser.parameter<std::string>(4);
        }

        ::configUrl = parser.option<std::string>("config", ::configUrl);

        ::databaseAllowReconnect        = parser.option<unsigned int>("db-allow-reconnect",     ::databaseAllowReconnect);
        ::databaseConnectTimeoutSec     = parser.option<unsigned int>("db-reconnect-timeout",   ::databaseConnectTimeoutSec);
        ::databaseMaxReconnects         = parser.option<unsigned int>("db-max-reconnects",      ::databaseMaxReconnects);
        ::databaseTransactionTimeoutSec = parser.option<unsigned int>("db-transaction-timeout", ::databaseTransactionTimeoutSec);

        ::maxReplicas = parser.option<unsigned int>("replicas", ::maxReplicas);

        ::enabledWorkersOnly = parser.flag("enabled-workers-only");

    } catch (std::exception const& ex) {
        return 1;
    }
    ::test();
    return 0;
}
