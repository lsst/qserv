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
 * The CPP-HTTPLIB-based frontend for Czar.
 */

// System headers
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>

// Third party headers
#include "boost/program_options.hpp"

// Qserv headers
#include "czar/Czar.h"
#include "czar/HttpCzarSvc.h"

using namespace std;
namespace po = boost::program_options;
namespace czar = lsst::qserv::czar;
namespace qserv = lsst::qserv;

namespace {
char const* const help = "The HTTP-based Czar frontend.";
char const* const context = "[CZAR-HTTP-FRONTEND]";
}  // namespace

int main(int argc, char* argv[]) {
    czar::HttpCzarConfig httpCzarConfig;

    po::options_description desc("", 120);
    desc.add_options()("help,h", "Print this help message and exit.");
    desc.add_options()("verbose,v", "Produce verbose output.");
    desc.add_options()("czar-name", po::value<string>()->default_value("http"),
                       "The name of this Czar frontend. Assign a unique name to each Czar.");
    desc.add_options()("config", po::value<string>()->default_value("/config-etc/qserv-czar.cnf"),
                       "The configuration file.");
    desc.add_options()("port", po::value<uint16_t>()->default_value(httpCzarConfig.port),
                       "HTTP/HTTPS port of the REST API. Assigning 0 would result in allocating"
                       " the first available port.");
    desc.add_options()("threads", po::value<size_t>()->default_value(httpCzarConfig.numThreads),
                       "The number of the request processing threads in the REST service."
                       " A value of 0 implies the number of hardware threads.");
    desc.add_options()("worker-ingest-threads",
                       po::value<size_t>()->default_value(httpCzarConfig.numWorkerIngestThreads),
                       "A size of a thread pool for pushing table contributions to workers over"
                       " the synchronous HTTP/HTTPS protocol. A value of 0 implies the number"
                       " of hardware threads");
    desc.add_options()("ssl-cert-file", po::value<string>()->default_value(httpCzarConfig.sslCertFile),
                       "The SSL/TSL certificate file.");
    desc.add_options()("ssl-private-key-file",
                       po::value<string>()->default_value(httpCzarConfig.sslPrivateKeyFile),
                       "The SSL/TSL private key file.");
    desc.add_options()("tmp-dir", po::value<string>()->default_value(httpCzarConfig.tmpDir),
                       "The temporary directory for the service.");
    desc.add_options()("max-queued-requests",
                       po::value<size_t>()->default_value(httpCzarConfig.maxQueuedRequests),
                       "The limit for the maximum number of pending requests, i.e. requests accept()ed"
                       " by the listener but still waiting to be serviced by worker threads."
                       " A value of 0 implies that there are no limit.");
    desc.add_options()("conn-pool-size",
                       po::value<size_t>()->default_value(httpCzarConfig.clientConnPoolSize),
                       "A size of a connection pool for synchronous communications over the HTTP"
                       " protocol with the Qserv Worker Ingest servers. A value of 0 implies"
                       " that the pool size is determined by an implementation of"
                       " the underlying library 'libcurl'. The number of connectons in a production"
                       " Qserv deployment should be at least the number of workers in the deployment."
                       " Ideally the number should be equal to the number of workers multiplied by"
                       " the number of threads in the worker's thread pool.");
    desc.add_options()("boost-asio-threads",
                       po::value<size_t>()->default_value(httpCzarConfig.numBoostAsioThreads),
                       "The number of the BOOST ASIO threads for ASYNC communicatons with "
                       " the Replication Controller and workers. A value of 0 implies the number"
                       " of hardware threads.");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, const_cast<char**>(argv), desc), vm);
    po::notify(vm);

    string const czarName = vm["czar-name"].as<string>();
    string const configFilePath = vm["config"].as<string>();

    httpCzarConfig.port = vm["port"].as<uint16_t>();
    httpCzarConfig.numThreads = vm["threads"].as<size_t>();
    httpCzarConfig.numWorkerIngestThreads = vm["worker-ingest-threads"].as<size_t>();
    httpCzarConfig.sslCertFile = vm["ssl-cert-file"].as<string>();
    httpCzarConfig.sslPrivateKeyFile = vm["ssl-private-key-file"].as<string>();
    httpCzarConfig.tmpDir = vm["tmp-dir"].as<string>();
    httpCzarConfig.maxQueuedRequests = vm["max-queued-requests"].as<size_t>();
    httpCzarConfig.clientConnPoolSize = vm["conn-pool-size"].as<size_t>();
    httpCzarConfig.numBoostAsioThreads = vm["boost-asio-threads"].as<size_t>();

    if (vm.count("help")) {
        cout << argv[0] << " [options]\n\n" << ::help << "\n\n" << desc << endl;
        return 0;
    }
    bool const verbose = vm.count("verbose") > 0;
    if (verbose) {
        cout << ::context << " Czar name: " << czarName << "\n"
             << ::context << " Configuration file: " << configFilePath << "\n"
             << ::context << " Port: " << httpCzarConfig.port << "\n"
             << ::context << " Number of threads: " << httpCzarConfig.numThreads << "\n"
             << ::context << " Number of worker ingest threads: " << httpCzarConfig.numWorkerIngestThreads
             << "\n"
             << ::context << " SSL certificate file: " << httpCzarConfig.sslCertFile << "\n"
             << ::context << " SSL private key file: " << httpCzarConfig.sslPrivateKeyFile << "\n"
             << ::context << " Temporary directory: " << httpCzarConfig.tmpDir << "\n"
             << ::context << " Max.number of queued requests: " << httpCzarConfig.maxQueuedRequests << "\n"
             << ::context << " Connection pool size (libcurl): " << httpCzarConfig.clientConnPoolSize << "\n"
             << ::context << " Number of BOOST ASIO threads: " << httpCzarConfig.numBoostAsioThreads << endl;
    }
    try {
        auto const czar = czar::Czar::createCzar(configFilePath, czarName);
        auto const svc = czar::HttpCzarSvc::create(httpCzarConfig);
        if (verbose) {
            cout << ::context << " The query processing service of Czar bound to port: " << svc->port()
                 << endl;
        }
        svc->startAndWait();
    } catch (exception const& ex) {
        cerr << ::context << " The application failed, exception: " << ex.what() << endl;
        return 1;
    }
    return 0;
}
