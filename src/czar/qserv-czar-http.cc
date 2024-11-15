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
    po::options_description desc("", 120);
    desc.add_options()("help,h", "Print this help message and exit.");
    desc.add_options()("verbose,v", "Produce verbose output.");
    desc.add_options()("czar-name", po::value<string>()->default_value("http"),
                       "The name of this Czar frontend. Assign a unique name to each Czar.");
    desc.add_options()("config", po::value<string>()->default_value("/config-etc/qserv-czar.cnf"),
                       "The configuration file.");
    desc.add_options()("port", po::value<uint16_t>()->default_value(4048),
                       "HTTP/HTTPS port of the REST API.");
    desc.add_options()("threads", po::value<unsigned int>()->default_value(thread::hardware_concurrency()),
                       "The number of the request processing threads in the REST service."
                       " The default value is the number of hardware threads. Zero value is not allowed.");
    desc.add_options()("worker-ingest-threads",
                       po::value<unsigned int>()->default_value(thread::hardware_concurrency()),
                       "A size of a thread pool for pushing table contributions to workers over"
                       " the synchronous HTTP/HTTPS protocol. The default value is the number of"
                       " hardware threads.  Zero value is not allowed.");
    desc.add_options()("ssl-cert-file", po::value<string>()->default_value("/config-etc/ssl/czar-cert.pem"),
                       "The SSL/TSL certificate file.");
    desc.add_options()("ssl-private-key-file",
                       po::value<string>()->default_value("/config-etc/ssl/czar-key.pem"),
                       "The SSL/TSL private key file.");
    desc.add_options()("tmp-dir", po::value<string>()->default_value("/tmp"),
                       "The temporary directory for the service.");
    desc.add_options()("conn-pool-size", po::value<unsigned int>()->default_value(0),
                       "A size of a connection pool for synchronous communications over the HTTP"
                       " protocol with the Qserv Worker Ingest servbers. The default value is 0,"
                       " which assumes that the pool size is determined by an implementation of"
                       " the underlying library 'libcurl'. The number of connectons in a production"
                       " Qserv deployment should be at least the number of workers in the deployment.");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, const_cast<char**>(argv), desc), vm);
    po::notify(vm);

    string const czarName = vm["czar-name"].as<string>();
    string const configFilePath = vm["config"].as<string>();
    uint16_t const port = vm["port"].as<uint16_t>();

    unsigned int const numThreads = vm["threads"].as<unsigned int>();
    if (numThreads == 0) {
        throw invalid_argument(
                "The number of threads in command line option '--threads'"
                " must be greater than zero");
    }

    unsigned int const numWorkerIngestThreads = vm["worker-ingest-threads"].as<unsigned int>();
    if (numWorkerIngestThreads == 0) {
        throw invalid_argument(
                "The number of threads in command line option '--worker-ingest-threads'"
                " must be greater than zero");
    }
    string const sslCertFile = vm["ssl-cert-file"].as<string>();
    string const sslPrivateKeyFile = vm["ssl-private-key-file"].as<string>();
    string const tmpDir = vm["tmp-dir"].as<string>();
    unsigned int connPoolSize = vm["conn-pool-size"].as<unsigned int>();

    if (vm.count("help")) {
        cout << argv[0] << " [options]\n\n" << ::help << "\n\n" << desc << endl;
        return 0;
    }
    bool const verbose = vm.count("verbose") > 0;
    if (verbose) {
        cout << ::context << " Czar name: " << czarName << "\n"
             << ::context << " Configuration file: " << configFilePath << "\n"
             << ::context << " Port: " << port << "\n"
             << ::context << " Number of threads: " << numThreads << "\n"
             << ::context << " Number of worker ingest threads: " << numWorkerIngestThreads << "\n"
             << ::context << " SSL certificate file: " << sslCertFile << "\n"
             << ::context << " SSL private key file: " << sslPrivateKeyFile << "\n"
             << ::context << " Temporary directory: " << tmpDir << "\n"
             << ::context << " Connection pool size: " << connPoolSize << endl;
    }
    try {
        auto const czar = czar::Czar::createCzar(configFilePath, czarName);
        auto const svc = czar::HttpCzarSvc::create(port, numThreads, numWorkerIngestThreads, sslCertFile,
                                                   sslPrivateKeyFile, tmpDir, connPoolSize);
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
