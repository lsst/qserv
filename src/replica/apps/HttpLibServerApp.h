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
#ifndef LSST_QSERV_REPLICA_HTTPLIBSERVERAPP_H
#define LSST_QSERV_REPLICA_HTTPLIBSERVERAPP_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/apps/Application.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpLibServerApp is performance and scalability test for the embedded
 * HTTP server based on "cpp-httplib".
 */
class HttpLibServerApp : public Application {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<HttpLibServerApp> Ptr;

    /**
     * The factory method is the only way of creating objects of this class
     * because of the very base class's inheritance from 'enable_shared_from_this'.
     *
     * @param argc The number of command-line arguments.
     * @param argv The vector of command-line arguments.
     */
    static Ptr create(int argc, char* argv[]);

    HttpLibServerApp() = delete;
    HttpLibServerApp(HttpLibServerApp const&) = delete;
    HttpLibServerApp& operator=(HttpLibServerApp const&) = delete;

    virtual ~HttpLibServerApp() final = default;

protected:
    /// @see Application::runImpl()
    virtual int runImpl() final;

private:
    /// @see HttpLibServerApp::create()
    HttpLibServerApp(int argc, char* argv[]);

    /// The port number for listening for incoming connections. Specifying the number
    /// of 0 will result in allocating the next available port.
    int _port = 0;

    /// An address to bind the server to
    std::string _bindAddr = "0.0.0.0";

    /// The number of the threads to run the server. The number of 0 means the number
    /// of the threads will be equal to the number of the CPU cores.
    size_t _numThreads = 0;

    /// The parameter limiting the maximum number of pending requests, i.e. requests
    /// accept()ed by the listener but still waiting to be serviced by worker threads.
    /// Default limit is 0 (unlimited). Once the limit is reached, the listener will
    /// shutdown the client connection.
    size_t _maxQueuedRequests = 0;

    /// A location of the data directory where the test files parsed in bodies of the multpart
    /// requess will be saved.
    std::string _dataDir;

    /// The size of the message to be sent in the response body.
    size_t _messageSizeBytes = 1;

    /// An interval (milliseconds) for reporting the progress counters
    int _reportIntervalMs = 1000;

    /// The flag which would turn on periodic progress report on the incoming requests
    bool _progress = false;

    // Flags which would control reports on the incoming requests
    bool _verbose = false;
    bool _verboseDumpRequestBody = false;
    bool _verboseDumpResponseBody = false;
};

}  // namespace lsst::qserv::replica

#endif /* LSST_QSERV_REPLICA_HTTPLIBSERVERAPP_H */
