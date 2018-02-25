// System header
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/worker.pb.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"
#include "wpublish/TestEchoQservRequest.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

namespace global   = lsst::qserv;
namespace util     = lsst::qserv::util;
namespace wpublish = lsst::qserv::wpublish;

namespace {

// Command line parameters

std::string  workersFileName;
unsigned int numRequests;
std::string  value;
std::string  serviceProviderLocation;
unsigned int numWorkers;
bool workerFirst;


std::vector<std::string> workers;

bool readWorkersFile() {
    std::ifstream file(workersFileName);
    if (not file.good()) {
        std::cerr << "error: failed to open a file with worker identifiers: "
                  << workersFileName << std::endl;
        return false;
    }
    std::string worker;
    while (file >> worker)
        workers.push_back(worker);

    if (not workers.size()) {
        std::cerr << "error: no workers found in file with worker identifiers: "
                  << workersFileName << std::endl;
        return false;
    }
    return true;
}


int test() {

    if (not readWorkersFile()) return 1;
    if (not numWorkers or (workers.size() < numWorkers)) {
        std::cerr << "error: specified number of workers not in the valid range: 1.."
                  << numWorkers << std::endl;
        return 1;
    }

    // Connect to a service provider
    XrdSsiErrInfo errInfo;
    auto serviceProvider =
        XrdSsiProviderClient->GetService(errInfo,
                                         serviceProviderLocation);
    if (!serviceProvider) {
        std::cerr
            << "failed to contact service provider at: " << serviceProviderLocation
            << ", error: " << errInfo.Get() << std::endl;
        return 1;
    }
    std::cout << "connected to service provider at: " << serviceProviderLocation << std::endl;

    // Instantiate a request object

    std::atomic<unsigned int> finished(0);


    if (workerFirst) {

        for (unsigned int j = 0; j < numWorkers; ++j) {
            std::string const& worker = workers[j];
    
            for (unsigned int i = 0; i < numRequests; ++i) {
    
                XrdSsiRequest* request = new wpublish::TestEchoQservRequest(
                    value,
                    [&finished] (wpublish::TestEchoQservRequest::Status status,
                                 std::string const& error,
                                 std::string const& sent,
                                 std::string const& received) {
        
                        if (status != wpublish::TestEchoQservRequest::Status::SUCCESS) {
                            std::cout << "status: " << wpublish::TestEchoQservRequest::status2str(status) << "\n"
                                      << "error:  " << error << std::endl;
                        } else {
                            std::cout << "value sent:     " << sent     << "\n"
                                      << "value received: " << received << std::endl;
                        }
                        finished--;
                    });
        
                // Submit the request
                finished++;
                XrdSsiResource resource(global::ResourceUnit::makeWorkerPath(worker));
                serviceProvider->ProcessRequest(*request, resource);
            }
        }

    } else {

        for (unsigned int i = 0; i < numRequests; ++i) {

            for (unsigned int j = 0; j < numWorkers; ++j) {
                std::string const& worker = workers[j];
    
    
                XrdSsiRequest* request = new wpublish::TestEchoQservRequest(
                    value,
                    [&finished] (wpublish::TestEchoQservRequest::Status status,
                                 std::string const& error,
                                 std::string const& sent,
                                 std::string const& received) {
        
                        if (status != wpublish::TestEchoQservRequest::Status::SUCCESS) {
                            std::cout << "status: " << wpublish::TestEchoQservRequest::status2str(status) << "\n"
                                      << "error:  " << error << std::endl;
                        } else {
                            std::cout << "value sent:     " << sent     << "\n"
                                      << "value received: " << received << std::endl;
                        }
                        finished--;
                    });
        
                // Submit the request
                finished++;
                XrdSsiResource resource(global::ResourceUnit::makeWorkerPath(worker));
                serviceProvider->ProcessRequest(*request, resource);
            }
        }
    }

    // Block while at least one request is in progress 
    util::BlockPost blockPost(1000, 2000);
    while (finished) blockPost.wait(200);

    return 0;
}
} // namespace

int main(int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <workers-file-name> <num-requests> <value>\n"
            "  [--service=<provider>]\n"
            "  [--num-workers=<value>]\n"
            "  [--worker-first]\b"
            "\n"
            "Flags an options:\n"
            "  --service=<provider>  - location of a service provider (default: 'localhost:1094')\n"
            "  --num-workers=<value> - the number of workers (default: 1, range: 1..10)\n"
            "  --worker-first        - iterate over workers, then over requests\n"
            "\n"
            "Parameters:\n"
            "  <workers-file-name>  - a file with worker identifiers (one worker per line)\n"
            "  <num-requests>       - chunk number\n"
            "  <value>              - arbitrary string\n");

        ::workersFileName = parser.parameter<std::string>(1);
        ::numRequests     = parser.parameter<unsigned int>(2);
        ::value           = parser.parameter<std::string>(3);

        ::serviceProviderLocation = parser.option<std::string>("service", "localhost:1094");
        ::numWorkers              = parser.option<unsigned int>("num-workers", 1);
        ::workerFirst             = parser.flag("worker-first");

    } catch (std::exception const& ex) {
        return 1;
    } 
    return ::test();
}
