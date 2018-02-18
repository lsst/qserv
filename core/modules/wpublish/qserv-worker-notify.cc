// System header
#include <iostream>
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
#include "util/CmdParser.h"
#include "wpublish/ReloadChunkListQservRequest.h"
#include "wpublish/TestEchoQservRequest.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

namespace global   = lsst::qserv;
namespace util     = lsst::qserv::util;
namespace wpublish = lsst::qserv::wpublish;

namespace {

// Command line parameters

std::string              operation;
std::string              worker;
unsigned int             chunk;
std::vector<std::string> databases;
std::string              value;
std::string              serviceProviderLocation;
bool                     printReport = false;


int test () {

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

    XrdSsiResource resource (global::ResourceUnit::makeWorkerPath(worker));

    util::BlockPost blockPost (1000, 2000);
    while (true) {

        XrdSsiRequest* request = nullptr;

        if ("RELOAD_CHUNK_LIST"  == operation) {
            request = new wpublish::ReloadChunkListQservRequest (
                [] (bool success,
                    wpublish::ReloadChunkListQservRequest::ChunkCollection const& added,
                    wpublish::ReloadChunkListQservRequest::ChunkCollection const& removed) {
                    std::cout << "# chunks added:   " << added.size()   << std::endl;
                    std::cout << "# chuks  removed: " << removed.size() << std::endl;
                });

        } else if ("ADD_CHUNK_GROUP"    == operation) {
            ;
        } else if ("REMOVE_CHUNK_GROUP" == operation) {
            ;
        } else if ("TEST_ECHO"          == operation) {
            request = new wpublish::TestEchoQservRequest (
                value,
                [] (bool success,
                    std::string const& sent,
                    std::string const& received) {
                    std::cout << "value sent:     " << sent     << std::endl;
                    std::cout << "value received: " << received << std::endl;
                });
        }
        if (not request) return 1;

        serviceProvider->ProcessRequest(*request, resource);

        blockPost.wait(1000);

        break;
    }

    return 0;
}
} // namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <operation> [<parameter> [<parameter> [...]]]\n"
            "              [--service=<provider>]\n"
            "              [--print-report]\n"
            "\n"
            "Supported operations and mandatory parameters:\n"
            "    RELOAD_CHUNK_LIST  <worker>\n"
            "    ADD_CHUNK_GROUP    <worker> <chunk> <db> [<db> [<db> ... ]]\n"
            "    REMOVE_CHUNK_GROUP <worker> <chunk> <db> [<db> [<db> ... ]]\n"
            "    TEST_ECHO          <worker> <value>\n"
            "\n"
            "Flags an options:\n"
            "  --service=<provider>  - location of a service provider (default: 'localhost:1094')\n"
            "  --print-report        - print \n"
            "\n"
            "Parameters:\n"
            "  <worker> - unique identifier of a worker (example: 'worker-1')\n"
            "  <chunk>  - chunk number\n"
            "  <db>     - database name\n"
            "  <value>  - arbitrary string\n");

        ::operation = parser.parameterRestrictedBy (1, {
            "RELOAD_CHUNK_LIST",
            "ADD_CHUNK_GROUP",
            "REMOVE_CHUNK_GROUP",
            "TEST_ECHO"});

        ::worker = parser.parameter<std::string>(2);

        if (parser.found_in(::operation, {
            "ADD_CHUNK_GROUP",
            "REMOVE_CHUNK_GROUP"})) {
            ::chunk     = parser.parameter <unsigned int>(3);
            ::databases = parser.parameters<std::string> (4);

        } else if (parser.found_in(::operation, {
            "TEST_ECHO"})) {
            ::value     = parser.parameter <std::string>(3);
        }
        ::serviceProviderLocation = parser.option<std::string> ("service", "localhost:1094");
        ::printReport             = parser.flag                ("print-report");

    } catch (std::exception const& ex) {
        return 1;
    } 
    return ::test ();
}
