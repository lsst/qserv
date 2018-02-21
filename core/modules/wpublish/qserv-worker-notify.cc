// System header
#include <iostream>
#include <iomanip>
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
#include "wpublish/ChunkGroupQservRequest.h"
#include "wpublish/ChunkListQservRequest.h"
#include "wpublish/GetChunkListQservRequest.h"
#include "wpublish/TestEchoQservRequest.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

namespace global   = lsst::qserv;
namespace util     = lsst::qserv::util;
namespace wpublish = lsst::qserv::wpublish;

namespace {

// Command line parameters

std::string operation;
std::string worker;
unsigned int chunk;
std::vector<std::string> dbs;
std::string value;
std::string serviceProviderLocation;
bool inUseOnly;
bool reload;
bool force;
bool printReport;


int test () {

    // Instantiate a request object

    std::atomic<bool> finished(false);

    XrdSsiRequest* request = nullptr;

    if ("GET_CHUNK_LIST" == operation) {
        request = new wpublish::GetChunkListQservRequest (
            inUseOnly,
            [&finished] (wpublish::GetChunkListQservRequest::Status status,
                         std::string const& error,
                         wpublish::GetChunkListQservRequest::ChunkCollection const& chunks) {

                if (status != wpublish::GetChunkListQservRequest::Status::SUCCESS) {
                    std::cout << "status: " << wpublish::GetChunkListQservRequest::status2str(status) << "\n"
                              << "error:  " << error << std::endl;
                } else {
                    std::cout << "# total chunks: " << chunks.size() << "\n"
                              << std::endl;
                    if (chunks.size()) {
                        std::cout << "      chunk |                         database | in use \n"
                                  << "------------+----------------------------------+--------\n";
                        for (auto const& entry: chunks) {
                            std::cout << " " << std::setw(10) << entry.chunk << " |"
                                      << " " << std::setw(32) << entry.database << " |"
                                      << " " << std::setw (6) << entry.use_count << " \n";
                        }
                        std::cout << std::endl;
                    }
                }
                finished = true;
            });

    } else if ("REBUILD_CHUNK_LIST" == operation) {
        request = new wpublish::RebuildChunkListQservRequest (
            reload,
            [&finished] (wpublish::ChunkListQservRequest::Status status,
                         std::string const& error,
                         wpublish::ChunkListQservRequest::ChunkCollection const& added,
                         wpublish::ChunkListQservRequest::ChunkCollection const& removed) {

                if (status != wpublish::ChunkListQservRequest::Status::SUCCESS) {
                    std::cout << "status: " << wpublish::ChunkListQservRequest::status2str(status) << "\n"
                              << "error:  " << error << std::endl;
                } else {
                    std::cout << "# chunks added:   " << added.size()   << "\n"
                              << "# chuks  removed: " << removed.size() << std::endl;
                }
                finished = true;
            });

    } else if ("RELOAD_CHUNK_LIST" == operation) {
        request = new wpublish::ReloadChunkListQservRequest (
            [&finished] (wpublish::ChunkListQservRequest::Status status,
                         std::string const& error,
                         wpublish::ChunkListQservRequest::ChunkCollection const& added,
                         wpublish::ChunkListQservRequest::ChunkCollection const& removed) {

                if (status != wpublish::ChunkListQservRequest::Status::SUCCESS) {
                    std::cout << "status: " << wpublish::ChunkListQservRequest::status2str(status) << "\n"
                              << "error:  " << error << std::endl;
                } else {
                    std::cout << "# chunks added:   " << added.size()   << "\n"
                              << "# chuks  removed: " << removed.size() << std::endl;
                }
                finished = true;
            });

    } else if ("ADD_CHUNK_GROUP" == operation) {
        request = new wpublish::AddChunkGroupQservRequest (
            chunk,
            dbs,
            [&finished] (wpublish::ChunkGroupQservRequest::Status status,
                         std::string const& error) {

                if (status != wpublish::ChunkGroupQservRequest::Status::SUCCESS) {
                    std::cout << "status: " << wpublish::ChunkGroupQservRequest::status2str(status) << "\n"
                              << "error:  " << error << std::endl;
                }
                finished = true;
            });

    } else if ("REMOVE_CHUNK_GROUP" == operation) {
        request = new wpublish::RemoveChunkGroupQservRequest (
            chunk,
            dbs,
            force,
            [&finished] (wpublish::ChunkGroupQservRequest::Status status,
                         std::string const& error) {

                if (status != wpublish::ChunkGroupQservRequest::Status::SUCCESS) {
                    std::cout << "status: " << wpublish::ChunkGroupQservRequest::status2str(status) << "\n"
                              << "error:  " << error << std::endl;
                }
                finished = true;
            });

    } else if ("TEST_ECHO" == operation) {
        request = new wpublish::TestEchoQservRequest (
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
                finished = true;
            });

    } else {
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

    // Submit the request
    XrdSsiResource resource (global::ResourceUnit::makeWorkerPath(worker));
    serviceProvider->ProcessRequest (*request, resource);

    // Block while the request is in progress 
    util::BlockPost blockPost (1000, 2000);
    while (not finished) blockPost.wait(200);

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
            "              [--in-use-only]\n"
            "              [--reload]\n"
            "              [--force>]\n"
            "              [--print-report]\n"
            "\n"
            "Supported operations and mandatory parameters:\n"
            "    GET_CHUNK_LIST     <worker>\n"
            "    REBUILD_CHUNK_LIST <worker>\n"
            "    RELOAD_CHUNK_LIST  <worker>\n"
            "    ADD_CHUNK_GROUP    <worker> <chunk> <db> [<db> [<db> ... ]]\n"
            "    REMOVE_CHUNK_GROUP <worker> <chunk> <db> [<db> [<db> ... ]]\n"
            "    TEST_ECHO          <worker> <value>\n"
            "\n"
            "Flags an options:\n"
            "  --service=<provider>  - location of a service provider (default: 'localhost:1094')\n"
            "  --in-use-only         - used with GET_CHUNK_LIST to only report chunks which are in use.\n"
            "                          Otherwise all chunks will be reported\n"
            "  --reload              - used with REBUILD_CHUNK_LIST to also reload the list into a worker\n"
            "  --force               - force operation in REMOVE_CHUNK_GROUP even for chunks in use\n"
            "  --print-report        - print \n"
            "\n"
            "Parameters:\n"
            "  <worker> - unique identifier of a worker (example: 'worker-1')\n"
            "  <chunk>  - chunk number\n"
            "  <db>     - database name\n"
            "  <value>  - arbitrary string\n");

        ::operation = parser.parameterRestrictedBy (1, {
            "GET_CHUNK_LIST",
            "REBUILD_CHUNK_LIST",
            "RELOAD_CHUNK_LIST",
            "ADD_CHUNK_GROUP",
            "REMOVE_CHUNK_GROUP",
            "TEST_ECHO"});

        ::worker = parser.parameter<std::string>(2);

        if (parser.found_in(::operation, {
            "ADD_CHUNK_GROUP",
            "REMOVE_CHUNK_GROUP"})) {
            ::chunk = parser.parameter <unsigned int>(3);
            ::dbs   = parser.parameters<std::string> (4);

        } else if (parser.found_in(::operation, {
            "TEST_ECHO"})) {
            ::value     = parser.parameter <std::string>(3);
        }
        ::serviceProviderLocation = parser.option<std::string> ("service", "localhost:1094");
        ::inUseOnly               = parser.flag                ("in-use-only");
        ::reload                  = parser.flag                ("reload");
        ::force                   = parser.flag                ("force");
        ::printReport             = parser.flag                ("print-report");

    } catch (std::exception const& ex) {
        return 1;
    } 
    return ::test ();
}
