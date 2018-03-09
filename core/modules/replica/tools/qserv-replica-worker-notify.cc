// System headers
#include <atomic>
#include <iostream>
#include <stdexcept>

#include "proto/replication.pb.h"
#include "replica/AddReplicaQservMgtRequest.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

namespace {

// Command line parameters

std::string  operation;
std::string  worker;
std::string  databaseFamily;
unsigned int chunk;
std::string  configUrl;

/// Run the test
bool test() {

    try {

        // Initialize the context

        replica::ServiceProvider::pointer const provider = replica::ServiceProvider::create(configUrl);

        // Launch the requst and wait for its completion
        //
        // Note that omFinish callbacks which are activated upon a completion
        // of the requsts will be run in a different thread.

        std::atomic<bool> finished(false);

        auto const& request = provider->qservMgtServices()->addRreplica(
            chunk,
            databaseFamily,
            worker,
            [&finished] (replica::AddReplicaQservMgtRequest::pointer const& request) {
                std::cout
                    << "state:         " << request->state2string(request->state()) << "\n"
                    << "extendedState: " << request->state2string(request->extendedState()) << "\n"
                    << "serverError:   " << request->serverError() << std::endl;
                finished = true;
            }
        );

        // Block while the request is in progress

        util::BlockPost blockPost(1000, 2000);
        while (not finished) {
            blockPost.wait(200);
        } 
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
    }
    return true;
}
} /// namespace

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
            "  <operation> <parameters> [--config=<url>]\n"
            "\n"
            "Supported operations & their parameters:\n"
            "  ADD_REPLICA <worker> <database-family> <chunk>\n"
            "\n"
            "Parameters:\n"
            "  <worker>          - the worker name (identifier)\n"
            "  <database-family> - the name of a database family\n"
            "  <chunk>           - the chunk number\n"
            "\n"
            "Flags and options:\n"
            "  --config          - a configuration URL (a configuration file or a set of the database\n"
            "                      connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::operation      = parser.parameterRestrictedBy(1, {"ADD_REPLICA"});
        ::worker         = parser.parameter<std::string>(2);
        ::databaseFamily = parser.parameter<std::string>(3);
        ::chunk          = parser.parameter<unsigned int>(4);

        ::configUrl = parser.option<std::string>("config", "file:replication.cfg");

    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    } 
    ::test();
    return 0;
}