// System headers
#include <algorithm>
#include <atomic>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <vector>

#include "proto/replication.pb.h"
#include "replica/AddReplicaQservMgtRequest.h"
#include "replica/GetReplicasQservMgtRequest.h"
#include "replica/QservMgtServices.h"
#include "replica/SetReplicasQservMgtRequest.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

#define OUT std::cout

namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

namespace {

// Command line parameters

std::string  operation;
std::string  worker;
std::string  database;
std::string  databaseFamily;
std::string  inFileName;
unsigned int chunk;
bool         force;
bool         inUseOnly;
std::string  configUrl;

/**
 * Read and parse a space/newline separated stream of pairs from the input
 * file and fill replica entries into the collection. Each pair has
 * the following format:
 *
 *   <database>:<chunk>
 *
 * For example:
 *
 *   LSST:123 LSST:124 LSST:23456
 *   LSST:0
 *
 * @param replicas - collection to be initialized
 */
void readInFile(replica::QservReplicaCollection& replicas) {

    replicas.clear();

    std::ifstream infile(inFileName);
    if (not infile.good()) {
        std::cerr << "failed to open file: " << inFileName << std::endl;
        throw std::runtime_error("failed to open file: " + inFileName);
    }

    std::string databaseAndChunk;
    while (infile >> databaseAndChunk) {

        OUT << "'" << databaseAndChunk << "'" << std::endl;

        if (databaseAndChunk.empty()) { continue; }

        std::string::size_type const pos = databaseAndChunk.rfind(':');
        if ((pos == std::string::npos) or
            (pos == 0) or (pos == databaseAndChunk.size() - 1)) {
            throw std::runtime_error(
                "failed to parse file: " + inFileName + ", illegal <database>::<chunk> pair: '" +
                databaseAndChunk + "'");
        }
        unsigned int const chunk    = (unsigned int)(std::stoul(databaseAndChunk.substr(pos + 1)));
        std::string  const database = databaseAndChunk.substr(0, pos);

        OUT << "chunk: " << chunk << " database: '" << database << "'" << std::endl;

        replicas.emplace_back(
            replica::QservReplica{
                chunk,
                database,
                0   /* useCount (UNUSED) */
            }
        );
    }
}

/**
  * Print a colletion of replicas
  * @param collection
  */
void dump(replica::QservReplicaCollection const& collection) {
    size_t databaseWidth = std::string("database").size();
    for (auto const& replica: collection) {
        databaseWidth = std::max(databaseWidth,replica.database.size());
    }
    size_t const chunkWidth = 12;
    size_t const useCountWidth = std::string("use count").size();
    OUT << "\n"
        << " -" << std::string(databaseWidth, '-')        << "-+-" << std::string(chunkWidth, '-')     << "-+-" << std::string(useCountWidth, '-')         << "-\n"
        << "  " << std::setw(databaseWidth) << "database" << " | " << std::setw(chunkWidth) << "chunk" << " | " << std::setw(useCountWidth) << "use count" << " \n"
        << " -" << std::string(databaseWidth, '-')        << "-+-" << std::string(chunkWidth, '-')     << "-+-" << std::string(useCountWidth, '-')         << "-\n";
    for (auto const& replica: collection) {
        OUT << "  "   << std::setw(databaseWidth) << replica.database
            << " | " << std::setw(chunkWidth)    << replica.chunk
            << " | " << std::setw(useCountWidth) << replica.useCount
            << "\n";
    }
}

/// Run the test
void test() {

    try {

        // Initialize the context

        replica::ServiceProvider::Ptr const provider = replica::ServiceProvider::create(configUrl);

        // Launch the requst and wait for its completion
        //
        // Note that omFinish callbacks which are activated upon a completion
        // of the requsts will be run in a different thread.

        std::atomic<bool> finished(false);

        if (operation == "GET_REPLICAS") {
            provider->qservMgtServices()->getReplicas(
                databaseFamily,
                worker,
                inUseOnly,
                std::string(),
                [&finished] (replica::GetReplicasQservMgtRequest::Ptr const& request) {
                    std::cout
                        << "state:         " << request->state2string(request->state()) << "\n"
                        << "extendedState: " << request->state2string(request->extendedState()) << "\n"
                        << "serverError:   " << request->serverError() << std::endl;
                    if (request->extendedState() == replica::QservMgtRequest::SUCCESS) {
                        dump(request->replicas());
                    }
                    finished = true;
                }
            );

        } else if (operation == "SET_REPLICAS") {
            replica::QservReplicaCollection replicas;
            readInFile(replicas);
            OUT << "replicas read: " << replicas.size() << std::endl;
            provider->qservMgtServices()->setReplicas(
                worker,
                replicas,
                force,
                std::string(),
                [&finished] (replica::SetReplicasQservMgtRequest::Ptr const& request) {
                    std::cout
                        << "state:         " << request->state2string(request->state()) << "\n"
                        << "extendedState: " << request->state2string(request->extendedState()) << "\n"
                        << "serverError:   " << request->serverError() << std::endl;
                    if (request->extendedState() == replica::QservMgtRequest::SUCCESS) {
                        dump(request->replicas());
                    }
                    finished = true;
                }
            );

        } else if (operation == "ADD_REPLICA") {

            std::vector<std::string> databases;
            databases.push_back(database);

            provider->qservMgtServices()->addReplica(
                chunk,
                databases,
                worker,
                [&finished] (replica::AddReplicaQservMgtRequest::Ptr const& request) {
                    std::cout
                        << "state:         " << request->state2string(request->state()) << "\n"
                        << "extendedState: " << request->state2string(request->extendedState()) << "\n"
                        << "serverError:   " << request->serverError() << std::endl;
                    finished = true;
                }
            );

        } else if (operation == "REMOVE_REPLICA") {

            std::vector<std::string> databases;
            databases.push_back(database);

            provider->qservMgtServices()->removeReplica(
                chunk,
                databases,
                worker,
                force,
                [&finished] (replica::RemoveReplicaQservMgtRequest::Ptr const& request) {
                    std::cout
                        << "state:         " << request->state2string(request->state()) << "\n"
                        << "extendedState: " << request->state2string(request->extendedState()) << "\n"
                        << "serverError:   " << request->serverError() << std::endl;
                    finished = true;
                }
            );

        } else {
            throw std::logic_error("unsupported operation: " + operation);
        }

        // Block while the request is in progress

        util::BlockPost blockPost(1000, 2000);
        while (not finished) {
            blockPost.wait(200);
        }
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
    }
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
            "Usage:                                      \n"
            "  <operation> <parameters> [--config=<url>] \n"
            "\n"
            "Supported operations & their parameters:                     \n"
            "  ADD_REPLICA    <worker> <database> <chunk>                 \n"
            "  REMOVE_REPLICA <worker> <database> <chunk> [--force]       \n"
            "  GET_REPLICAS   <worker> <database-family>  [--in-use-only] \n"
            "  SET_REPLICAS   <worker> <filename>         [--force]       \n"
            "\n"
            "Parameters:                                                                               \n"
            "  <worker>          - the worker name (identifier)                                        \n"
            "  <database>        - the name of a database                                              \n"
            "  <database-family> - the name of a database family                                       \n"
            "  <filename>        - the name of a file with space-separated pairs of <database>:<chunk> \n"
            "  <chunk>           - the chunk number\n"
            "\n"
            "Flags and options:                                                                    \n"
            "  --config       - a configuration URL (a configuration file or a set of the database \n"
            "                   connection parameters [ DEFAULT: file:replication.cfg ]            \n"
            "  --force        - the flag telling the remote service to proceed with requested      \n"
            "                   replica removal regardless of the replica usage status             \n"
            "  --in-use-only  - select/report chunks which are in use                              \n"
        );

        ::operation = parser.parameterRestrictedBy(1, {
            "ADD_REPLICA",  "REMOVE_REPLICA",
            "GET_REPLICAS", "SET_REPLICAS"});

        ::worker         = parser.parameter<std::string>(2);
        ::database       =
        ::databaseFamily =
        ::inFileName     = parser.parameter<std::string>(3);

        if (parser.in(::operation, {"ADD_REPLICA", "REMOVE_REPLICA"})) {
            ::chunk = parser.parameter<unsigned int>(4);
        }
        if (parser.in(::operation, {"REMOVE_REPLICA","SET_REPLICAS"})) {
            ::force = parser.flag("force");
        }
        if (parser.in(::operation, {"GET_REPLICAS"})) {
            ::inUseOnly = parser.flag("in-use-only");
        }
        ::configUrl = parser.option<std::string>("config", "file:replication.cfg");

    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }
    ::test();
    return 0;
}
