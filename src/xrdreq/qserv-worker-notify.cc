// System header
#include <fstream>
#include <iostream>
#include <iomanip>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/intTypes.h"
#include "global/ResourceUnit.h"
#include "proto/worker.pb.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"
#include "xrdreq/ChunkGroupQservRequest.h"
#include "xrdreq/ChunkListQservRequest.h"
#include "xrdreq/GetChunkListQservRequest.h"
#include "xrdreq/GetStatusQservRequest.h"
#include "xrdreq/SetChunkListQservRequest.h"
#include "xrdreq/TestEchoQservRequest.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

namespace global = lsst::qserv;
namespace proto = lsst::qserv::proto;
namespace util = lsst::qserv::util;
namespace xrdreq = lsst::qserv::xrdreq;

using namespace std;

namespace {

// Command line parameters

string operation;
string worker;
string inFileName;
unsigned int chunk;
vector<string> dbs;
vector<QueryId> queryIds;
string value;
string serviceProviderLocation;
bool inUseOnly;
bool includeTasks;
bool reload;
bool force;
bool printReport;

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
 * @param chunk - collection to be initialize
 */
void readInFile(xrdreq::SetChunkListQservRequest::ChunkCollection& chunks, vector<string>& databases) {
    chunks.clear();
    databases.clear();

    set<string> uniqueDatabaseNames;

    ifstream infile(inFileName);
    if (not infile.good()) {
        cerr << "failed to open file: " << inFileName << endl;
        throw runtime_error("failed to open file: " + inFileName);
    }

    string databaseAndChunk;
    while (infile >> databaseAndChunk) {
        if (databaseAndChunk.empty()) {
            continue;
        }

        string::size_type const pos = databaseAndChunk.rfind(':');
        if ((pos == string::npos) or (pos == 0) or (pos == databaseAndChunk.size() - 1)) {
            throw runtime_error("failed to parse file: " + inFileName +
                                ", illegal <database>::<chunk> pair: '" + databaseAndChunk + "'");
        }
        unsigned long const chunk = stoul(databaseAndChunk.substr(pos + 1));
        string const database = databaseAndChunk.substr(0, pos);
        chunks.emplace_back(xrdreq::SetChunkListQservRequest::Chunk{
                (unsigned int)chunk, database, 0 /* use_count (UNUSED) */
        });
        uniqueDatabaseNames.insert(database);
    }
    for (auto&& database : uniqueDatabaseNames) {
        databases.push_back(database);
    }
}

int test() {
    // Instantiate a request object

    atomic<bool> finished(false);

    shared_ptr<xrdreq::QservRequest> request = nullptr;

    if ("GET_CHUNK_LIST" == operation) {
        request = xrdreq::GetChunkListQservRequest::create(
                inUseOnly, [&finished](proto::WorkerCommandStatus::Code code, string const& error,
                                       xrdreq::GetChunkListQservRequest::ChunkCollection const& chunks) {
                    if (code != proto::WorkerCommandStatus::SUCCESS) {
                        cout << "code:  " << proto::WorkerCommandStatus_Code_Name(code) << "\n"
                             << "error: " << error << endl;
                    } else {
                        cout << "# total chunks: " << chunks.size() << "\n" << endl;
                        if (chunks.size()) {
                            cout << "      chunk |                         database | in use \n"
                                 << "------------+----------------------------------+--------\n";
                            for (auto const& entry : chunks) {
                                cout << " " << setw(10) << entry.chunk << " |"
                                     << " " << setw(32) << entry.database << " |"
                                     << " " << setw(6) << entry.use_count << " \n";
                            }
                            cout << endl;
                        }
                    }
                    finished = true;
                });

    } else if ("SET_CHUNK_LIST" == operation) {
        xrdreq::SetChunkListQservRequest::ChunkCollection chunks;
        vector<string> databases;
        readInFile(chunks, databases);

        request = xrdreq::SetChunkListQservRequest::create(
                chunks, databases, force,
                [&finished](proto::WorkerCommandStatus::Code code, string const& error,
                            xrdreq::SetChunkListQservRequest::ChunkCollection const& chunks) {
                    if (code != proto::WorkerCommandStatus::SUCCESS) {
                        cout << "code:  " << proto::WorkerCommandStatus_Code_Name(code) << "\n"
                             << "error: " << error << endl;
                    } else {
                        cout << "# total chunks: " << chunks.size() << "\n" << endl;
                        if (chunks.size()) {
                            cout << "      chunk |                         database | in use \n"
                                 << "------------+----------------------------------+--------\n";
                            for (auto const& entry : chunks) {
                                cout << " " << setw(10) << entry.chunk << " |"
                                     << " " << setw(32) << entry.database << " |"
                                     << " " << setw(6) << entry.use_count << " \n";
                            }
                            cout << endl;
                        }
                    }
                    finished = true;
                });

    } else if ("REBUILD_CHUNK_LIST" == operation) {
        request = xrdreq::RebuildChunkListQservRequest::create(
                reload, [&finished](proto::WorkerCommandStatus::Code code, string const& error,
                                    xrdreq::ChunkListQservRequest::ChunkCollection const& added,
                                    xrdreq::ChunkListQservRequest::ChunkCollection const& removed) {
                    if (code != proto::WorkerCommandStatus::SUCCESS) {
                        cout << "code:  " << proto::WorkerCommandStatus_Code_Name(code) << "\n"
                             << "error: " << error << endl;
                    } else {
                        cout << "# chunks added:   " << added.size() << "\n"
                             << "# chuks  removed: " << removed.size() << endl;
                    }
                    finished = true;
                });

    } else if ("RELOAD_CHUNK_LIST" == operation) {
        request = xrdreq::ReloadChunkListQservRequest::create(
                [&finished](proto::WorkerCommandStatus::Code code, string const& error,
                            xrdreq::ChunkListQservRequest::ChunkCollection const& added,
                            xrdreq::ChunkListQservRequest::ChunkCollection const& removed) {
                    if (code != proto::WorkerCommandStatus::SUCCESS) {
                        cout << "code:  " << proto::WorkerCommandStatus_Code_Name(code) << "\n"
                             << "error: " << error << endl;
                    } else {
                        cout << "# chunks added:   " << added.size() << "\n"
                             << "# chuks  removed: " << removed.size() << endl;
                    }
                    finished = true;
                });

    } else if ("ADD_CHUNK_GROUP" == operation) {
        request = xrdreq::AddChunkGroupQservRequest::create(
                chunk, dbs, [&finished](proto::WorkerCommandStatus::Code code, string const& error) {
                    if (code != proto::WorkerCommandStatus::SUCCESS) {
                        cout << "code:  " << proto::WorkerCommandStatus_Code_Name(code) << "\n"
                             << "error: " << error << endl;
                    }
                    finished = true;
                });

    } else if ("REMOVE_CHUNK_GROUP" == operation) {
        request = xrdreq::RemoveChunkGroupQservRequest::create(
                chunk, dbs, force, [&finished](proto::WorkerCommandStatus::Code code, string const& error) {
                    if (code != proto::WorkerCommandStatus::SUCCESS) {
                        cout << "code:  " << proto::WorkerCommandStatus_Code_Name(code) << "\n"
                             << "error: " << error << endl;
                    }
                    finished = true;
                });

    } else if ("TEST_ECHO" == operation) {
        request = xrdreq::TestEchoQservRequest::create(
                value, [&finished](proto::WorkerCommandStatus::Code code, string const& error,
                                   string const& sent, string const& received) {
                    if (code != proto::WorkerCommandStatus::SUCCESS) {
                        cout << "code:  " << proto::WorkerCommandStatus_Code_Name(code) << "\n"
                             << "error: " << error << endl;
                    } else {
                        cout << "value sent:     " << sent << "\n"
                             << "value received: " << received << endl;
                    }
                    finished = true;
                });

    } else if ("GET_STATUS" == operation) {
        request = xrdreq::GetStatusQservRequest::create(
                includeTasks, queryIds,
                [&finished](proto::WorkerCommandStatus::Code code, string const& error, string const& info) {
                    if (code != proto::WorkerCommandStatus::SUCCESS) {
                        cout << "code:  " << proto::WorkerCommandStatus_Code_Name(code) << "\n"
                             << "error: " << error << endl;
                    } else {
                        cout << "worker info: " << info << endl;
                    }
                    finished = true;
                });

    } else {
        return 1;
    }

    // Connect to a service provider
    XrdSsiErrInfo errInfo;
    auto serviceProvider = XrdSsiProviderClient->GetService(errInfo, serviceProviderLocation);
    if (nullptr == serviceProvider) {
        cerr << "failed to contact service provider at: " << serviceProviderLocation
             << ", error: " << errInfo.Get() << endl;
        return 1;
    }
    cout << "connected to service provider at: " << serviceProviderLocation << endl;

    // Submit the request
    XrdSsiResource resource(global::ResourceUnit::makeWorkerPath(worker));
    serviceProvider->ProcessRequest(*request, resource);

    // Block while the request is in progress
    util::BlockPost blockPost(1000, 2000);
    while (not finished) blockPost.wait(200);

    return 0;
}
}  // namespace

int main(int argc, const char* const argv[]) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
                argc, argv,
                "\n"
                "Usage:\n"
                "  <operation> [<parameter> [<parameter> [...]]]\n"
                "              [--service=<provider>]\n"
                "              [--in-use-only]\n"
                "              [--include-tasks]\n"
                "              [--reload]\n"
                "              [--force>]\n"
                "              [--print-report]\n"
                "\n"
                "Supported operations and mandatory parameters:\n"
                "    GET_CHUNK_LIST     <worker>\n"
                "    SET_CHUNK_LIST     <worker> <infile>\n"
                "    REBUILD_CHUNK_LIST <worker>\n"
                "    RELOAD_CHUNK_LIST  <worker>\n"
                "    ADD_CHUNK_GROUP    <worker> <chunk> <db> [<db> [<db> ... ]]\n"
                "    REMOVE_CHUNK_GROUP <worker> <chunk> <db> [<db> [<db> ... ]]\n"
                "    TEST_ECHO          <worker> <value>\n"
                "    GET_STATUS         <worker> [<qid> [<qid> ... ]]\n"
                "\n"
                "Flags an options:\n"
                "  --service=<provider>  - location of a service provider (default: 'localhost:1094')\n"
                "  --in-use-only         - used with GET_CHUNK_LIST to only report chunks which are in use.\n"
                "                          Otherwise all chunks will be reported\n"
                "  --include-tasks       - include detail info on the tasks\n"
                "  --reload              - used with REBUILD_CHUNK_LIST to also reload the list into a "
                "worker\n"
                "  --force               - force operation in REMOVE_CHUNK_GROUP even for chunks in use\n"
                "  --print-report        - print \n"
                "\n"
                "Parameters:\n"
                "  <worker>  - unique identifier of a worker (example: 'worker-1')\n"
                "  <infile>  - text file with space or newline separated pairs of <database>:<chunk>\n"
                "  <chunk>   - chunk number\n"
                "  <db>      - database name\n"
                "  <qid>     - user query identifier\n"
                "  <value>   - arbitrary string\n");

        ::operation = parser.parameterRestrictedBy(
                1, {"GET_CHUNK_LIST", "SET_CHUNK_LIST", "REBUILD_CHUNK_LIST", "RELOAD_CHUNK_LIST",
                    "ADD_CHUNK_GROUP", "REMOVE_CHUNK_GROUP", "TEST_ECHO", "GET_STATUS"});

        ::worker = parser.parameter<string>(2);

        if (parser.in(::operation, {"SET_CHUNK_LIST"})) {
            ::inFileName = parser.parameter<string>(3);
        } else if (parser.in(::operation, {"ADD_CHUNK_GROUP", "REMOVE_CHUNK_GROUP"})) {
            ::chunk = parser.parameter<unsigned int>(3);
            ::dbs = parser.parameters<string>(4);
        } else if (parser.in(::operation, {"TEST_ECHO"})) {
            ::value = parser.parameter<string>(3);
        } else if (parser.in(::operation, {"GET_STATUS"})) {
            ::queryIds = parser.parameters<QueryIds>(2);
        }
        ::serviceProviderLocation = parser.option<string>("service", "localhost:1094");
        ::inUseOnly = parser.flag("in-use-only");
        ::includeTasks = parser.flag("include-tasks");
        ::reload = parser.flag("reload");
        ::force = parser.flag("force");
        ::printReport = parser.flag("print-report");

    } catch (exception const& ex) {
        return 1;
    }
    return ::test();
}
