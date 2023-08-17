// -*- LSST-C++ -*-
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
// System header
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/intTypes.h"
#include "proto/worker.pb.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"
#include "xrdreq/QueryManagementAction.h"
#include "xrdreq/QueryManagementRequest.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

namespace global = lsst::qserv;
namespace proto = lsst::qserv::proto;
namespace util = lsst::qserv::util;
namespace xrdreq = lsst::qserv::xrdreq;

using namespace std;

namespace {

// Command line parameters

vector<std::string> const allowedOperations = {"CANCEL_AFTER_RESTART", "CANCEL", "COMPLETE"};
proto::QueryManagement::Operation operation = proto::QueryManagement::CANCEL_AFTER_RESTART;
global::QueryId queryId;
bool allWorkers = false;
string serviceProviderLocation;

proto::QueryManagement::Operation str2operation(string const& str) {
    if (str == "CANCEL_AFTER_RESTART") {
        return proto::QueryManagement::CANCEL_AFTER_RESTART;
    } else if (str == "CANCEL") {
        return proto::QueryManagement::CANCEL;
    } else if (str == "COMPLETE") {
        return proto::QueryManagement::COMPLETE;
    }
    throw invalid_argument("error: unknown operation '" + str + "'");
}

int test() {
    bool finished = false;
    if (allWorkers) {
        xrdreq::QueryManagementAction::notifyAllWorkers(
                serviceProviderLocation, operation, queryId,
                [&finished](xrdreq::QueryManagementAction::Response const& response) {
                    for (auto itr : response) {
                        cout << "worker: " << itr.first << " error: " << itr.second << endl;
                    }
                    finished = true;
                });
    } else {
        // Connect to a service provider
        XrdSsiErrInfo errInfo;
        auto serviceProvider = XrdSsiProviderClient->GetService(errInfo, serviceProviderLocation);
        if (nullptr == serviceProvider) {
            cerr << "failed to contact service provider at: " << serviceProviderLocation
                 << ", error: " << errInfo.Get() << endl;
            return 1;
        }
        cout << "connected to service provider at: " << serviceProviderLocation << endl;

        // Prepare the request
        auto request = xrdreq::QueryManagementRequest::create(
                operation, queryId, [&finished](proto::WorkerCommandStatus::Code code, string const& error) {
                    cout << "code=" << proto::WorkerCommandStatus_Code_Name(code) << ", error='" << error
                         << "'" << endl;
                    finished = true;
                });

        // Submit the request
        XrdSsiResource resource("/query");
        serviceProvider->ProcessRequest(*request, resource);
    }

    // Wait before the request will finish or fail
    util::BlockPost blockPost(1000, 2000);
    while (!finished) {
        blockPost.wait(200);
    }
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
                "  <operation> <qid>\n"
                "  [--service=<provider>]\n"
                "\n"
                "Flags an options:\n"
                "  --all-workers         - The flag indicating if the operation had to involve all workers.\n"
                "  --service=<provider>  - A location of the service provider (default: 'localhost:1094').\n"
                "\n"
                "Parameters:\n"
                "  <operation>  - An operation over the query (queries). Allowed values of\n"
                "                 the parameter are: CANCEL_AFTER_RESTART, CANCEL, COMPLETE.\n"
                "  <qid>        - User query identifier.\n");

        ::operation = ::str2operation(parser.parameterRestrictedBy(1, ::allowedOperations));
        ::queryId = parser.parameter<global::QueryId>(2);
        ::allWorkers = parser.flag("all-workers");
        ::serviceProviderLocation = parser.option<string>("service", "localhost:1094");

    } catch (exception const& ex) {
        cerr << ex.what() << endl;
        return 1;
    }
    return ::test();
}
