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

// Class header
#include "replica/MessengerTestApp.h"

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/Messenger.h"
#include "replica/MessengerConnector.h"
#include "replica/ProtocolBuffer.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
    "This application tests the Messenger Network w/o leaving side effects on the workers.";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

MessengerTestApp::Ptr MessengerTestApp::create(int argc, char* argv[]) {
    return Ptr(
        new MessengerTestApp(argc, argv)
    );
}


MessengerTestApp::MessengerTestApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            false   /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().required(
        "worker",
        "The name of a worker to be used during the testing.",
        _workerName);

    parser().option(
        "iterations",
        "The number of number of iterations (must be strictly greater than 0).",
        _numIterations);

    parser().option(
        "cancel-after-iter",
        "If provided and if positive then issue a request to cancel an earlier made"
        " request iteration (starting from 0 and before the specified number of iterations)."
        " Also, if provided this number should not exceed the number of iterations.",
        _cancelAfterIter);
}


int MessengerTestApp::runImpl() {

    // Check if the input parameters make a sense

    if (_numIterations <= 0) {
        throw invalid_argument(
                    string(__func__) + ": the number of iterations must be strictly greater than 0");
    }
    if (_cancelAfterIter > 0 and _cancelAfterIter > _numIterations) {
        throw invalid_argument(
                    string(__func__) +
                    ": the number of iteration after which to cancel a message must not exceed"
                    " the total number of iterations");
    }

    // Instantiate the messenger configured in the same way as Controller 

    auto const controller = Controller::create(serviceProvider());
    auto const messenger  = Messenger::create(serviceProvider(), controller->io_service());

    // Prepare, serialize and launch multiple requests

    atomic<int> numFinished{0};

    for (int i=0; i < _numIterations; ++i) {

        string const id = "unique-request-id-" + to_string(i);

        auto const requestBufferPtr =
            make_shared<ProtocolBuffer>(serviceProvider()->config()->requestBufferSizeBytes());

        requestBufferPtr->resize();

        proto::ReplicationRequestHeader hdr;
        hdr.set_id(id);
        hdr.set_type(proto::ReplicationRequestHeader::SERVICE);
        hdr.set_service_type(proto::ReplicationServiceRequestType::SERVICE_STATUS);

        requestBufferPtr->serialize(hdr);

        messenger->send<proto::ReplicationServiceResponse>(
            _workerName,
            id,
            requestBufferPtr,
            [&numFinished] (string const& id,
                            bool success,
                            proto::ReplicationServiceResponse const& response) {
                numFinished++;
                cout << setw(32) << id << "  ** finished **  "
                     << (success ? "SUCCEEDED" : "FAILED") << endl;
            }
        );
    }
    if (_cancelAfterIter >= 0) {
        string const id = "unique-request-id-" + to_string(_cancelAfterIter);
        messenger->cancel(_workerName, id);
    }

    // Wait before all requests finish

    util::BlockPost blockPost(1000, 2000);
    while (numFinished < _numIterations) {
        cout << "HEARTBEAT  " << blockPost.wait() << " millisec" << endl;
    }

    return 0;
}

}}} // namespace lsst::qserv::replica
