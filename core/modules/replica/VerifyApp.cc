/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/VerifyApp.h"

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/VerifyJob.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description {
    "This application runs the replica verification algorithm for all known"
    " replicas across all ENABLED workers"
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

VerifyApp::Ptr VerifyApp::create(int argc,
                                 const char* const argv[]) {
    return Ptr(
        new VerifyApp(
            argc,
            argv
        )
    );
}


VerifyApp::VerifyApp(int argc,
                     const char* const argv[])
    :   Application(
            argc,
            argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().option(
        "max-replicas",
        "The maximum number of replicas to be processed simultaneously",
        _maxReplicas
    );

    parser().flag(
        "compute-check-sum",
        " automatically compute and store in the database check/control sums for"
        " all files of the found replica",
        _computeCheckSum);
}


int VerifyApp::runImpl() {

    // Once started this job will run indefinitely or until it fails and throws
    // an exception.

    atomic<bool> finished{false};
    auto const job = VerifyJob::create (
        _maxReplicas,
        _computeCheckSum,
        [] (VerifyJob::Ptr const& job,
            ReplicaDiff const& selfReplicaDiff,
            vector<ReplicaDiff> const& otherReplicaDiff) {

                ReplicaInfo const& r1 = selfReplicaDiff.replica1();
                ReplicaInfo const& r2 = selfReplicaDiff.replica2();
                cout << "Compared with OWN previous state  "
                     << " " << setw(20) << r1.database() << " " << setw(12) << r1.chunk()
                     << " " << setw(20) << r1.worker()   << " " << setw(20) << r2.worker() << " "
                     << " " << selfReplicaDiff.flags2string()
                     << endl;

                for (auto const& diff: otherReplicaDiff) {
                    ReplicaInfo const& r1 = diff.replica1();
                    ReplicaInfo const& r2 = diff.replica2();
                    cout << "Compared with OTHER replica state "
                         << " " << setw(20) << r1.database() << " " << setw(12) << r1.chunk()
                         << " " << setw(20) << r1.worker()   << " " << setw(20) << r2.worker() << " "
                         << " " << diff.flags2string()
                         << endl;
                }
        },
        Controller::create(serviceProvider()),
        string(),
        [&finished] (VerifyJob::Ptr const& job) {
            finished = true;
        }
    );
    job->start();

    util::BlockPost blockPost(1000,2000);
    while (not finished) {
        blockPost.wait();
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
