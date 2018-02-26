/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

/// replica_job_verify.cc implements a command-line tool which verifies
/// the integrity of existing replicas.

// System headers
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/CmdParser.h"
#include "replica/Controller.h"
#include "replica/VerifyJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"

namespace rc = lsst::qserv::replica;

namespace {

// Command line parameters

std::string configUrl;
size_t maxReplicas;
bool   computeCheckSum;
bool   progressReport;
bool   errorReport;
bool   detailedReport;
bool   chunkLocksReport = false;

/// Run the test
bool run () {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests
        // Note that omFinish callbak which are activated upon a completion
        // of the requsts will be run in that Controller's thread.

        rc::ServiceProvider provider (configUrl);

        rc::Controller::pointer controller = rc::Controller::create (provider);

        controller->run();

        ////////////////////////////////////////
        // Find all replicas accross all workers

        auto job =
            rc::VerifyJob::create (
                controller,
                [](rc::VerifyJob::pointer job) {
                    // Not using the callback because the completion of
                    // the request will be caught by the tracker below
                    ;
                },
                [] (rc::VerifyJob::pointer              job,
                    rc::ReplicaDiff const&              selfReplicaDiff,
                    std::vector<rc::ReplicaDiff> const& otherReplicaDiff) {

                    rc::ReplicaInfo const& r1 = selfReplicaDiff.replica1();
                    rc::ReplicaInfo const& r2 = selfReplicaDiff.replica2();
                    std::cout
                        << "Compared with OWN previous state  "
                        << " " << std::setw(20) << r1.database() << " " << std::setw(12) << r1.chunk()
                        << " " << std::setw(20) << r1.worker()   << " " << std::setw(20) << r2.worker() << " "
                        << " " << selfReplicaDiff.flags2string()
                        << std::endl;

                    for (auto const& diff: otherReplicaDiff) {
                        rc::ReplicaInfo const& r1 = diff.replica1();
                        rc::ReplicaInfo const& r2 = diff.replica2();
                        std::cout
                            << "Compared with OTHER replica state "
                            << " " << std::setw(20) << r1.database() << " " << std::setw(12) << r1.chunk()
                            << " " << std::setw(20) << r1.worker()   << " " << std::setw(20) << r2.worker() << " "
                            << " " << diff.flags2string()
                            << std::endl;
                    }
                },
                maxReplicas,
                computeCheckSum
            );

        job->start();
        job->track (progressReport,
                    errorReport,
                    chunkLocksReport,
                    std::cout);

        ///////////////////////////////////////////////////
        // Shutdown the controller and join with its thread

        controller->stop();
        controller->join();

    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
    }
    return true;
}
} /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;
    
    // Parse command line parameters
    try {
        rc::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  [--config=<url>]\n"
            "  [--max-replicas]\n"
            "  [--check-sum]\n"
            "  [--progress-report]\n"
            "  [--error-report]\n"
            "  [--detailed-report]\n"
            "\n"
            "Flags and options:\n"
            "  --config           - a configuration URL (a configuration file or a set of the database\n"
            "                       connection parameters [ DEFAULT: file:replication.cfg ]\n"
            "  --max-replicas     - the maximum number of replicas to be processed simultaneously\n"
            "                       [ DEFAULT: 1 ]\n"
            "  --check-sum        - compute check/control sum of files\n"
            "  --progress-report  - progress report when executing batches of requests\n"
            "  --error-report     - detailed report on failed requests\n"
            "  --detailed-report  - detailed report on results\n");

        ::configUrl       = parser.option<std::string> ("config", "file:replication.cfg");
        ::maxReplicas     = parser.option<unsigned int>("max-replicas", 1);
        ::computeCheckSum = parser.flag                ("check-sum");
        ::progressReport  = parser.flag                ("progress-report");
        ::errorReport     = parser.flag                ("error-report");
        ::detailedReport  = parser.flag                ("detailed-report");

    } catch (std::exception const& ex) {
        return 1;
    } 
    ::run ();
    return 0;
}
