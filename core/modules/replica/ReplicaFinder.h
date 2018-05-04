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
#ifndef LSST_QSERV_REPLICA_REPLICA_CORE_FINDER_H
#define LSST_QSERV_REPLICA_REPLICA_CORE_FINDER_H

/// ReplicaFinder.h declares:
///
/// class ReplicaFinder
/// (see individual class documentation for more information)

// System headers
#include <atomic>
#include <ostream>
#include <string>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/FindAllRequest.h"
#include "replica/RequestTracker.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * The class implements replica lookup requsts in a scope of a database
 * accross all worker nodes of a replication cluster.
 *
 * TODO: this is a pilot implementation of a special kind of requests
 *       which will be implemented in the future. These requests will
 *       be initited via the Controller.
 */
class ReplicaFinder
    :   public CommonRequestTracker<FindAllRequest> {

public:

    // Default construction and copy semantics are prohibited

    ReplicaFinder() = delete;
    ReplicaFinder(ReplicaFinder const&) = delete;
    ReplicaFinder& operator=(ReplicaFinder const&) = delete;

    /**
     * The constructor is a blocking operation which will launch the requests
     * and wait until they're complete. Exceptions may be thrown in case
     * of errors. When the contructor unblocks a list of requests in the base class
     * should be inspected to see what's been found.
     *
     * @param controller     - a reference to the Controller for launching requests
     * @param database       - the name of a database
     * @param                - an output stream for monitoring and error printouts
     * @param progressReport - triggers periodic printout onto an output stream
     *                         to see the overall progress of the operation
     * @param errorReport    - trigger detailed error reporting after the completion
     *                         of the operation
     */
    ReplicaFinder(Controller::Ptr const& controller,
                  std::string const& database,
                  std::ostream& os,
                  bool progressReport=true,
                  bool errorReport=false);

    /// Destructor
    ~ReplicaFinder() override = default;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REPLICA_CORE_FINDER_H
